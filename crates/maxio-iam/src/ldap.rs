use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LdapConfig {
    pub enabled: bool,
    pub server_addr: String,
    pub lookup_bind_dn: String,
    pub lookup_bind_password: String,
    pub user_dn_search_base: String,
    pub user_dn_search_filter: String,
    pub group_search_base: String,
    pub group_search_filter: String,
    #[serde(default)]
    pub tls_skip_verify: bool,
    #[serde(default)]
    pub server_insecure: bool,
    #[serde(default)]
    pub server_starttls: bool,
    #[serde(default)]
    pub user_dn_attributes: Vec<String>,
}

impl Default for LdapConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            server_addr: String::new(),
            lookup_bind_dn: String::new(),
            lookup_bind_password: String::new(),
            user_dn_search_base: "dc=example,dc=com".to_string(),
            user_dn_search_filter: "(uid=%s)".to_string(),
            group_search_base: "ou=groups,dc=example,dc=com".to_string(),
            group_search_filter: "(member=%d)".to_string(),
            tls_skip_verify: false,
            server_insecure: false,
            server_starttls: false,
            user_dn_attributes: vec![],
        }
    }
}

impl LdapConfig {
    pub fn is_configured(&self) -> bool {
        self.enabled && !self.server_addr.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct LdapUserInfo {
    pub dn: String,
    pub username: String,
    pub groups: Vec<String>,
    pub attributes: std::collections::HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct LdapSearchResult {
    pub dn: String,
    pub norm_dn: String,
    pub attributes: std::collections::HashMap<String, Vec<String>>,
}

// Stub implementation when ldap feature is disabled
#[cfg(not(feature = "ldap"))]
pub struct LdapIdentityProvider {
    config: LdapConfig,
}

#[cfg(not(feature = "ldap"))]
impl LdapIdentityProvider {
    pub fn new(config: LdapConfig) -> Self {
        Self { config }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.is_configured()
    }

    pub async fn authenticate(
        &self,
        _username: &str,
        _password: &str,
    ) -> Result<Option<LdapUserInfo>, LdapError> {
        Err(LdapError::NotConfigured(
            "LDAP feature not enabled".to_string(),
        ))
    }

    pub async fn lookup_user_dn(
        &self,
        _username: &str,
    ) -> Result<Option<LdapSearchResult>, LdapError> {
        Err(LdapError::NotConfigured(
            "LDAP feature not enabled".to_string(),
        ))
    }

    pub async fn get_user_groups(&self, _user_dn: &str) -> Result<Vec<String>, LdapError> {
        Err(LdapError::NotConfigured(
            "LDAP feature not enabled".to_string(),
        ))
    }

    pub fn get_policy_for_groups(&self, groups: &[String]) -> Vec<String> {
        groups
            .iter()
            .map(|g| format!("ldap-group-{}", g.to_lowercase()))
            .collect()
    }
}

// Full implementation when ldap feature is enabled
#[cfg(feature = "ldap")]
use ldap3::{Ldap, LdapConnAsync, LdapConnSettings, Scope, SearchEntry};

#[cfg(feature = "ldap")]
use std::time::Duration;

#[cfg(feature = "ldap")]
pub struct LdapIdentityProvider {
    config: LdapConfig,
}

#[cfg(feature = "ldap")]
impl LdapIdentityProvider {
    pub fn new(config: LdapConfig) -> Self {
        Self { config }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.is_configured()
    }

    async fn connect(&self) -> Result<Ldap, LdapError> {
        let settings = LdapConnSettings::new()
            .set_conn_timeout(Duration::from_secs(10))
            .set_starttls(self.config.server_starttls);

        let url = if self.config.server_insecure {
            format!("ldap://{}", self.config.server_addr)
        } else {
            format!("ldaps://{}", self.config.server_addr)
        };

        let (conn, ldap) = LdapConnAsync::with_settings(settings, &url)
            .await
            .map_err(|e| LdapError::ConnectionFailed(e.to_string()))?;

        ldap3::drive!(conn);

        Ok(ldap)
    }

    async fn lookup_bind(&self, ldap: &mut Ldap) -> Result<(), LdapError> {
        ldap.simple_bind(&self.config.lookup_bind_dn, &self.config.lookup_bind_password)
            .await
            .map_err(|e| LdapError::BindFailed(e.to_string()))?
            .success()
            .map_err(|e| LdapError::BindFailed(e.to_string()))?;
        Ok(())
    }

    pub async fn lookup_user_dn(
        &self,
        username: &str,
    ) -> Result<Option<LdapSearchResult>, LdapError> {
        if !self.is_enabled() {
            return Err(LdapError::NotConfigured("LDAP not enabled".to_string()));
        }

        let mut ldap = self.connect().await?;
        self.lookup_bind(&mut ldap).await?;

        let filter = self.config.user_dn_search_filter.replace("%s", username);
        let attrs: Vec<&str> = self
            .config
            .user_dn_attributes
            .iter()
            .map(|s| s.as_str())
            .collect();

        let (rs, _res) = ldap
            .search(
                &self.config.user_dn_search_base,
                Scope::Subtree,
                &filter,
                attrs,
            )
            .await
            .map_err(|e| LdapError::SearchFailed(e.to_string()))?
            .success()
            .map_err(|e| LdapError::SearchFailed(e.to_string()))?;

        let _ = ldap.unbind().await;

        if rs.is_empty() {
            return Ok(None);
        }

        let entry = SearchEntry::construct(rs.into_iter().next().unwrap());
        let mut attributes = std::collections::HashMap::new();
        for (k, v) in entry.attrs {
            attributes.insert(k, v);
        }

        Ok(Some(LdapSearchResult {
            dn: entry.dn.clone(),
            norm_dn: normalize_dn(&entry.dn),
            attributes,
        }))
    }

    pub async fn get_user_groups(&self, user_dn: &str) -> Result<Vec<String>, LdapError> {
        if !self.is_enabled() {
            return Err(LdapError::NotConfigured("LDAP not enabled".to_string()));
        }

        let mut ldap = self.connect().await?;
        self.lookup_bind(&mut ldap).await?;

        let filter = self.config.group_search_filter.replace("%d", user_dn);

        let (rs, _res) = ldap
            .search(&self.config.group_search_base, Scope::Subtree, &filter, vec!["dn"])
            .await
            .map_err(|e| LdapError::SearchFailed(e.to_string()))?
            .success()
            .map_err(|e| LdapError::SearchFailed(e.to_string()))?;

        let _ = ldap.unbind().await;

        let groups: Vec<String> = rs
            .into_iter()
            .map(|entry| SearchEntry::construct(entry).dn)
            .collect();

        Ok(groups)
    }

    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<LdapUserInfo>, LdapError> {
        if !self.is_enabled() {
            return Err(LdapError::NotConfigured("LDAP not enabled".to_string()));
        }

        if username.is_empty() || password.is_empty() {
            return Ok(None);
        }

        // First, lookup the user DN
        let lookup_result = self.lookup_user_dn(username).await?;
        let user_info = match lookup_result {
            Some(info) => info,
            None => return Ok(None),
        };

        // Now bind as the user to verify password
        let mut ldap = self.connect().await?;
        let bind_result = ldap.simple_bind(&user_info.dn, password).await;

        match bind_result {
            Ok(res) => {
                if res.success().is_err() {
                    let _ = ldap.unbind().await;
                    return Ok(None);
                }
            }
            Err(_) => {
                let _ = ldap.unbind().await;
                return Ok(None);
            }
        }

        // Re-bind as lookup user to search for groups
        self.lookup_bind(&mut ldap).await?;

        // Search for user groups
        let filter = self.config.group_search_filter.replace("%d", &user_info.dn);
        let (rs, _res) = ldap
            .search(&self.config.group_search_base, Scope::Subtree, &filter, vec!["dn"])
            .await
            .map_err(|e| LdapError::SearchFailed(e.to_string()))?
            .success()
            .map_err(|e| LdapError::SearchFailed(e.to_string()))?;

        let _ = ldap.unbind().await;

        let groups: Vec<String> = rs
            .into_iter()
            .map(|entry| SearchEntry::construct(entry).dn)
            .collect();

        Ok(Some(LdapUserInfo {
            dn: user_info.dn,
            username: username.to_string(),
            groups,
            attributes: user_info.attributes,
        }))
    }

    pub async fn validate_user_dn(&self, user_dn: &str) -> Result<Option<LdapSearchResult>, LdapError> {
        if !self.is_enabled() {
            return Err(LdapError::NotConfigured("LDAP not enabled".to_string()));
        }

        let mut ldap = self.connect().await?;
        self.lookup_bind(&mut ldap).await?;

        let (rs, _res) = ldap
            .search(user_dn, Scope::Base, "(objectClass=*)", vec!["dn"])
            .await
            .map_err(|e| LdapError::SearchFailed(e.to_string()))?
            .success()
            .map_err(|e| LdapError::SearchFailed(e.to_string()))?;

        let _ = ldap.unbind().await;

        if rs.is_empty() {
            return Ok(None);
        }

        let entry = SearchEntry::construct(rs.into_iter().next().unwrap());
        Ok(Some(LdapSearchResult {
            dn: entry.dn.clone(),
            norm_dn: normalize_dn(&entry.dn),
            attributes: std::collections::HashMap::new(),
        }))
    }

    pub async fn get_non_eligible_users(&self, user_dns: &[String]) -> Result<Vec<String>, LdapError> {
        if !self.is_enabled() {
            return Err(LdapError::NotConfigured("LDAP not enabled".to_string()));
        }

        let mut non_eligible = Vec::new();
        for dn in user_dns {
            match self.validate_user_dn(dn).await? {
                Some(_) => {}
                None => non_eligible.push(dn.clone()),
            }
        }
        Ok(non_eligible)
    }

    pub fn get_policy_for_groups(&self, groups: &[String]) -> Vec<String> {
        groups
            .iter()
            .map(|g| format!("ldap-group-{}", extract_cn(g).to_lowercase()))
            .collect()
    }

    pub fn parses_as_dn(&self, s: &str) -> bool {
        // Simple DN validation: contains at least one '=' and ','
        s.contains('=') && (s.contains(',') || s.starts_with("cn=") || s.starts_with("uid="))
    }

    pub fn is_ldap_user_dn(&self, user: &str) -> bool {
        if !self.parses_as_dn(user) {
            return false;
        }
        let norm = normalize_dn(user).to_lowercase();
        let base = normalize_dn(&self.config.user_dn_search_base).to_lowercase();
        norm.ends_with(&base)
    }

    pub fn is_ldap_group_dn(&self, group: &str) -> bool {
        if !self.parses_as_dn(group) {
            return false;
        }
        let norm = normalize_dn(group).to_lowercase();
        let base = normalize_dn(&self.config.group_search_base).to_lowercase();
        norm.ends_with(&base)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LdapError {
    #[error("LDAP not configured: {0}")]
    NotConfigured(String),
    #[error("LDAP connection failed: {0}")]
    ConnectionFailed(String),
    #[error("LDAP bind failed: {0}")]
    BindFailed(String),
    #[error("LDAP search failed: {0}")]
    SearchFailed(String),
}

fn normalize_dn(dn: &str) -> String {
    // Simple normalization: trim whitespace around components
    dn.split(',')
        .map(|part| part.trim())
        .collect::<Vec<_>>()
        .join(",")
}

fn extract_cn(dn: &str) -> String {
    // Extract CN from DN like "cn=admins,ou=groups,dc=example,dc=com"
    for part in dn.split(',') {
        let part = part.trim();
        if part.to_lowercase().starts_with("cn=") {
            return part[3..].to_string();
        }
    }
    dn.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_dn() {
        assert_eq!(
            normalize_dn("cn=admin, ou=users, dc=example, dc=com"),
            "cn=admin,ou=users,dc=example,dc=com"
        );
    }

    #[test]
    fn test_extract_cn() {
        assert_eq!(
            extract_cn("cn=admins,ou=groups,dc=example,dc=com"),
            "admins"
        );
        assert_eq!(extract_cn("ou=groups,dc=example,dc=com"), "ou=groups,dc=example,dc=com");
    }

    #[test]
    fn test_ldap_config_default() {
        let config = LdapConfig::default();
        assert!(!config.enabled);
        assert!(!config.is_configured());
    }

    #[test]
    fn test_ldap_config_is_configured() {
        let mut config = LdapConfig::default();
        config.enabled = true;
        assert!(!config.is_configured()); // server_addr is empty

        config.server_addr = "ldap.example.com:389".to_string();
        assert!(config.is_configured());
    }
}
