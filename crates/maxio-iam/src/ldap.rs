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
}

pub struct LdapIdentityProvider {
    config: LdapConfig,
}

impl LdapIdentityProvider {
    pub fn new(config: LdapConfig) -> Self {
        Self { config }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.is_configured()
    }

    pub async fn authenticate(&self, username: &str, password: &str) -> Option<LdapUserInfo> {
        if !self.is_enabled() {
            return None;
        }

        if username.is_empty() || password.is_empty() {
            return None;
        }

        let user_dn = self.config.user_dn_search_filter.replace("%s", username);
        let full_dn = format!("{},{}", user_dn, self.config.user_dn_search_base);

        Some(LdapUserInfo {
            dn: full_dn,
            username: username.to_string(),
            groups: vec![],
        })
    }

    pub fn get_policy_for_groups(&self, groups: &[String]) -> Vec<String> {
        groups
            .iter()
            .map(|g| format!("ldap-group-{}", g.to_lowercase()))
            .collect()
    }
}
