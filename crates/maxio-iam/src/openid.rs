use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenIdConfig {
    pub enabled: bool,
    pub config_url: String,
    pub client_id: String,
    pub client_secret: String,
    pub claim_name: String,
    pub claim_prefix: String,
    pub redirect_uri: String,
    #[serde(default)]
    pub scopes: Vec<String>,
}

impl Default for OpenIdConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            config_url: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            claim_name: "policy".to_string(),
            claim_prefix: String::new(),
            redirect_uri: String::new(),
            scopes: vec!["openid".to_string()],
        }
    }
}

impl OpenIdConfig {
    pub fn is_configured(&self) -> bool {
        self.enabled && !self.config_url.is_empty() && !self.client_id.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenIdDiscovery {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub userinfo_endpoint: Option<String>,
    pub jwks_uri: String,
}

#[derive(Debug, Clone)]
pub struct OpenIdUserInfo {
    pub subject: String,
    pub claims: std::collections::HashMap<String, String>,
    pub policies: Vec<String>,
}

pub struct OpenIdIdentityProvider {
    config: OpenIdConfig,
    discovery: Option<OpenIdDiscovery>,
}

impl OpenIdIdentityProvider {
    pub fn new(config: OpenIdConfig) -> Self {
        Self {
            config,
            discovery: None,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.is_configured()
    }

    pub async fn discover(&mut self) -> Result<(), String> {
        if !self.is_enabled() {
            return Err("OpenID not configured".to_string());
        }

        let client = reqwest::Client::new();
        let discovery: OpenIdDiscovery = client
            .get(&self.config.config_url)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;

        self.discovery = Some(discovery);
        Ok(())
    }

    pub fn get_authorization_url(&self, state: &str) -> Option<String> {
        let discovery = self.discovery.as_ref()?;
        let scopes = self.config.scopes.join(" ");

        Some(format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&scope={}&state={}",
            discovery.authorization_endpoint,
            urlencoding::encode(&self.config.client_id),
            urlencoding::encode(&self.config.redirect_uri),
            urlencoding::encode(&scopes),
            urlencoding::encode(state)
        ))
    }

    pub async fn exchange_code(&self, code: &str) -> Result<OpenIdUserInfo, String> {
        let discovery = self.discovery.as_ref().ok_or("Not discovered")?;

        let client = reqwest::Client::new();
        let params = [
            ("grant_type", "authorization_code"),
            ("code", code),
            ("client_id", &self.config.client_id),
            ("client_secret", &self.config.client_secret),
            ("redirect_uri", &self.config.redirect_uri),
        ];

        let token_response: serde_json::Value = client
            .post(&discovery.token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;

        let _access_token = token_response["access_token"]
            .as_str()
            .ok_or("No access token")?;

        let id_token = token_response["id_token"]
            .as_str()
            .ok_or("No ID token")?;

        self.parse_id_token(id_token)
    }

    fn parse_id_token(&self, token: &str) -> Result<OpenIdUserInfo, String> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err("Invalid JWT format".to_string());
        }

        let payload = base64::Engine::decode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            parts[1],
        )
        .map_err(|e| e.to_string())?;

        let claims: serde_json::Value =
            serde_json::from_slice(&payload).map_err(|e| e.to_string())?;

        let subject = claims["sub"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();

        let mut claim_map = std::collections::HashMap::new();
        if let Some(obj) = claims.as_object() {
            for (k, v) in obj {
                if let Some(s) = v.as_str() {
                    claim_map.insert(k.clone(), s.to_string());
                }
            }
        }

        let policies = if let Some(policy_claim) = claims.get(&self.config.claim_name) {
            if let Some(arr) = policy_claim.as_array() {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| format!("{}{}", self.config.claim_prefix, s))
                    .collect()
            } else if let Some(s) = policy_claim.as_str() {
                vec![format!("{}{}", self.config.claim_prefix, s)]
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        Ok(OpenIdUserInfo {
            subject,
            claims: claim_map,
            policies,
        })
    }
}
