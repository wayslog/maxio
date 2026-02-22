use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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
    pub redirect_uri_dynamic: bool,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub claim_userinfo: bool,
    #[serde(default)]
    pub role_policy: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub user_id_claim: String,
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
            redirect_uri_dynamic: false,
            scopes: vec!["openid".to_string()],
            claim_userinfo: false,
            role_policy: String::new(),
            display_name: String::new(),
            user_id_claim: "sub".to_string(),
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
    #[serde(default)]
    pub userinfo_endpoint: Option<String>,
    pub jwks_uri: String,
    #[serde(default)]
    pub scopes_supported: Vec<String>,
    #[serde(default)]
    pub response_types_supported: Vec<String>,
    #[serde(default)]
    pub id_token_signing_alg_values_supported: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct OpenIdUserInfo {
    pub subject: String,
    pub claims: HashMap<String, serde_json::Value>,
    pub policies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwkSet {
    pub keys: Vec<Jwk>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Jwk {
    pub kty: String,
    #[serde(default)]
    pub use_: Option<String>,
    #[serde(default)]
    pub kid: Option<String>,
    #[serde(default)]
    pub alg: Option<String>,
    // RSA keys
    #[serde(default)]
    pub n: Option<String>,
    #[serde(default)]
    pub e: Option<String>,
    // EC keys
    #[serde(default)]
    pub crv: Option<String>,
    #[serde(default)]
    pub x: Option<String>,
    #[serde(default)]
    pub y: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum OpenIdError {
    #[error("OpenID not configured: {0}")]
    NotConfigured(String),
    #[error("Discovery failed: {0}")]
    DiscoveryFailed(String),
    #[error("Token exchange failed: {0}")]
    TokenExchangeFailed(String),
    #[error("Token validation failed: {0}")]
    TokenValidationFailed(String),
    #[error("JWKS fetch failed: {0}")]
    JwksFetchFailed(String),
    #[error("Invalid token: {0}")]
    InvalidToken(String),
    #[error("UserInfo fetch failed: {0}")]
    UserInfoFailed(String),
}

// Stub implementation when openid feature is disabled
#[cfg(not(feature = "openid"))]
pub struct OpenIdIdentityProvider {
    config: OpenIdConfig,
}

#[cfg(not(feature = "openid"))]
impl OpenIdIdentityProvider {
    pub fn new(config: OpenIdConfig) -> Self {
        Self { config }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.is_configured()
    }

    pub async fn discover(&mut self) -> Result<(), OpenIdError> {
        Err(OpenIdError::NotConfigured(
            "OpenID feature not enabled".to_string(),
        ))
    }

    pub fn get_authorization_url(&self, _state: &str) -> Option<String> {
        None
    }

    pub async fn exchange_code(&self, _code: &str) -> Result<OpenIdUserInfo, OpenIdError> {
        Err(OpenIdError::NotConfigured(
            "OpenID feature not enabled".to_string(),
        ))
    }

    pub async fn validate_token(&self, _token: &str) -> Result<OpenIdUserInfo, OpenIdError> {
        Err(OpenIdError::NotConfigured(
            "OpenID feature not enabled".to_string(),
        ))
    }
}

// Full implementation when openid feature is enabled
#[cfg(feature = "openid")]
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};

#[cfg(feature = "openid")]
pub struct OpenIdIdentityProvider {
    config: OpenIdConfig,
    discovery: Option<OpenIdDiscovery>,
    jwks: Arc<RwLock<HashMap<String, DecodingKey>>>,
    jwk_set: Arc<RwLock<Option<JwkSet>>>,
}

#[cfg(feature = "openid")]
impl OpenIdIdentityProvider {
    pub fn new(config: OpenIdConfig) -> Self {
        Self {
            config,
            discovery: None,
            jwks: Arc::new(RwLock::new(HashMap::new())),
            jwk_set: Arc::new(RwLock::new(None)),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.is_configured()
    }

    pub fn get_discovery(&self) -> Option<&OpenIdDiscovery> {
        self.discovery.as_ref()
    }

    pub async fn discover(&mut self) -> Result<(), OpenIdError> {
        if !self.config.is_configured() {
            return Err(OpenIdError::NotConfigured(
                "OpenID not configured".to_string(),
            ));
        }

        let client = reqwest::Client::new();
        let discovery: OpenIdDiscovery = client
            .get(&self.config.config_url)
            .send()
            .await
            .map_err(|e| OpenIdError::DiscoveryFailed(e.to_string()))?
            .json()
            .await
            .map_err(|e| OpenIdError::DiscoveryFailed(e.to_string()))?;

        // Fetch JWKS
        self.fetch_jwks(&discovery.jwks_uri).await?;

        self.discovery = Some(discovery);
        Ok(())
    }

    async fn fetch_jwks(&self, jwks_uri: &str) -> Result<(), OpenIdError> {
        let client = reqwest::Client::new();
        let jwk_set: JwkSet = client
            .get(jwks_uri)
            .send()
            .await
            .map_err(|e| OpenIdError::JwksFetchFailed(e.to_string()))?
            .json()
            .await
            .map_err(|e| OpenIdError::JwksFetchFailed(e.to_string()))?;

        let mut keys = self.jwks.write().await;
        keys.clear();

        for jwk in &jwk_set.keys {
            if let Some(kid) = &jwk.kid {
                if let Ok(decoding_key) = self.jwk_to_decoding_key(jwk) {
                    keys.insert(kid.clone(), decoding_key);
                }
            }
        }

        let mut jwk_set_lock = self.jwk_set.write().await;
        *jwk_set_lock = Some(jwk_set);

        Ok(())
    }

    fn jwk_to_decoding_key(&self, jwk: &Jwk) -> Result<DecodingKey, OpenIdError> {
        match jwk.kty.as_str() {
            "RSA" => {
                let n = jwk
                    .n
                    .as_ref()
                    .ok_or_else(|| OpenIdError::InvalidToken("Missing RSA n".to_string()))?;
                let e = jwk
                    .e
                    .as_ref()
                    .ok_or_else(|| OpenIdError::InvalidToken("Missing RSA e".to_string()))?;
                DecodingKey::from_rsa_components(n, e)
                    .map_err(|e| OpenIdError::InvalidToken(e.to_string()))
            }
            "EC" => {
                let x = jwk
                    .x
                    .as_ref()
                    .ok_or_else(|| OpenIdError::InvalidToken("Missing EC x".to_string()))?;
                let y = jwk
                    .y
                    .as_ref()
                    .ok_or_else(|| OpenIdError::InvalidToken("Missing EC y".to_string()))?;
                DecodingKey::from_ec_components(x, y)
                    .map_err(|e| OpenIdError::InvalidToken(e.to_string()))
            }
            _ => Err(OpenIdError::InvalidToken(format!(
                "Unsupported key type: {}",
                jwk.kty
            ))),
        }
    }

    pub fn get_authorization_url(&self, state: &str) -> Option<String> {
        let discovery = self.discovery.as_ref()?;
        let scopes = if self.config.scopes.is_empty() {
            "openid".to_string()
        } else {
            self.config.scopes.join(" ")
        };

        Some(format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&scope={}&state={}",
            discovery.authorization_endpoint,
            urlencoding::encode(&self.config.client_id),
            urlencoding::encode(&self.config.redirect_uri),
            urlencoding::encode(&scopes),
            urlencoding::encode(state)
        ))
    }

    pub async fn exchange_code(&self, code: &str) -> Result<OpenIdUserInfo, OpenIdError> {
        let discovery = self
            .discovery
            .as_ref()
            .ok_or_else(|| OpenIdError::NotConfigured("Not discovered".to_string()))?;

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
            .map_err(|e| OpenIdError::TokenExchangeFailed(e.to_string()))?
            .json()
            .await
            .map_err(|e| OpenIdError::TokenExchangeFailed(e.to_string()))?;

        if let Some(error) = token_response.get("error") {
            return Err(OpenIdError::TokenExchangeFailed(
                error.as_str().unwrap_or("unknown error").to_string(),
            ));
        }

        let id_token = token_response["id_token"]
            .as_str()
            .ok_or_else(|| OpenIdError::TokenExchangeFailed("No ID token".to_string()))?;

        let mut user_info = self.validate_token(id_token).await?;

        // Optionally fetch additional claims from userinfo endpoint
        if self.config.claim_userinfo {
            if let Some(access_token) = token_response["access_token"].as_str() {
                if let Ok(userinfo_claims) = self.fetch_userinfo(access_token).await {
                    for (k, v) in userinfo_claims {
                        user_info.claims.insert(k, v);
                    }
                    // Re-extract policies after merging userinfo claims
                    user_info.policies = self.extract_policies(&user_info.claims);
                }
            }
        }

        Ok(user_info)
    }

    async fn fetch_userinfo(
        &self,
        access_token: &str,
    ) -> Result<HashMap<String, serde_json::Value>, OpenIdError> {
        let discovery = self
            .discovery
            .as_ref()
            .ok_or_else(|| OpenIdError::NotConfigured("Not discovered".to_string()))?;

        let userinfo_endpoint = discovery
            .userinfo_endpoint
            .as_ref()
            .ok_or_else(|| OpenIdError::UserInfoFailed("No userinfo endpoint".to_string()))?;

        let client = reqwest::Client::new();
        let response: HashMap<String, serde_json::Value> = client
            .get(userinfo_endpoint)
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|e| OpenIdError::UserInfoFailed(e.to_string()))?
            .json()
            .await
            .map_err(|e| OpenIdError::UserInfoFailed(e.to_string()))?;

        Ok(response)
    }

    pub async fn validate_token(&self, token: &str) -> Result<OpenIdUserInfo, OpenIdError> {
        let header = decode_header(token)
            .map_err(|e| OpenIdError::TokenValidationFailed(e.to_string()))?;

        let kid = header
            .kid
            .ok_or_else(|| OpenIdError::TokenValidationFailed("No kid in token".to_string()))?;

        let keys = self.jwks.read().await;
        let decoding_key = keys.get(&kid).ok_or_else(|| {
            OpenIdError::TokenValidationFailed(format!("Unknown key id: {}", kid))
        })?;

        let algorithm = match header.alg {
            jsonwebtoken::Algorithm::RS256 => Algorithm::RS256,
            jsonwebtoken::Algorithm::RS384 => Algorithm::RS384,
            jsonwebtoken::Algorithm::RS512 => Algorithm::RS512,
            jsonwebtoken::Algorithm::ES256 => Algorithm::ES256,
            jsonwebtoken::Algorithm::ES384 => Algorithm::ES384,
            jsonwebtoken::Algorithm::PS256 => Algorithm::PS256,
            jsonwebtoken::Algorithm::PS384 => Algorithm::PS384,
            jsonwebtoken::Algorithm::PS512 => Algorithm::PS512,
            alg => {
                return Err(OpenIdError::TokenValidationFailed(format!(
                    "Unsupported algorithm: {:?}",
                    alg
                )))
            }
        };

        let mut validation = Validation::new(algorithm);
        validation.set_audience(&[&self.config.client_id]);

        if let Some(discovery) = &self.discovery {
            validation.set_issuer(&[&discovery.issuer]);
        }

        let token_data = decode::<HashMap<String, serde_json::Value>>(token, decoding_key, &validation)
            .map_err(|e| OpenIdError::TokenValidationFailed(e.to_string()))?;

        let claims = token_data.claims;

        let subject = claims
            .get(&self.config.user_id_claim)
            .and_then(|v| v.as_str())
            .or_else(|| claims.get("sub").and_then(|v| v.as_str()))
            .unwrap_or("unknown")
            .to_string();

        let policies = self.extract_policies(&claims);

        Ok(OpenIdUserInfo {
            subject,
            claims,
            policies,
        })
    }

    fn extract_policies(&self, claims: &HashMap<String, serde_json::Value>) -> Vec<String> {
        if let Some(policy_claim) = claims.get(&self.config.claim_name) {
            if let Some(arr) = policy_claim.as_array() {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| format!("{}{}", self.config.claim_prefix, s))
                    .collect()
            } else if let Some(s) = policy_claim.as_str() {
                // Handle comma-separated policies
                s.split(',')
                    .map(|p| format!("{}{}", self.config.claim_prefix, p.trim()))
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }

    pub async fn refresh_jwks(&self) -> Result<(), OpenIdError> {
        let discovery = self
            .discovery
            .as_ref()
            .ok_or_else(|| OpenIdError::NotConfigured("Not discovered".to_string()))?;

        self.fetch_jwks(&discovery.jwks_uri).await
    }

    pub fn get_claim_name(&self) -> &str {
        &self.config.claim_name
    }

    pub fn get_role_policy(&self) -> &str {
        &self.config.role_policy
    }

    pub fn has_role_policy(&self) -> bool {
        !self.config.role_policy.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_openid_config_default() {
        let config = OpenIdConfig::default();
        assert!(!config.enabled);
        assert!(!config.is_configured());
        assert_eq!(config.claim_name, "policy");
        assert_eq!(config.user_id_claim, "sub");
    }

    #[test]
    fn test_openid_config_is_configured() {
        let mut config = OpenIdConfig::default();
        config.enabled = true;
        assert!(!config.is_configured()); // config_url and client_id are empty

        config.config_url = "https://example.com/.well-known/openid-configuration".to_string();
        assert!(!config.is_configured()); // client_id is empty

        config.client_id = "my-client".to_string();
        assert!(config.is_configured());
    }

    #[test]
    fn test_jwk_deserialization() {
        let jwk_json = r#"{
            "kty": "RSA",
            "kid": "test-key-1",
            "use": "sig",
            "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
            "e": "AQAB"
        }"#;

        let jwk: Jwk = serde_json::from_str(jwk_json).unwrap();
        assert_eq!(jwk.kty, "RSA");
        assert_eq!(jwk.kid, Some("test-key-1".to_string()));
        assert!(jwk.n.is_some());
        assert!(jwk.e.is_some());
    }
}
