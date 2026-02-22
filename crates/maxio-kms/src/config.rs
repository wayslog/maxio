use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsConfig {
    pub endpoint: String,
    pub key_id: String,
    #[serde(default)]
    pub tls_skip_verify: bool,
    pub credentials: Option<KmsCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsCredentials {
    pub access_key: String,
    pub secret_key: String,
}

impl Default for KmsConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            key_id: "my-minio-key".to_string(),
            tls_skip_verify: false,
            credentials: None,
        }
    }
}

impl KmsConfig {
    pub fn is_configured(&self) -> bool {
        !self.endpoint.is_empty() && !self.key_id.is_empty()
    }
}
