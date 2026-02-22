use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierConfig {
    pub name: String,
    pub target: TierTarget,
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TierTarget {
    #[serde(rename = "s3")]
    S3 {
        endpoint: String,
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
        #[serde(default)]
        use_ssl: bool,
    },
    #[serde(rename = "azure")]
    Azure {
        account_name: String,
        account_key: String,
        container: String,
        #[serde(default)]
        endpoint: Option<String>,
    },
    #[serde(rename = "gcs")]
    Gcs {
        bucket: String,
        credentials_json: String,
        #[serde(default)]
        prefix: String,
    },
}

impl TierConfig {
    pub fn tier_type(&self) -> &'static str {
        match &self.target {
            TierTarget::S3 { .. } => "s3",
            TierTarget::Azure { .. } => "azure",
            TierTarget::Gcs { .. } => "gcs",
        }
    }
}
