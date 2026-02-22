use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    pub enabled: bool,
    #[serde(default)]
    pub targets: Vec<AuditTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AuditTarget {
    #[serde(rename = "console")]
    Console,
    #[serde(rename = "file")]
    File { path: String },
    #[serde(rename = "webhook")]
    Webhook {
        endpoint: String,
        #[serde(default)]
        auth_token: Option<String>,
    },
    #[serde(rename = "kafka")]
    Kafka { brokers: Vec<String>, topic: String },
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            targets: vec![],
        }
    }
}

impl AuditConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled && !self.targets.is_empty()
    }
}
