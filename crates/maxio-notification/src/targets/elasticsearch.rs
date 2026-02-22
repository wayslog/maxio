use async_trait::async_trait;
use maxio_common::error::Result;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::system::NotificationTarget;
use crate::types::S3Event;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElasticsearchConfig {
    pub enabled: bool,
    pub url: String,
    pub index: String,
    #[serde(default)]
    pub format: ElasticsearchFormat,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default = "default_queue_limit")]
    pub queue_limit: usize,
}

fn default_queue_limit() -> usize {
    100000
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ElasticsearchFormat {
    #[default]
    Namespace,
    Access,
}

impl Default for ElasticsearchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: String::new(),
            index: "minio_events".to_string(),
            format: ElasticsearchFormat::default(),
            username: None,
            password: None,
            queue_limit: default_queue_limit(),
        }
    }
}

pub struct ElasticsearchTarget {
    config: ElasticsearchConfig,
    client: reqwest::Client,
}

impl ElasticsearchTarget {
    pub fn new(config: ElasticsearchConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    pub fn is_active(&self) -> bool {
        self.config.enabled && !self.config.url.is_empty()
    }

    fn build_document(&self, event: &S3Event) -> serde_json::Value {
        match self.config.format {
            ElasticsearchFormat::Namespace => {
                serde_json::json!({
                    "Records": [event]
                })
            }
            ElasticsearchFormat::Access => {
                serde_json::json!({
                    "eventTime": event.event_time,
                    "eventName": event.event_name,
                    "bucket": event.bucket.name,
                    "object": event.object.key,
                    "size": event.object.size,
                    "etag": event.object.etag,
                })
            }
        }
    }
}

#[async_trait]
impl NotificationTarget for ElasticsearchTarget {
    async fn send(&self, event: &S3Event) -> Result<()> {
        if !self.is_active() {
            return Ok(());
        }

        let url = format!(
            "{}/{}/_doc",
            self.config.url.trim_end_matches('/'),
            self.config.index
        );
        let document = self.build_document(event);

        let mut request = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&document);

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            request = request.basic_auth(username, Some(password));
        }

        match request.send().await {
            Ok(response) => {
                if response.status().is_success() {
                    info!(
                        target: "elasticsearch",
                        index = %self.config.index,
                        event_name = %event.event_name,
                        "event sent successfully"
                    );
                    Ok(())
                } else {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    Err(maxio_common::error::MaxioError::InternalError(format!(
                        "Elasticsearch returned {}: {}",
                        status, body
                    )))
                }
            }
            Err(err) => Err(maxio_common::error::MaxioError::InternalError(format!(
                "Failed to connect to Elasticsearch: {}",
                err
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_elasticsearch_config_default() {
        let config = ElasticsearchConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.index, "minio_events");
        assert_eq!(config.queue_limit, 100000);
    }

    #[test]
    fn test_elasticsearch_target_inactive_when_disabled() {
        let config = ElasticsearchConfig::default();
        let target = ElasticsearchTarget::new(config);
        assert!(!target.is_active());
    }

    #[test]
    fn test_elasticsearch_target_active() {
        let config = ElasticsearchConfig {
            enabled: true,
            url: "http://localhost:9200".to_string(),
            ..Default::default()
        };
        let target = ElasticsearchTarget::new(config);
        assert!(target.is_active());
    }
}
