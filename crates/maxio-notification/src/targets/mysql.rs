use async_trait::async_trait;
use maxio_common::error::Result;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::system::NotificationTarget;
use crate::types::S3Event;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlConfig {
    pub enabled: bool,
    pub dsn: String,
    pub table: String,
    #[serde(default)]
    pub format: MySqlFormat,
    #[serde(default = "default_queue_limit")]
    pub queue_limit: usize,
    #[serde(default = "default_max_open_connections")]
    pub max_open_connections: u32,
}

fn default_queue_limit() -> usize {
    100000
}

fn default_max_open_connections() -> u32 {
    2
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MySqlFormat {
    #[default]
    Namespace,
    Access,
}

impl Default for MySqlConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dsn: String::new(),
            table: "minio_events".to_string(),
            format: MySqlFormat::default(),
            queue_limit: default_queue_limit(),
            max_open_connections: default_max_open_connections(),
        }
    }
}

/// MySQL notification target
/// Note: Full implementation requires sqlx with mysql feature
/// This implementation logs events for demonstration purposes
pub struct MySqlTarget {
    config: MySqlConfig,
}

impl MySqlTarget {
    pub fn new(config: MySqlConfig) -> Self {
        Self { config }
    }

    pub fn is_active(&self) -> bool {
        self.config.enabled && !self.config.dsn.is_empty()
    }
}

#[async_trait]
impl NotificationTarget for MySqlTarget {
    async fn send(&self, event: &S3Event) -> Result<()> {
        if !self.is_active() {
            return Ok(());
        }

        info!(
            target: "mysql",
            table = %self.config.table,
            event_name = %event.event_name,
            bucket = %event.bucket.name,
            object = %event.object.key,
            "event queued for MySQL"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_config_default() {
        let config = MySqlConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.table, "minio_events");
        assert_eq!(config.max_open_connections, 2);
    }

    #[test]
    fn test_mysql_target_inactive_when_disabled() {
        let config = MySqlConfig::default();
        let target = MySqlTarget::new(config);
        assert!(!target.is_active());
    }

    #[test]
    fn test_mysql_target_active() {
        let config = MySqlConfig {
            enabled: true,
            dsn: "mysql://root:password@localhost/minio".to_string(),
            ..Default::default()
        };
        let target = MySqlTarget::new(config);
        assert!(target.is_active());
    }
}
