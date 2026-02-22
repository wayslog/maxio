use async_trait::async_trait;
use maxio_common::error::Result;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::system::NotificationTarget;
use crate::types::S3Event;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub enabled: bool,
    pub connection_string: String,
    pub table: String,
    #[serde(default)]
    pub format: PostgresFormat,
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
pub enum PostgresFormat {
    #[default]
    Namespace,
    Access,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            connection_string: String::new(),
            table: "minio_events".to_string(),
            format: PostgresFormat::default(),
            queue_limit: default_queue_limit(),
            max_open_connections: default_max_open_connections(),
        }
    }
}

/// PostgreSQL notification target
/// Note: Full implementation requires tokio-postgres or sqlx dependency
/// This implementation logs events for demonstration purposes
pub struct PostgresTarget {
    config: PostgresConfig,
}

impl PostgresTarget {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }

    pub fn is_active(&self) -> bool {
        self.config.enabled && !self.config.connection_string.is_empty()
    }
}

#[async_trait]
impl NotificationTarget for PostgresTarget {
    async fn send(&self, event: &S3Event) -> Result<()> {
        if !self.is_active() {
            return Ok(());
        }

        info!(
            target: "postgres",
            table = %self.config.table,
            event_name = %event.event_name,
            bucket = %event.bucket.name,
            object = %event.object.key,
            "event queued for PostgreSQL"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_config_default() {
        let config = PostgresConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.table, "minio_events");
        assert_eq!(config.max_open_connections, 2);
    }

    #[test]
    fn test_postgres_target_inactive_when_disabled() {
        let config = PostgresConfig::default();
        let target = PostgresTarget::new(config);
        assert!(!target.is_active());
    }

    #[test]
    fn test_postgres_target_active() {
        let config = PostgresConfig {
            enabled: true,
            connection_string: "postgres://localhost/minio".to_string(),
            ..Default::default()
        };
        let target = PostgresTarget::new(config);
        assert!(target.is_active());
    }
}
