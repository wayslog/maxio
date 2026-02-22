use maxio_common::error::{MaxioError, Result};
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use super::config::{AuditConfig, AuditTarget};
use super::types::AuditEvent;

pub struct AuditLogger {
    config: AuditConfig,
    sender: Option<mpsc::Sender<AuditEvent>>,
}

impl AuditLogger {
    pub fn new(config: AuditConfig) -> Self {
        Self {
            config,
            sender: None,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        if !self.config.is_enabled() {
            return Ok(());
        }

        let (tx, mut rx) = mpsc::channel::<AuditEvent>(1000);
        self.sender = Some(tx);

        let targets = self.config.targets.clone();

        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                for target in &targets {
                    let _ = Self::send_to_target(target, &event).await;
                }
            }
        });

        Ok(())
    }

    pub async fn log(&self, event: AuditEvent) -> Result<()> {
        if let Some(sender) = &self.sender {
            sender
                .send(event)
                .await
                .map_err(|e| MaxioError::InternalError(format!("Failed to send audit event: {e}")))?;
        }
        Ok(())
    }

    pub fn is_enabled(&self) -> bool {
        self.config.is_enabled()
    }

    async fn send_to_target(target: &AuditTarget, event: &AuditEvent) -> Result<()> {
        match target {
            AuditTarget::Console => Self::send_to_console(event).await,
            AuditTarget::File { path } => Self::send_to_file(path, event).await,
            AuditTarget::Webhook {
                endpoint,
                auth_token,
            } => Self::send_to_webhook(endpoint, auth_token.as_deref(), event).await,
            AuditTarget::Kafka { brokers, topic } => {
                Self::send_to_kafka(brokers, topic, event).await
            }
        }
    }

    async fn send_to_console(event: &AuditEvent) -> Result<()> {
        let json = serde_json::to_string(event)
            .map_err(|e| MaxioError::InternalError(e.to_string()))?;
        println!("{json}");
        Ok(())
    }

    async fn send_to_file(path: &str, event: &AuditEvent) -> Result<()> {
        let json = serde_json::to_string(event)
            .map_err(|e| MaxioError::InternalError(format!("Failed to serialize audit event: {e}")))?;

        if let Some(parent) = Path::new(path).parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| MaxioError::InternalError(format!("Failed to create audit log directory: {e}")))?;
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| MaxioError::InternalError(format!("Failed to open audit log file: {e}")))?;

        file.write_all(json.as_bytes())
            .await
            .map_err(|e| MaxioError::InternalError(format!("Failed to write audit event: {e}")))?;
        file.write_all(b"\n")
            .await
            .map_err(|e| MaxioError::InternalError(format!("Failed to write newline: {e}")))?;

        Ok(())
    }

    async fn send_to_webhook(
        endpoint: &str,
        auth_token: Option<&str>,
        event: &AuditEvent,
    ) -> Result<()> {
        let client = reqwest::Client::new();

        let mut request = client
            .post(endpoint)
            .header("Content-Type", "application/json")
            .json(event);

        if let Some(token) = auth_token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .map_err(|e| MaxioError::InternalError(format!("Failed to send webhook: {e}")))?;

        if !response.status().is_success() {
            return Err(MaxioError::InternalError(format!(
                "Webhook returned error status: {}",
                response.status()
            )));
        }

        Ok(())
    }

    async fn send_to_kafka(brokers: &[String], topic: &str, event: &AuditEvent) -> Result<()> {
        let json = serde_json::to_string(event)
            .map_err(|e| MaxioError::InternalError(format!("Failed to serialize audit event: {e}")))?;

        let _brokers_str = brokers.join(",");
        let _topic = topic;
        let _payload = json;

        Err(MaxioError::NotImplemented(
            "Kafka audit target requires rdkafka dependency".to_string(),
        ))
    }
}
