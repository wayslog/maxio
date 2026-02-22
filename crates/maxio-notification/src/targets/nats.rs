use async_trait::async_trait;
use async_nats::Client;
use maxio_common::error::{MaxioError, Result};

use crate::{system::NotificationTarget, types::S3Event};

pub struct NatsTarget {
    client: Client,
    subject: String,
}

impl NatsTarget {
    pub async fn new(url: &str, subject: String) -> Result<Self> {
        let client = async_nats::connect(url).await.map_err(|err| {
            MaxioError::InternalError(format!("failed to connect to NATS: {err}"))
        })?;

        Ok(Self { client, subject })
    }

    pub fn with_client(client: Client, subject: String) -> Self {
        Self { client, subject }
    }
}

#[async_trait]
impl NotificationTarget for NatsTarget {
    async fn send(&self, event: &S3Event) -> Result<()> {
        let payload = serde_json::to_vec(event).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize event: {err}"))
        })?;

        self.client
            .publish(self.subject.clone(), payload.into())
            .await
            .map_err(|err| {
                MaxioError::InternalError(format!("failed to publish NATS message: {err}"))
            })?;

        Ok(())
    }
}
