use async_trait::async_trait;
use maxio_common::error::{MaxioError, Result};

use crate::{system::NotificationTarget, types::S3Event};

pub struct WebhookTarget {
    endpoint: String,
    client: reqwest::Client,
}

impl WebhookTarget {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            client: reqwest::Client::new(),
        }
    }

    pub async fn send(&self, event: &S3Event) -> Result<()> {
        let response = self
            .client
            .post(&self.endpoint)
            .json(event)
            .send()
            .await
            .map_err(|err| {
                MaxioError::InternalError(format!(
                    "failed to send webhook notification to {}: {err}",
                    self.endpoint
                ))
            })?;

        if !response.status().is_success() {
            return Err(MaxioError::InternalError(format!(
                "webhook notification target {} returned status {}",
                self.endpoint,
                response.status()
            )));
        }

        Ok(())
    }
}

#[async_trait]
impl NotificationTarget for WebhookTarget {
    async fn send(&self, event: &S3Event) -> Result<()> {
        Self::send(self, event).await
    }
}
