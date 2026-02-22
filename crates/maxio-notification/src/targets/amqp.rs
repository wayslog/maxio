use async_trait::async_trait;
use lapin::{
    options::BasicPublishOptions, BasicProperties, Channel, Connection, ConnectionProperties,
};
use maxio_common::error::{MaxioError, Result};

use crate::{system::NotificationTarget, types::S3Event};

pub struct AmqpTarget {
    channel: Channel,
    exchange: String,
    routing_key: String,
}

impl AmqpTarget {
    pub async fn new(uri: &str, exchange: String, routing_key: String) -> Result<Self> {
        let conn = Connection::connect(uri, ConnectionProperties::default())
            .await
            .map_err(|err| {
                MaxioError::InternalError(format!("failed to connect to AMQP broker: {err}"))
            })?;

        let channel = conn.create_channel().await.map_err(|err| {
            MaxioError::InternalError(format!("failed to create AMQP channel: {err}"))
        })?;

        Ok(Self {
            channel,
            exchange,
            routing_key,
        })
    }

    pub fn with_channel(channel: Channel, exchange: String, routing_key: String) -> Self {
        Self {
            channel,
            exchange,
            routing_key,
        }
    }
}

#[async_trait]
impl NotificationTarget for AmqpTarget {
    async fn send(&self, event: &S3Event) -> Result<()> {
        let payload = serde_json::to_vec(event).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize event: {err}"))
        })?;

        self.channel
            .basic_publish(
                &self.exchange,
                &self.routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2),
            )
            .await
            .map_err(|err| {
                MaxioError::InternalError(format!("failed to publish AMQP message: {err}"))
            })?
            .await
            .map_err(|err| {
                MaxioError::InternalError(format!("AMQP publish confirmation failed: {err}"))
            })?;

        Ok(())
    }
}
