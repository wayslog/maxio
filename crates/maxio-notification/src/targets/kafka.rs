use async_trait::async_trait;
use maxio_common::error::{MaxioError, Result};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::Duration;

use crate::{system::NotificationTarget, types::S3Event};

pub struct KafkaTarget {
    producer: FutureProducer,
    topic: String,
}

impl KafkaTarget {
    pub fn new(brokers: &str, topic: String) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|err| {
                MaxioError::InternalError(format!("failed to create Kafka producer: {err}"))
            })?;

        Ok(Self { producer, topic })
    }

    pub fn with_config(config: ClientConfig, topic: String) -> Result<Self> {
        let producer: FutureProducer = config.create().map_err(|err| {
            MaxioError::InternalError(format!("failed to create Kafka producer: {err}"))
        })?;

        Ok(Self { producer, topic })
    }
}

#[async_trait]
impl NotificationTarget for KafkaTarget {
    async fn send(&self, event: &S3Event) -> Result<()> {
        let payload = serde_json::to_string(event).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize event: {err}"))
        })?;

        let key = format!("{}:{}", event.bucket.name, event.object.key);
        let record = FutureRecord::to(&self.topic)
            .payload(&payload)
            .key(&key);

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(err, _)| {
                MaxioError::InternalError(format!("failed to send Kafka message: {err}"))
            })?;

        Ok(())
    }
}
