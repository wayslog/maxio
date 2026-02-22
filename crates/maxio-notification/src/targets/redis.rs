use async_trait::async_trait;
use maxio_common::error::{MaxioError, Result};
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;

use crate::{system::NotificationTarget, types::S3Event};

pub struct RedisTarget {
    conn: MultiplexedConnection,
    channel: String,
}

impl RedisTarget {
    pub async fn new(url: &str, channel: String) -> Result<Self> {
        let client = redis::Client::open(url).map_err(|err| {
            MaxioError::InternalError(format!("failed to create Redis client: {err}"))
        })?;

        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| {
                MaxioError::InternalError(format!("failed to connect to Redis: {err}"))
            })?;

        Ok(Self { conn, channel })
    }

    pub fn with_connection(conn: MultiplexedConnection, channel: String) -> Self {
        Self { conn, channel }
    }
}

#[async_trait]
impl NotificationTarget for RedisTarget {
    async fn send(&self, event: &S3Event) -> Result<()> {
        let payload = serde_json::to_string(event).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize event: {err}"))
        })?;

        let mut conn = self.conn.clone();
        conn.publish::<_, _, ()>(&self.channel, &payload)
            .await
            .map_err(|err| {
                MaxioError::InternalError(format!("failed to publish Redis message: {err}"))
            })?;

        Ok(())
    }
}
