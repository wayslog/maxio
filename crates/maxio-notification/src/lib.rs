pub mod store;
pub mod system;
pub mod targets;
pub mod types;

pub use store::NotificationStore;
pub use system::{NotificationSys, NotificationTarget};
pub use targets::webhook::WebhookTarget;
#[cfg(feature = "kafka")]
pub use targets::kafka::KafkaTarget;
#[cfg(feature = "amqp")]
pub use targets::amqp::AmqpTarget;
#[cfg(feature = "redis")]
pub use targets::redis::RedisTarget;
#[cfg(feature = "nats")]
pub use targets::nats::NatsTarget;
