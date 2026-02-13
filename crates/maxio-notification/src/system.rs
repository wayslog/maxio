use std::collections::HashMap;

use async_trait::async_trait;
use maxio_common::error::Result;
use tracing::warn;

use crate::{
    store::NotificationStore,
    types::{FilterRules, NotificationConfiguration, S3Event},
};

#[async_trait]
pub trait NotificationTarget: Send + Sync {
    async fn send(&self, event: &S3Event) -> Result<()>;
}

pub struct NotificationSys {
    store: NotificationStore,
    targets: HashMap<String, Box<dyn NotificationTarget>>,
}

impl NotificationSys {
    pub fn new(store: NotificationStore) -> Self {
        Self {
            store,
            targets: HashMap::new(),
        }
    }

    pub fn register_target(&mut self, name: String, target: Box<dyn NotificationTarget>) {
        self.targets.insert(name, target);
    }

    pub async fn notify(&self, bucket: &str, event: S3Event) -> Result<()> {
        let config = self.get_config(bucket).await?;

        for queue in &config.queue_configurations {
            if !event_matches(&queue.events, &event.event_name)
                || !filter_matches(queue.filter.as_ref(), &event.object.key)
            {
                continue;
            }

            let Some(target_name) = target_name_from_arn(&queue.queue_arn) else {
                warn!(queue_arn = %queue.queue_arn, "invalid queue target arn");
                continue;
            };
            dispatch_target(self.targets.get(target_name), target_name, &event).await;
        }

        for topic in &config.topic_configurations {
            if !event_matches(&topic.events, &event.event_name)
                || !filter_matches(topic.filter.as_ref(), &event.object.key)
            {
                continue;
            }

            let Some(target_name) = target_name_from_arn(&topic.topic_arn) else {
                warn!(topic_arn = %topic.topic_arn, "invalid topic target arn");
                continue;
            };
            dispatch_target(self.targets.get(target_name), target_name, &event).await;
        }

        for lambda in &config.lambda_configurations {
            if !event_matches(&lambda.events, &event.event_name)
                || !filter_matches(lambda.filter.as_ref(), &event.object.key)
            {
                continue;
            }

            let Some(target_name) = target_name_from_arn(&lambda.lambda_arn) else {
                warn!(lambda_arn = %lambda.lambda_arn, "invalid lambda target arn");
                continue;
            };
            dispatch_target(self.targets.get(target_name), target_name, &event).await;
        }

        Ok(())
    }

    pub async fn get_config(&self, bucket: &str) -> Result<NotificationConfiguration> {
        self.store.get_config(bucket).await
    }

    pub async fn set_config(&self, bucket: &str, config: NotificationConfiguration) -> Result<()> {
        self.store.set_config(bucket, &config).await
    }

    pub async fn delete_config(&self, bucket: &str) -> Result<()> {
        self.store.delete_config(bucket).await
    }
}

async fn dispatch_target(
    target: Option<&Box<dyn NotificationTarget>>,
    target_name: &str,
    event: &S3Event,
) {
    let Some(target) = target else {
        warn!(
            target = target_name,
            "notification target is not registered"
        );
        return;
    };

    if let Err(err) = target.send(event).await {
        warn!(target = target_name, error = %err, "failed to send notification event");
    }
}

fn target_name_from_arn(arn: &str) -> Option<&str> {
    arn.rsplit(':').next().filter(|name| !name.is_empty())
}

fn event_matches(patterns: &[String], event_name: &str) -> bool {
    patterns.iter().any(|pattern| {
        if pattern == event_name {
            return true;
        }

        pattern
            .strip_suffix('*')
            .is_some_and(|prefix| event_name.starts_with(prefix))
    })
}

fn filter_matches(filter: Option<&FilterRules>, key: &str) -> bool {
    let Some(filter) = filter else {
        return true;
    };

    if let Some(prefix) = filter.prefix.as_deref() {
        if !key.starts_with(prefix) {
            return false;
        }
    }

    if let Some(suffix) = filter.suffix.as_deref() {
        if !key.ends_with(suffix) {
            return false;
        }
    }

    true
}
