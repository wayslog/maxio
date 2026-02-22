use maxio_common::error::{MaxioError, Result};
use maxio_storage::traits::ObjectLayer;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ReplicationJobConfig {
    pub source_bucket: String,
    pub target_bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub delete_after_replication: bool,
}

impl ReplicationJobConfig {
    pub fn validate(&self) -> Result<()> {
        if self.source_bucket.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "replication job source_bucket is required".to_string(),
            ));
        }
        if self.target_bucket.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "replication job target_bucket is required".to_string(),
            ));
        }
        if self.source_bucket == self.target_bucket && self.prefix.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "replication job cannot replicate bucket to itself without prefix filter".to_string(),
            ));
        }
        Ok(())
    }
}

pub async fn collect_keys_for_replication(
    object_layer: &dyn ObjectLayer,
    config: &ReplicationJobConfig,
) -> Result<Vec<String>> {
    config.validate()?;
    let mut marker = String::new();
    let mut keys = Vec::new();

    loop {
        let page = object_layer
            .list_objects(&config.source_bucket, &config.prefix, &marker, "", 1000)
            .await?;

        keys.extend(page.objects.into_iter().map(|object| object.key));

        if !page.is_truncated {
            break;
        }

        marker = match page.next_marker {
            Some(next_marker) => next_marker,
            None => break,
        };
    }

    Ok(keys)
}

pub async fn replicate_object(
    object_layer: &dyn ObjectLayer,
    config: &ReplicationJobConfig,
    key: &str,
) -> Result<()> {
    let (info, data) = object_layer
        .get_object(&config.source_bucket, key, None)
        .await?;

    object_layer
        .put_object(
            &config.target_bucket,
            key,
            data,
            Some(&info.content_type),
            info.metadata,
            None,
        )
        .await?;

    if config.delete_after_replication {
        object_layer
            .delete_object(&config.source_bucket, key)
            .await?;
    }

    Ok(())
}
