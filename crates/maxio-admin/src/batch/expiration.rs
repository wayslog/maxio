use chrono::{Duration, Utc};
use maxio_common::{
    error::{MaxioError, Result},
    types::ObjectInfo,
};
use maxio_storage::traits::ObjectLayer;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ExpirationJobConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    pub older_than_days: i64,
}

impl ExpirationJobConfig {
    pub fn validate(&self) -> Result<()> {
        if self.bucket.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "expiration job bucket is required".to_string(),
            ));
        }
        if self.older_than_days < 0 {
            return Err(MaxioError::InvalidArgument(
                "expiration job older_than_days must be non-negative".to_string(),
            ));
        }
        Ok(())
    }
}

pub async fn collect_expired_keys(
    object_layer: &dyn ObjectLayer,
    config: &ExpirationJobConfig,
) -> Result<Vec<String>> {
    config.validate()?;
    let cutoff = Utc::now() - Duration::days(config.older_than_days);
    let mut marker = String::new();
    let mut keys = Vec::new();

    loop {
        let page = object_layer
            .list_objects(&config.bucket, &config.prefix, &marker, "", 1000)
            .await?;

        keys.extend(
            page.objects
                .into_iter()
                .filter(|object| is_expired(object, cutoff))
                .map(|object| object.key),
        );

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

fn is_expired(object: &ObjectInfo, cutoff: chrono::DateTime<Utc>) -> bool {
    object.last_modified <= cutoff
}
