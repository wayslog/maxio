use maxio_common::error::{MaxioError, Result};
use maxio_storage::traits::ObjectLayer;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct KeyRotationJobConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    pub new_key_id: Option<String>,
}

impl KeyRotationJobConfig {
    pub fn validate(&self) -> Result<()> {
        if self.bucket.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "key rotation job bucket is required".to_string(),
            ));
        }
        Ok(())
    }
}

pub async fn collect_keys_for_rotation(
    object_layer: &dyn ObjectLayer,
    config: &KeyRotationJobConfig,
) -> Result<Vec<String>> {
    config.validate()?;
    let mut marker = String::new();
    let mut keys = Vec::new();

    loop {
        let page = object_layer
            .list_objects(&config.bucket, &config.prefix, &marker, "", 1000)
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

pub async fn rotate_object_key(
    object_layer: &dyn ObjectLayer,
    config: &KeyRotationJobConfig,
    key: &str,
) -> Result<()> {
    let (info, data) = object_layer
        .get_object(&config.bucket, key, None)
        .await?;

    object_layer
        .put_object(
            &config.bucket,
            key,
            data,
            Some(&info.content_type),
            info.metadata,
            None,
        )
        .await?;

    Ok(())
}
