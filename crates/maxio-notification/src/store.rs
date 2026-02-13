use std::path::PathBuf;

use maxio_common::error::{MaxioError, Result};
use tokio::fs;

use crate::types::NotificationConfiguration;

const NOTIFICATION_FILE_NAME: &str = ".notification.json";

#[derive(Debug, Clone)]
pub struct NotificationStore {
    root: PathBuf,
}

impl NotificationStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub async fn get_config(&self, bucket: &str) -> Result<NotificationConfiguration> {
        self.ensure_bucket_dir(bucket).await?;
        let path = self.config_path(bucket);
        match fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|err| {
                MaxioError::InternalError(format!(
                    "failed to parse bucket notification config {}: {err}",
                    path.display()
                ))
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Ok(NotificationConfiguration::default())
            }
            Err(err) => Err(MaxioError::Io(err)),
        }
    }

    pub async fn set_config(&self, bucket: &str, config: &NotificationConfiguration) -> Result<()> {
        self.ensure_bucket_dir(bucket).await?;
        let path = self.config_path(bucket);
        let bytes = serde_json::to_vec_pretty(config).map_err(|err| {
            MaxioError::InternalError(format!(
                "failed to serialize bucket notification config {}: {err}",
                path.display()
            ))
        })?;
        fs::write(path, bytes).await?;
        Ok(())
    }

    pub async fn delete_config(&self, bucket: &str) -> Result<()> {
        self.ensure_bucket_dir(bucket).await?;
        let path = self.config_path(bucket);
        match fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(MaxioError::Io(err)),
        }
    }

    fn config_path(&self, bucket: &str) -> PathBuf {
        self.bucket_dir(bucket).join(NOTIFICATION_FILE_NAME)
    }

    fn bucket_dir(&self, bucket: &str) -> PathBuf {
        self.root.join(bucket)
    }

    async fn ensure_bucket_dir(&self, bucket: &str) -> Result<()> {
        let bucket_dir = self.bucket_dir(bucket);
        let metadata = fs::metadata(&bucket_dir).await.map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                return MaxioError::BucketNotFound(bucket.to_string());
            }
            MaxioError::Io(err)
        })?;
        if !is_dir(&metadata) {
            return Err(MaxioError::BucketNotFound(bucket.to_string()));
        }
        Ok(())
    }
}

fn is_dir(metadata: &std::fs::Metadata) -> bool {
    metadata.is_dir()
}
