use std::path::PathBuf;

use maxio_common::error::{MaxioError, Result};
use tokio::fs;

use crate::types::LifecycleConfiguration;

const LIFECYCLE_FILE_NAME: &str = ".lifecycle.json";

#[derive(Debug, Clone)]
pub struct LifecycleStore {
    root: PathBuf,
}

impl LifecycleStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub async fn get_config(&self, bucket: &str) -> Result<Option<LifecycleConfiguration>> {
        self.ensure_bucket_dir(bucket).await?;
        let path = self.config_path(bucket);
        match fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|err| {
                MaxioError::InternalError(format!(
                    "failed to parse bucket lifecycle config {}: {err}",
                    path.display()
                ))
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(MaxioError::Io(err)),
        }
    }

    pub async fn set_config(&self, bucket: &str, config: &LifecycleConfiguration) -> Result<()> {
        self.ensure_bucket_dir(bucket).await?;
        let path = self.config_path(bucket);
        let bytes = serde_json::to_vec_pretty(config).map_err(|err| {
            MaxioError::InternalError(format!(
                "failed to serialize bucket lifecycle config {}: {err}",
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

    pub async fn list_configured_buckets(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut entries = fs::read_dir(&self.root).await?;
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if !metadata.is_dir() {
                continue;
            }

            let bucket = entry.file_name().to_string_lossy().to_string();
            let lifecycle_file = entry.path().join(LIFECYCLE_FILE_NAME);
            if fs::try_exists(&lifecycle_file).await? {
                out.push(bucket);
            }
        }
        out.sort();
        Ok(out)
    }

    fn config_path(&self, bucket: &str) -> PathBuf {
        self.bucket_dir(bucket).join(LIFECYCLE_FILE_NAME)
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
        if !metadata.is_dir() {
            return Err(MaxioError::BucketNotFound(bucket.to_string()));
        }
        Ok(())
    }
}
