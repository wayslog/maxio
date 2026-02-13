use std::path::{Path, PathBuf};

use maxio_common::error::{MaxioError, Result};

use crate::erasure::ErasureConfig;
use crate::xl::storage::XlStorage;

#[derive(Debug, Clone)]
pub struct DiskShard {
    pub(crate) path: PathBuf,
    pub(crate) storage: XlStorage,
}

#[derive(Debug, Clone)]
pub struct ErasureStorage {
    config: ErasureConfig,
    shards: Vec<DiskShard>,
}

impl ErasureStorage {
    pub async fn new(disk_paths: Vec<PathBuf>, config: ErasureConfig) -> Result<Self> {
        if disk_paths.len() != config.total_shards() {
            return Err(MaxioError::InvalidArgument(format!(
                "invalid disk count: expected {}, got {}",
                config.total_shards(),
                disk_paths.len()
            )));
        }

        let mut shards = Vec::with_capacity(disk_paths.len());
        for path in disk_paths {
            let storage = XlStorage::new(path.clone()).await?;
            shards.push(DiskShard { path, storage });
        }

        Ok(Self { config, shards })
    }

    pub fn config(&self) -> &ErasureConfig {
        &self.config
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shard_path(&self, index: usize) -> Option<&Path> {
        self.shards.get(index).map(|shard| shard.path.as_path())
    }

    pub fn shard_storage(&self, index: usize) -> Option<&XlStorage> {
        self.shards.get(index).map(|shard| &shard.storage)
    }

    pub fn shards(&self) -> &[DiskShard] {
        &self.shards
    }
}
