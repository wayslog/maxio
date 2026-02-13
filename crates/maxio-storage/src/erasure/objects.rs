use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use maxio_common::error::{MaxioError, Result};
use maxio_common::types::{BucketInfo, ObjectInfo};
use md5::Md5;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::fs;

use md5::Digest as _;

use crate::erasure::{ErasureConfig, ErasureInfo, decode_block, encode_block};
use crate::erasure::storage::ErasureStorage;
use crate::traits::{ListObjectsResult, ObjectLayer};

const META_FILE_NAME: &str = "xl.meta";
const DATA_PART_FILE_NAME: &str = "part.1";
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

#[derive(Debug, Clone)]
pub struct ErasureObjectLayer {
    storage: ErasureStorage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErasureMeta {
    version: String,
    size: i64,
    etag: String,
    content_type: String,
    mod_time: DateTime<Utc>,
    metadata: HashMap<String, String>,
    erasure: ErasureInfo,
}

impl ErasureObjectLayer {
    pub async fn new(disk_paths: Vec<PathBuf>, config: ErasureConfig) -> Result<Self> {
        let storage = ErasureStorage::new(disk_paths, config).await?;
        Ok(Self { storage })
    }

    fn object_path(&self, shard_idx: usize, bucket: &str, key: &str) -> Result<PathBuf> {
        let shard_root = self
            .storage
            .shard_path(shard_idx)
            .ok_or_else(|| MaxioError::InternalError(format!("invalid shard index: {shard_idx}")))?;
        Ok(shard_root.join(bucket).join(key))
    }

    fn block_part_path(&self, shard_idx: usize, bucket: &str, key: &str, block_idx: usize) -> Result<PathBuf> {
        Ok(self
            .object_path(shard_idx, bucket, key)?
            .join(format!("block_{block_idx}"))
            .join(DATA_PART_FILE_NAME))
    }

    async fn ensure_bucket_exists_for_quorum(&self, bucket: &str) -> Result<()> {
        let mut available = 0_usize;
        for shard in self.storage.shards() {
            match fs::metadata(shard.path.join(bucket)).await {
                Ok(metadata) if metadata.is_dir() => available += 1,
                Ok(_) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {}
            }
        }

        if available < self.storage.config().data_shards {
            return Err(MaxioError::BucketNotFound(bucket.to_string()));
        }

        Ok(())
    }

    async fn write_meta_to_quorum(&self, bucket: &str, key: &str, meta: &ErasureMeta) -> Result<()> {
        let meta_bytes = serde_json::to_vec(meta)
            .map_err(|err| MaxioError::InternalError(format!("failed to serialize xl.meta: {err}")))?;
        let mut success = 0_usize;

        for shard_idx in 0..self.storage.shard_count() {
            let object_path = self.object_path(shard_idx, bucket, key)?;
            if fs::create_dir_all(&object_path).await.is_err() {
                continue;
            }

            if fs::write(object_path.join(META_FILE_NAME), &meta_bytes)
                .await
                .is_ok()
            {
                success += 1;
            }
        }

        if success < self.storage.config().data_shards {
            return Err(MaxioError::InternalError(format!(
                "failed to write metadata quorum: wrote {}, need {}",
                success,
                self.storage.config().data_shards
            )));
        }

        Ok(())
    }

    async fn read_meta_from_any(&self, bucket: &str, key: &str) -> Result<ErasureMeta> {
        let mut last_error: Option<MaxioError> = None;

        for shard_idx in 0..self.storage.shard_count() {
            let meta_path = self.object_path(shard_idx, bucket, key)?.join(META_FILE_NAME);
            match fs::read(meta_path).await {
                Ok(bytes) => {
                    let meta: ErasureMeta = serde_json::from_slice(&bytes).map_err(|err| {
                        MaxioError::InternalError(format!("failed to parse xl.meta: {err}"))
                    })?;
                    return Ok(meta);
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    last_error = Some(MaxioError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    });
                }
                Err(err) => {
                    last_error = Some(MaxioError::Io(err));
                }
            }
        }

        Err(last_error.unwrap_or(MaxioError::ObjectNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }))
    }

    fn meta_to_object_info(bucket: &str, key: &str, meta: &ErasureMeta) -> ObjectInfo {
        ObjectInfo {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: meta.size,
            etag: meta.etag.clone(),
            content_type: meta.content_type.clone(),
            last_modified: meta.mod_time,
            metadata: meta.metadata.clone(),
            version_id: None,
        }
    }
}

#[async_trait]
impl ObjectLayer for ErasureObjectLayer {
    async fn make_bucket(&self, bucket: &str) -> Result<()> {
        validate_bucket_name(bucket)?;

        let mut created = 0_usize;
        let mut already_exists = 0_usize;

        for shard in self.storage.shards() {
            match shard.storage.make_bucket(bucket).await {
                Ok(()) => created += 1,
                Err(MaxioError::BucketAlreadyExists(_)) => already_exists += 1,
                Err(err) => return Err(err),
            }
        }

        if already_exists == self.storage.shard_count() {
            return Err(MaxioError::BucketAlreadyExists(bucket.to_string()));
        }

        if created + already_exists < self.storage.config().data_shards {
            return Err(MaxioError::InternalError(format!(
                "insufficient buckets created: have {}, need {}",
                created + already_exists,
                self.storage.config().data_shards
            )));
        }

        Ok(())
    }

    async fn get_bucket_info(&self, bucket: &str) -> Result<BucketInfo> {
        validate_bucket_name(bucket)?;

        let mut last_not_found: Option<MaxioError> = None;
        for shard in self.storage.shards() {
            match shard.storage.get_bucket_info(bucket).await {
                Ok(info) => return Ok(info),
                Err(MaxioError::BucketNotFound(_)) => {
                    last_not_found = Some(MaxioError::BucketNotFound(bucket.to_string()));
                }
                Err(err) => return Err(err),
            }
        }

        Err(last_not_found.unwrap_or(MaxioError::BucketNotFound(bucket.to_string())))
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let mut last_error: Option<MaxioError> = None;
        for shard in self.storage.shards() {
            match shard.storage.list_buckets().await {
                Ok(buckets) => return Ok(buckets),
                Err(err) => last_error = Some(err),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            MaxioError::InternalError("no readable disks available for list_buckets".to_string())
        }))
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        validate_bucket_name(bucket)?;

        let mut deleted = 0_usize;
        let mut missing = 0_usize;

        for shard in self.storage.shards() {
            match shard.storage.delete_bucket(bucket).await {
                Ok(()) => deleted += 1,
                Err(MaxioError::BucketNotFound(_)) => missing += 1,
                Err(err) => return Err(err),
            }
        }

        if deleted == 0 && missing == self.storage.shard_count() {
            return Err(MaxioError::BucketNotFound(bucket.to_string()));
        }

        Ok(())
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<&str>,
        metadata: HashMap<String, String>,
    ) -> Result<ObjectInfo> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        self.ensure_bucket_exists_for_quorum(bucket).await?;

        for shard_idx in 0..self.storage.shard_count() {
            let object_path = self.object_path(shard_idx, bucket, key)?;
            match fs::remove_dir_all(&object_path).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {}
            }
        }

        let total_size = i64::try_from(data.len()).map_err(|_| {
            MaxioError::InvalidArgument(format!("object is too large to store: {bucket}/{key}"))
        })?;
        let etag = format!("{:x}", Md5::digest(&data));
        let mod_time = Utc::now();
        let content_type = content_type.unwrap_or(DEFAULT_CONTENT_TYPE).to_string();

        let config = self.storage.config();
        let block_count = if data.is_empty() {
            1
        } else {
            data.len().div_ceil(config.block_size)
        };
        let mut block_checksums = Vec::with_capacity(block_count);

        for block_idx in 0..block_count {
            let block = if data.is_empty() {
                &[][..]
            } else {
                let start = block_idx * config.block_size;
                let end = std::cmp::min(start + config.block_size, data.len());
                &data[start..end]
            };

            let checksum = format!("{:x}", Sha256::digest(block));
            block_checksums.push(checksum);

            let shards = encode_block(block, config)?;
            let mut successful_writes = 0_usize;

            for (shard_idx, shard) in shards.iter().enumerate() {
                let part_path = self.block_part_path(shard_idx, bucket, key, block_idx)?;
                if let Some(parent) = part_path.parent() {
                    if fs::create_dir_all(parent).await.is_err() {
                        continue;
                    }
                }

                if fs::write(part_path, shard).await.is_ok() {
                    successful_writes += 1;
                }
            }

            if successful_writes < config.data_shards {
                return Err(MaxioError::InternalError(format!(
                    "failed to write shard quorum for block {}: wrote {}, need {}",
                    block_idx, successful_writes, config.data_shards
                )));
            }
        }

        let erasure_info = ErasureInfo {
            data_shards: config.data_shards,
            parity_shards: config.parity_shards,
            block_size: config.block_size,
            total_size,
            block_checksums,
        };

        let meta = ErasureMeta {
            version: "1.0".to_string(),
            size: total_size,
            etag: etag.clone(),
            content_type: content_type.clone(),
            mod_time,
            metadata: metadata.clone(),
            erasure: erasure_info,
        };
        self.write_meta_to_quorum(bucket, key, &meta).await?;

        Ok(ObjectInfo {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: total_size,
            etag,
            content_type,
            last_modified: mod_time,
            metadata,
            version_id: None,
        })
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, Bytes)> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;

        let meta = self.read_meta_from_any(bucket, key).await?;
        let object_info = Self::meta_to_object_info(bucket, key, &meta);

        if meta.erasure.total_size == 0 {
            return Ok((object_info, Bytes::new()));
        }

        let total_size = usize::try_from(meta.erasure.total_size).map_err(|_| {
            MaxioError::InternalError("invalid total_size in erasure metadata".to_string())
        })?;
        let block_count = if meta.erasure.block_checksums.is_empty() {
            total_size.div_ceil(meta.erasure.block_size)
        } else {
            meta.erasure.block_checksums.len()
        };

        let block_config = ErasureConfig {
            data_shards: meta.erasure.data_shards,
            parity_shards: meta.erasure.parity_shards,
            block_size: meta.erasure.block_size,
        };

        let mut output = Vec::with_capacity(total_size);
        for block_idx in 0..block_count {
            let mut shards = Vec::with_capacity(block_config.total_shards());
            let mut available = 0_usize;

            for shard_idx in 0..block_config.total_shards() {
                let part_path = self.block_part_path(shard_idx, bucket, key, block_idx)?;
                match fs::read(part_path).await {
                    Ok(bytes) => {
                        available += 1;
                        shards.push(Some(bytes));
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        shards.push(None);
                    }
                    Err(_) => {
                        shards.push(None);
                    }
                }
            }

            if available < block_config.data_shards {
                return Err(MaxioError::InternalError(format!(
                    "insufficient shards for block {}: got {}, need {}",
                    block_idx, available, block_config.data_shards
                )));
            }

            let decoded = decode_block(shards, &block_config)?;
            let written = block_idx * block_config.block_size;
            let expected_block_size = std::cmp::min(block_config.block_size, total_size.saturating_sub(written));

            if decoded.len() < expected_block_size {
                return Err(MaxioError::InternalError(format!(
                    "decoded block {} too short: got {}, expected at least {}",
                    block_idx,
                    decoded.len(),
                    expected_block_size
                )));
            }

            let block_data = &decoded[..expected_block_size];
            let checksum = format!("{:x}", Sha256::digest(block_data));
            if let Some(expected_checksum) = meta.erasure.block_checksums.get(block_idx) {
                if &checksum != expected_checksum {
                    return Err(MaxioError::InternalError(format!(
                        "bitrot detected in block {}",
                        block_idx
                    )));
                }
            }

            output.extend_from_slice(block_data);
        }

        if output.len() > total_size {
            output.truncate(total_size);
        }

        Ok((object_info, Bytes::from(output)))
    }

    async fn get_object_info(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;

        let meta = self.read_meta_from_any(bucket, key).await?;
        Ok(Self::meta_to_object_info(bucket, key, &meta))
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;

        let mut removed = 0_usize;

        for shard_idx in 0..self.storage.shard_count() {
            let object_path = self.object_path(shard_idx, bucket, key)?;
            match fs::remove_dir_all(object_path).await {
                Ok(()) => removed += 1,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {}
            }
        }

        if removed == 0 {
            return Err(MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        Ok(())
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        delimiter: &str,
        max_keys: i32,
    ) -> Result<ListObjectsResult> {
        validate_bucket_name(bucket)?;

        let mut last_error: Option<MaxioError> = None;
        for shard in self.storage.shards() {
            match shard
                .storage
                .list_objects(bucket, prefix, marker, delimiter, max_keys)
                .await
            {
                Ok(result) => return Ok(result),
                Err(err) => last_error = Some(err),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            MaxioError::InternalError("no readable disks available for list_objects".to_string())
        }))
    }
}

fn validate_bucket_name(bucket: &str) -> Result<()> {
    if bucket.is_empty() || bucket == ".maxio.sys" || bucket.contains('/') || bucket.contains('\\') {
        return Err(MaxioError::InvalidBucketName(bucket.to_string()));
    }
    Ok(())
}

fn validate_object_key(key: &str) -> Result<()> {
    if key.is_empty() || key.contains('\\') {
        return Err(MaxioError::InvalidObjectName(key.to_string()));
    }

    let key_path = Path::new(key);
    if key_path.is_absolute() {
        return Err(MaxioError::InvalidObjectName(key.to_string()));
    }

    for component in key_path.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir | Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(MaxioError::InvalidObjectName(key.to_string()));
            }
        }
    }

    Ok(())
}
