use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use maxio_common::error::{MaxioError, Result};
use maxio_storage::erasure::{ErasureConfig, decode_block, encode_block};
use serde::{Deserialize, Serialize};

const META_FILE_NAME: &str = "xl.meta";
const DATA_PART_FILE_NAME: &str = "part.1";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum HealShardState {
    Healthy,
    Missing,
    Corrupted,
    Outdated,
    Repaired,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealResultItem {
    pub disk_index: usize,
    pub before: HealShardState,
    pub after: HealShardState,
    pub bytes_repaired: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealResult {
    pub bucket: String,
    pub object: String,
    pub read_quorum: usize,
    pub write_quorum: usize,
    pub bytes_done: u64,
    pub healed: bool,
    pub items: Vec<HealResultItem>,
}

#[derive(Debug, Clone)]
pub struct HealEngine {
    disk_paths: Vec<PathBuf>,
    erasure: ErasureConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErasureMeta {
    version: String,
    size: i64,
    etag: String,
    content_type: String,
    metadata: HashMap<String, String>,
    erasure: ErasureMetaInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErasureMetaInfo {
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
    total_size: i64,
    block_checksums: Vec<String>,
}

#[derive(Debug)]
struct MetaObservation {
    disk_index: usize,
    meta: Option<ErasureMeta>,
    meta_signature: Option<String>,
    state: HealShardState,
    error: Option<String>,
}

impl HealEngine {
    pub fn new(disk_paths: Vec<PathBuf>, erasure: ErasureConfig) -> Result<Self> {
        if disk_paths.len() != erasure.total_shards() {
            return Err(MaxioError::InvalidArgument(format!(
                "invalid disk count for healing: expected {}, got {}",
                erasure.total_shards(),
                disk_paths.len()
            )));
        }

        Ok(Self { disk_paths, erasure })
    }

    pub async fn heal_object(&self, bucket: &str, object: &str) -> Result<HealResult> {
        let observations = self.read_meta_from_all_disks(bucket, object).await;
        let (canonical_meta, canonical_signature, read_quorum) =
            self.select_canonical_meta(&observations)?;

        let mut items = observations
            .iter()
            .map(|observation| HealResultItem {
                disk_index: observation.disk_index,
                before: observation.state,
                after: observation.state,
                bytes_repaired: 0,
                error: observation.error.clone(),
            })
            .collect::<Vec<_>>();

        let mut repair_targets = HashSet::new();
        for observation in &observations {
            match (&observation.meta_signature, observation.state) {
                (Some(signature), HealShardState::Healthy) if signature == &canonical_signature => {}
                _ => {
                    repair_targets.insert(observation.disk_index);
                    items[observation.disk_index].before = match observation.state {
                        HealShardState::Healthy => HealShardState::Outdated,
                        other => other,
                    };
                    items[observation.disk_index].after = HealShardState::Outdated;
                }
            }
        }

        let block_config = ErasureConfig {
            data_shards: canonical_meta.erasure.data_shards,
            parity_shards: canonical_meta.erasure.parity_shards,
            block_size: canonical_meta.erasure.block_size,
        };
        let block_count = object_block_count(&canonical_meta);

        if block_config.total_shards() != self.disk_paths.len() {
            return Err(MaxioError::InternalError(format!(
                "metadata shard configuration mismatch for {bucket}/{object}: meta shards {}, local disks {}",
                block_config.total_shards(),
                self.disk_paths.len()
            )));
        }

        let mut bytes_done = 0_u64;

        for block_index in 0..block_count {
            let mut shards = Vec::with_capacity(block_config.total_shards());
            let mut available = 0_usize;

            for disk_index in 0..self.disk_paths.len() {
                let is_canonical = match observations
                    .get(disk_index)
                    .and_then(|entry| entry.meta_signature.as_ref())
                {
                    Some(signature) => signature == &canonical_signature,
                    None => false,
                };

                if !is_canonical {
                    shards.push(None);
                    continue;
                }

                let part_path = self.block_part_path(disk_index, bucket, object, block_index);
                match tokio::fs::read(&part_path).await {
                    Ok(bytes) => {
                        available += 1;
                        shards.push(Some(bytes));
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        repair_targets.insert(disk_index);
                        items[disk_index].before = HealShardState::Outdated;
                        items[disk_index].after = HealShardState::Outdated;
                        shards.push(None);
                    }
                    Err(err) => {
                        repair_targets.insert(disk_index);
                        items[disk_index].before = HealShardState::Corrupted;
                        items[disk_index].after = HealShardState::Outdated;
                        items[disk_index].error = Some(err.to_string());
                        shards.push(None);
                    }
                }
            }

            if available < block_config.data_shards {
                return Err(MaxioError::InternalError(format!(
                    "insufficient shard quorum for {bucket}/{object}, block {block_index}: have {}, need {}",
                    available, block_config.data_shards
                )));
            }

            let decoded = decode_block(shards, &block_config)?;
            let expected_block_size = expected_block_size(
                block_index,
                canonical_meta.erasure.total_size,
                block_config.block_size,
            )?;

            if decoded.len() < expected_block_size {
                return Err(MaxioError::InternalError(format!(
                    "decoded block too short for {bucket}/{object}, block {block_index}: {} < {}",
                    decoded.len(),
                    expected_block_size
                )));
            }

            let encoded = encode_block(&decoded[..expected_block_size], &block_config)?;

            for &disk_index in &repair_targets {
                let part_path = self.block_part_path(disk_index, bucket, object, block_index);
                if let Some(parent) = part_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                let encoded_shard = encoded.get(disk_index).ok_or_else(|| {
                    MaxioError::InternalError(format!(
                        "encoded shard index out of range for {bucket}/{object}: index {}, shards {}",
                        disk_index,
                        encoded.len()
                    ))
                })?;

                match tokio::fs::write(&part_path, encoded_shard).await {
                    Ok(()) => {
                        items[disk_index].after = HealShardState::Repaired;
                        let repaired_bytes = u64::try_from(encoded_shard.len()).map_err(|_| {
                            MaxioError::InternalError(
                                "repaired shard length overflows u64".to_string(),
                            )
                        })?;
                        items[disk_index].bytes_repaired += repaired_bytes;
                        bytes_done += repaired_bytes;
                    }
                    Err(err) => {
                        items[disk_index].after = HealShardState::Failed;
                        items[disk_index].error = Some(err.to_string());
                    }
                }
            }
        }

        let canonical_meta_bytes = serde_json::to_vec(&canonical_meta).map_err(|err| {
            MaxioError::InternalError(format!(
                "failed to serialize canonical metadata for {bucket}/{object}: {err}"
            ))
        })?;

        for &disk_index in &repair_targets {
            if items[disk_index].after == HealShardState::Failed {
                continue;
            }

            let meta_path = self.meta_path(disk_index, bucket, object);
            if let Some(parent) = meta_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            if let Err(err) = tokio::fs::write(meta_path, &canonical_meta_bytes).await {
                items[disk_index].after = HealShardState::Failed;
                items[disk_index].error = Some(err.to_string());
            }
        }

        let healed = items
            .iter()
            .any(|item| item.before != item.after && item.after == HealShardState::Repaired);

        Ok(HealResult {
            bucket: bucket.to_string(),
            object: object.to_string(),
            read_quorum,
            write_quorum: block_config.data_shards,
            bytes_done,
            healed,
            items,
        })
    }

    #[allow(non_snake_case)]
    pub async fn healObject(&self, bucket: &str, object: &str) -> Result<HealResult> {
        self.heal_object(bucket, object).await
    }

    pub async fn heal_bucket(&self, bucket: &str) -> Result<Vec<HealResult>> {
        let objects = self.collect_bucket_objects(bucket).await?;
        let mut objects = objects.into_iter().collect::<Vec<_>>();
        objects.sort_unstable();

        let mut results = Vec::with_capacity(objects.len());
        for object in objects {
            let healed = self.heal_object(bucket, &object).await?;
            results.push(healed);
        }

        Ok(results)
    }

    #[allow(non_snake_case)]
    pub async fn healBucket(&self, bucket: &str) -> Result<Vec<HealResult>> {
        self.heal_bucket(bucket).await
    }

    async fn read_meta_from_all_disks(&self, bucket: &str, object: &str) -> Vec<MetaObservation> {
        let mut observations = Vec::with_capacity(self.disk_paths.len());

        for disk_index in 0..self.disk_paths.len() {
            let meta_path = self.meta_path(disk_index, bucket, object);
            let observation = match tokio::fs::read(&meta_path).await {
                Ok(bytes) => match serde_json::from_slice::<ErasureMeta>(&bytes) {
                    Ok(meta) => {
                        let signature = meta_signature(&meta);
                        MetaObservation {
                            disk_index,
                            meta: Some(meta),
                            meta_signature: signature,
                            state: HealShardState::Healthy,
                            error: None,
                        }
                    }
                    Err(err) => MetaObservation {
                        disk_index,
                        meta: None,
                        meta_signature: None,
                        state: HealShardState::Corrupted,
                        error: Some(err.to_string()),
                    },
                },
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => MetaObservation {
                    disk_index,
                    meta: None,
                    meta_signature: None,
                    state: HealShardState::Missing,
                    error: None,
                },
                Err(err) => MetaObservation {
                    disk_index,
                    meta: None,
                    meta_signature: None,
                    state: HealShardState::Corrupted,
                    error: Some(err.to_string()),
                },
            };
            observations.push(observation);
        }

        observations
    }

    fn select_canonical_meta(
        &self,
        observations: &[MetaObservation],
    ) -> Result<(ErasureMeta, String, usize)> {
        let mut by_signature: HashMap<String, (usize, ErasureMeta)> = HashMap::new();

        for observation in observations {
            if let (Some(signature), Some(meta)) =
                (observation.meta_signature.clone(), observation.meta.clone())
            {
                let entry = by_signature.entry(signature).or_insert((0, meta));
                entry.0 += 1;
            }
        }

        let mut selected: Option<(String, usize, ErasureMeta)> = None;
        for (signature, (count, meta)) in by_signature {
            match selected {
                Some((_, best_count, _)) if count <= best_count => {}
                _ => {
                    selected = Some((signature, count, meta));
                }
            }
        }

        let (signature, count, meta) = selected.ok_or_else(|| {
            MaxioError::InternalError("missing metadata quorum for healing".to_string())
        })?;

        if count < self.erasure.data_shards {
            return Err(MaxioError::InternalError(format!(
                "metadata read quorum not met: have {}, need {}",
                count, self.erasure.data_shards
            )));
        }

        Ok((meta, signature, count))
    }

    fn meta_path(&self, disk_index: usize, bucket: &str, object: &str) -> PathBuf {
        self.disk_paths[disk_index]
            .join(bucket)
            .join(object)
            .join(META_FILE_NAME)
    }

    fn block_part_path(&self, disk_index: usize, bucket: &str, object: &str, block_index: usize) -> PathBuf {
        self.disk_paths[disk_index]
            .join(bucket)
            .join(object)
            .join(format!("block_{block_index}"))
            .join(DATA_PART_FILE_NAME)
    }

    async fn collect_bucket_objects(&self, bucket: &str) -> Result<HashSet<String>> {
        let mut objects = HashSet::new();

        for disk_path in &self.disk_paths {
            let bucket_root = disk_path.join(bucket);
            if !path_exists(&bucket_root).await {
                continue;
            }

            let mut stack = vec![bucket_root.clone()];
            while let Some(current_dir) = stack.pop() {
                let mut entries = match tokio::fs::read_dir(&current_dir).await {
                    Ok(entries) => entries,
                    Err(_) => continue,
                };

                while let Some(entry) = entries.next_entry().await? {
                    let file_type = entry.file_type().await?;
                    let path = entry.path();
                    if file_type.is_dir() {
                        stack.push(path);
                        continue;
                    }

                    if !file_type.is_file() {
                        continue;
                    }

                    let file_name = entry.file_name();
                    if file_name.to_string_lossy() != META_FILE_NAME {
                        continue;
                    }

                    if let Some(parent) = path.parent()
                        && let Ok(relative) = parent.strip_prefix(&bucket_root)
                    {
                        let key = path_to_object_key(relative);
                        if !key.is_empty() {
                            objects.insert(key);
                        }
                    }
                }
            }
        }

        Ok(objects)
    }
}

fn meta_signature(meta: &ErasureMeta) -> Option<String> {
    Some(format!(
        "{}:{}:{}:{}:{}:{}:{}",
        meta.version,
        meta.size,
        meta.etag,
        meta.erasure.data_shards,
        meta.erasure.parity_shards,
        meta.erasure.block_size,
        meta.erasure.block_checksums.join(",")
    ))
}

fn object_block_count(meta: &ErasureMeta) -> usize {
    if !meta.erasure.block_checksums.is_empty() {
        return meta.erasure.block_checksums.len();
    }

    let total_size = match usize::try_from(meta.erasure.total_size) {
        Ok(size) => size,
        Err(_) => 0,
    };
    if total_size == 0 {
        return 1;
    }

    total_size.div_ceil(meta.erasure.block_size)
}

fn expected_block_size(block_index: usize, total_size: i64, block_size: usize) -> Result<usize> {
    if total_size <= 0 {
        return Ok(0);
    }
    let total_size = usize::try_from(total_size)
        .map_err(|_| MaxioError::InternalError("invalid object size in metadata".to_string()))?;
    let start = block_index
        .checked_mul(block_size)
        .ok_or_else(|| MaxioError::InternalError("block offset overflow".to_string()))?;

    Ok(std::cmp::min(block_size, total_size.saturating_sub(start)))
}

async fn path_exists(path: &Path) -> bool {
    tokio::fs::metadata(path).await.is_ok()
}

fn path_to_object_key(path: &Path) -> String {
    let key = path.to_string_lossy().to_string();
    key.replace(std::path::MAIN_SEPARATOR, "/")
}
