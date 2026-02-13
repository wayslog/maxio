use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use maxio_common::error::{MaxioError, Result};
use maxio_common::types::{BucketInfo, ObjectInfo};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

use crate::traits::ListObjectsResult;

const SYS_DIR_NAME: &str = ".maxio.sys";
const META_FILE_NAME: &str = "xl.meta";
const DATA_PART_FILE_NAME: &str = "part.1";
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

#[derive(Debug, Clone)]
pub struct XlStorage {
    root_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct XlMeta {
    version: String,
    data_dir: String,
    size: i64,
    etag: String,
    content_type: String,
    mod_time: DateTime<Utc>,
    metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
enum ListEntry {
    Object(ObjectInfo),
    Prefix(String),
}

impl ListEntry {
    fn marker(&self) -> &str {
        match self {
            Self::Object(obj) => &obj.key,
            Self::Prefix(prefix) => prefix,
        }
    }
}

impl XlStorage {
    pub async fn new(root_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&root_dir).await?;
        fs::create_dir_all(root_dir.join(SYS_DIR_NAME)).await?;
        Ok(Self { root_dir })
    }

    pub async fn make_bucket(&self, bucket: &str) -> Result<()> {
        validate_bucket_name(bucket)?;
        let bucket_path = self.bucket_path(bucket);

        if is_existing_directory(&bucket_path).await? {
            return Err(MaxioError::BucketAlreadyExists(bucket.to_string()));
        }

        fs::create_dir_all(bucket_path).await?;
        Ok(())
    }

    pub async fn get_bucket_info(&self, bucket: &str) -> Result<BucketInfo> {
        validate_bucket_name(bucket)?;
        let bucket_path = self.bucket_path(bucket);
        let metadata = fs::metadata(&bucket_path)
            .await
            .map_err(|err| map_bucket_io_error(bucket, err))?;

        if !metadata.is_dir() {
            return Err(MaxioError::BucketNotFound(bucket.to_string()));
        }

        let created = filetime_to_utc(metadata.created().ok())
            .or_else(|| filetime_to_utc(metadata.modified().ok()))
            .unwrap_or_else(Utc::now);

        Ok(BucketInfo {
            name: bucket.to_string(),
            created,
        })
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let mut entries = fs::read_dir(&self.root_dir).await?;
        let mut buckets = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy().to_string();

            if name == SYS_DIR_NAME {
                continue;
            }

            let metadata = entry.metadata().await?;
            if !metadata.is_dir() {
                continue;
            }

            let created = filetime_to_utc(metadata.created().ok())
                .or_else(|| filetime_to_utc(metadata.modified().ok()))
                .unwrap_or_else(Utc::now);

            buckets.push(BucketInfo { name, created });
        }

        buckets.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(buckets)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        validate_bucket_name(bucket)?;
        let bucket_path = self.bucket_path(bucket);

        let mut entries = fs::read_dir(&bucket_path)
            .await
            .map_err(|err| map_bucket_io_error(bucket, err))?;

        if entries.next_entry().await?.is_some() {
            return Err(MaxioError::InvalidArgument(format!(
                "bucket is not empty: {bucket}"
            )));
        }

        fs::remove_dir(bucket_path)
            .await
            .map_err(|err| map_bucket_io_error(bucket, err))?;
        Ok(())
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<&str>,
        metadata: HashMap<String, String>,
    ) -> Result<ObjectInfo> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        ensure_bucket_exists(self, bucket).await?;

        let object_path = self.object_path(bucket, key);
        if is_existing_directory(&object_path).await? {
            fs::remove_dir_all(&object_path).await?;
        }

        let data_dir = Uuid::new_v4().to_string();
        let data_path = object_path.join(&data_dir);
        fs::create_dir_all(&data_path).await?;

        let size = i64::try_from(data.len()).map_err(|_| {
            MaxioError::InvalidArgument(format!("object is too large to store: {bucket}/{key}"))
        })?;
        let etag = format!("{:x}", Md5::digest(&data));
        let mod_time = Utc::now();
        let content_type = content_type.unwrap_or(DEFAULT_CONTENT_TYPE).to_string();

        let xl_meta = XlMeta {
            version: "1.0".to_string(),
            data_dir: data_dir.clone(),
            size,
            etag: etag.clone(),
            content_type: content_type.clone(),
            mod_time,
            metadata: metadata.clone(),
        };

        fs::write(data_path.join(DATA_PART_FILE_NAME), data).await?;
        let meta_json = serde_json::to_vec(&xl_meta)
            .map_err(|err| MaxioError::InternalError(format!("failed to serialize xl.meta: {err}")))?;
        fs::write(object_path.join(META_FILE_NAME), meta_json).await?;

        Ok(ObjectInfo {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size,
            etag,
            content_type,
            last_modified: mod_time,
            metadata,
            version_id: None,
        })
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, Bytes)> {
        let (object_info, xl_meta, object_path) = self.read_object(bucket, key).await?;
        let data_path = object_path.join(xl_meta.data_dir).join(DATA_PART_FILE_NAME);
        let data = fs::read(data_path)
            .await
            .map_err(|_| MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;

        Ok((object_info, Bytes::from(data)))
    }

    pub async fn get_object_info(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        let (object_info, _, _) = self.read_object(bucket, key).await?;
        Ok(object_info)
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        ensure_bucket_exists(self, bucket).await?;

        let object_path = self.object_path(bucket, key);
        if !is_existing_directory(&object_path).await? {
            return Err(MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        fs::remove_dir_all(object_path).await?;
        Ok(())
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        delimiter: &str,
        max_keys: i32,
    ) -> Result<ListObjectsResult> {
        validate_bucket_name(bucket)?;
        ensure_bucket_exists(self, bucket).await?;

        let bucket_path = self.bucket_path(bucket);
        let mut dirs = vec![bucket_path.clone()];
        let mut objects = Vec::new();

        while let Some(dir_path) = dirs.pop() {
            let mut entries = fs::read_dir(&dir_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let metadata = entry.metadata().await?;
                if metadata.is_dir() {
                    dirs.push(path);
                    continue;
                }

                if entry.file_name() != META_FILE_NAME {
                    continue;
                }

                let object_dir = match path.parent() {
                    Some(parent) => parent,
                    None => continue,
                };

                let rel = match object_dir.strip_prefix(&bucket_path) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let object_key = rel.to_string_lossy().replace('\\', "/");

                let meta_bytes = fs::read(path).await?;
                let xl_meta: XlMeta = serde_json::from_slice(&meta_bytes).map_err(|err| {
                    MaxioError::InternalError(format!("failed to parse xl.meta during list: {err}"))
                })?;

                objects.push(ObjectInfo {
                    bucket: bucket.to_string(),
                    key: object_key,
                    size: xl_meta.size,
                    etag: xl_meta.etag,
                    content_type: xl_meta.content_type,
                    last_modified: xl_meta.mod_time,
                    metadata: xl_meta.metadata,
                    version_id: None,
                });
            }
        }

        objects.sort_by(|a, b| a.key.cmp(&b.key));
        let mut filtered: Vec<ObjectInfo> = objects
            .into_iter()
            .filter(|obj| obj.key.starts_with(prefix))
            .filter(|obj| marker.is_empty() || obj.key.as_str() > marker)
            .collect();

        let mut entries = Vec::new();
        let mut prefixes = HashSet::new();

        if delimiter.is_empty() {
            for obj in filtered {
                entries.push(ListEntry::Object(obj));
            }
        } else {
            for obj in filtered.drain(..) {
                let suffix = &obj.key[prefix.len()..];
                if let Some(idx) = suffix.find(delimiter) {
                    let prefix_value = format!("{}{}", prefix, &suffix[..idx + delimiter.len()]);
                    prefixes.insert(prefix_value);
                } else {
                    entries.push(ListEntry::Object(obj));
                }
            }

            for prefix_value in prefixes {
                entries.push(ListEntry::Prefix(prefix_value));
            }
        }

        entries.sort_by(|a, b| a.marker().cmp(b.marker()));

        let limit = if max_keys > 0 {
            usize::try_from(max_keys).unwrap_or(usize::MAX)
        } else {
            entries.len()
        };
        let is_truncated = entries.len() > limit;
        let selected = if is_truncated {
            &entries[..limit]
        } else {
            &entries[..]
        };

        let mut out_objects = Vec::new();
        let mut out_prefixes = Vec::new();
        for entry in selected {
            match entry {
                ListEntry::Object(obj) => out_objects.push(obj.clone()),
                ListEntry::Prefix(prefix_value) => out_prefixes.push(prefix_value.clone()),
            }
        }

        Ok(ListObjectsResult {
            objects: out_objects,
            prefixes: out_prefixes,
            is_truncated,
            next_marker: selected.last().map(|entry| entry.marker().to_string()),
        })
    }

    fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.root_dir.join(bucket)
    }

    fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.bucket_path(bucket).join(key)
    }

    async fn read_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, XlMeta, PathBuf)> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        ensure_bucket_exists(self, bucket).await?;

        let object_path = self.object_path(bucket, key);
        let meta_path = object_path.join(META_FILE_NAME);
        let meta_bytes = fs::read(meta_path)
            .await
            .map_err(|_| MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;
        let xl_meta: XlMeta = serde_json::from_slice(&meta_bytes)
            .map_err(|err| MaxioError::InternalError(format!("failed to parse xl.meta: {err}")))?;

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: xl_meta.size,
            etag: xl_meta.etag.clone(),
            content_type: xl_meta.content_type.clone(),
            last_modified: xl_meta.mod_time,
            metadata: xl_meta.metadata.clone(),
            version_id: None,
        };

        Ok((object_info, xl_meta, object_path))
    }
}

fn validate_bucket_name(bucket: &str) -> Result<()> {
    if bucket.is_empty() || bucket == SYS_DIR_NAME || bucket.contains('/') || bucket.contains('\\') {
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

async fn ensure_bucket_exists(storage: &XlStorage, bucket: &str) -> Result<()> {
    let bucket_path = storage.bucket_path(bucket);
    if !is_existing_directory(&bucket_path).await? {
        return Err(MaxioError::BucketNotFound(bucket.to_string()));
    }
    Ok(())
}

async fn is_existing_directory(path: &Path) -> Result<bool> {
    match fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.is_dir()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(MaxioError::Io(err)),
    }
}

fn map_bucket_io_error(bucket: &str, err: std::io::Error) -> MaxioError {
    if err.kind() == std::io::ErrorKind::NotFound {
        MaxioError::BucketNotFound(bucket.to_string())
    } else {
        MaxioError::Io(err)
    }
}

fn filetime_to_utc(filetime: Option<std::time::SystemTime>) -> Option<DateTime<Utc>> {
    filetime.map(DateTime::<Utc>::from)
}
