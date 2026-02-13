use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use maxio_common::error::{MaxioError, Result};
use maxio_common::types::{BucketInfo, ObjectEncryption, ObjectInfo};
use maxio_crypto::{MasterKey, cipher};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

use crate::traits::{
    CompletePart, GetEncryptionOptions, ListObjectsResult, MultipartUploadInfo, ObjectVersion,
    PartInfo, PutEncryptionOptions, VersioningState,
};

const SYS_DIR_NAME: &str = ".maxio.sys";
const CRYPTO_DIR_NAME: &str = ".crypto";
const MASTER_KEY_FILE_NAME: &str = "master.key";
const META_FILE_NAME: &str = "xl.meta";
const DATA_PART_FILE_NAME: &str = "part.1";
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
const MULTIPART_DIR_NAME: &str = ".multipart";
const MULTIPART_META_FILE_NAME: &str = "upload.json";
const VERSIONING_FILE_NAME: &str = ".versioning.json";
const VERSIONS_INDEX_FILE_NAME: &str = ".versions.json";
const NULL_VERSION_ID: &str = "null";

#[derive(Debug, Clone)]
pub struct XlStorage {
    root_dir: PathBuf,
    master_key: MasterKey,
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
    version_id: Option<String>,
    is_delete_marker: bool,
    encryption: Option<EncryptionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EncryptionInfo {
    algorithm: String,
    sse_type: String,
    key_md5: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionIndexEntry {
    version_id: String,
    is_delete_marker: bool,
    last_modified: DateTime<Utc>,
    etag: Option<String>,
    size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MultipartUploadMeta {
    key: String,
    content_type: Option<String>,
    metadata: HashMap<String, String>,
    initiated: DateTime<Utc>,
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
        let master_key = load_or_create_master_key(&root_dir).await?;
        Ok(Self {
            root_dir,
            master_key,
        })
    }

    pub async fn make_bucket(&self, bucket: &str) -> Result<()> {
        validate_bucket_name(bucket)?;
        let bucket_path = self.bucket_path(bucket);

        if is_existing_directory(&bucket_path).await? {
            return Err(MaxioError::BucketAlreadyExists(bucket.to_string()));
        }

        fs::create_dir_all(bucket_path).await?;
        self.set_bucket_versioning(bucket, VersioningState::Unversioned)
            .await?;
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

            if name == SYS_DIR_NAME || name == CRYPTO_DIR_NAME {
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

    pub async fn get_bucket_versioning(&self, bucket: &str) -> Result<VersioningState> {
        validate_bucket_name(bucket)?;
        ensure_bucket_exists(self, bucket).await?;
        self.read_bucket_versioning(bucket).await
    }

    pub async fn set_bucket_versioning(&self, bucket: &str, state: VersioningState) -> Result<()> {
        validate_bucket_name(bucket)?;
        let bucket_path = self.bucket_path(bucket);
        if !is_existing_directory(&bucket_path).await? {
            return Err(MaxioError::BucketNotFound(bucket.to_string()));
        }

        let bytes = serde_json::to_vec(&state).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize versioning state: {err}"))
        })?;
        fs::write(bucket_path.join(VERSIONING_FILE_NAME), bytes).await?;
        Ok(())
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<&str>,
        metadata: HashMap<String, String>,
        encryption: Option<PutEncryptionOptions>,
    ) -> Result<ObjectInfo> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        ensure_bucket_exists(self, bucket).await?;
        let state = self.read_bucket_versioning(bucket).await?;
        let size = i64::try_from(data.len()).map_err(|_| {
            MaxioError::InvalidArgument(format!("object is too large to store: {bucket}/{key}"))
        })?;
        let etag = format!("{:x}", Md5::digest(&data));
        let mod_time = Utc::now();
        let content_type = content_type.unwrap_or(DEFAULT_CONTENT_TYPE).to_string();

        match state {
            VersioningState::Unversioned => {
                let object_path = self.object_path(bucket, key);
                if is_existing_directory(&object_path).await? {
                    fs::remove_dir_all(&object_path).await?;
                }

                let data_dir = Uuid::new_v4().to_string();
                let data_path = object_path.join(&data_dir);
                fs::create_dir_all(&data_path).await?;
                let (object_key, encryption_info) =
                    self.resolve_put_encryption(bucket, key, None, encryption.as_ref())?;
                let stored_data = match object_key {
                    Some(object_key) => {
                        cipher::encrypt(&object_key, &data).map_err(map_crypto_error)?
                    }
                    None => data.to_vec(),
                };

                let xl_meta = XlMeta {
                    version: "1.0".to_string(),
                    data_dir: data_dir.clone(),
                    size,
                    etag: etag.clone(),
                    content_type: content_type.clone(),
                    mod_time,
                    metadata: metadata.clone(),
                    version_id: None,
                    is_delete_marker: false,
                    encryption: encryption_info,
                };

                fs::write(data_path.join(DATA_PART_FILE_NAME), stored_data).await?;
                self.write_xl_meta(&object_path.join(META_FILE_NAME), &xl_meta)
                    .await?;

                Ok(ObjectInfo {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    size,
                    etag,
                    content_type,
                    last_modified: mod_time,
                    metadata,
                    version_id: None,
                    encryption: xl_meta.encryption.clone().map(meta_encryption_to_object),
                })
            }
            VersioningState::Enabled | VersioningState::Suspended => {
                let object_path = self.object_path(bucket, key);
                let mut versions = self.ensure_versions_index(bucket, key).await?;

                let version_id = if state == VersioningState::Enabled {
                    Uuid::new_v4().to_string()
                } else {
                    NULL_VERSION_ID.to_string()
                };

                if state == VersioningState::Suspended {
                    versions.retain(|entry| entry.version_id != version_id);
                    self.remove_version_dir_if_exists(&object_path, &version_id)
                        .await?;
                }

                let data_dir = Uuid::new_v4().to_string();
                let version_path = object_path.join(&version_id);
                let data_path = version_path.join(&data_dir);
                fs::create_dir_all(&data_path).await?;
                let (object_key, encryption_info) = self.resolve_put_encryption(
                    bucket,
                    key,
                    Some(version_id.as_str()),
                    encryption.as_ref(),
                )?;
                let stored_data = match object_key {
                    Some(object_key) => {
                        cipher::encrypt(&object_key, &data).map_err(map_crypto_error)?
                    }
                    None => data.to_vec(),
                };

                let xl_meta = XlMeta {
                    version: "1.0".to_string(),
                    data_dir,
                    size,
                    etag: etag.clone(),
                    content_type: content_type.clone(),
                    mod_time,
                    metadata: metadata.clone(),
                    version_id: Some(version_id.clone()),
                    is_delete_marker: false,
                    encryption: encryption_info,
                };

                fs::write(data_path.join(DATA_PART_FILE_NAME), stored_data).await?;
                self.write_xl_meta(&version_path.join(META_FILE_NAME), &xl_meta)
                    .await?;

                versions.insert(
                    0,
                    VersionIndexEntry {
                        version_id: version_id.clone(),
                        is_delete_marker: false,
                        last_modified: mod_time,
                        etag: Some(etag.clone()),
                        size,
                    },
                );
                self.write_versions_index(&object_path, &versions).await?;

                Ok(ObjectInfo {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    size,
                    etag,
                    content_type,
                    last_modified: mod_time,
                    metadata,
                    version_id: Some(version_id),
                    encryption: xl_meta.encryption.clone().map(meta_encryption_to_object),
                })
            }
        }
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        encryption: Option<GetEncryptionOptions>,
    ) -> Result<(ObjectInfo, Bytes)> {
        let state = self.read_bucket_versioning(bucket).await?;
        if state == VersioningState::Unversioned {
            let (object_info, xl_meta, object_path) = self.read_object(bucket, key).await?;
            let data_path = object_path.join(xl_meta.data_dir).join(DATA_PART_FILE_NAME);
            let data = fs::read(data_path)
                .await
                .map_err(|_| MaxioError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                })?;
            let plain = self.decrypt_object_data(
                bucket,
                key,
                None,
                xl_meta.encryption.as_ref(),
                &data,
                encryption.as_ref(),
            )?;
            return Ok((object_info, Bytes::from(plain)));
        }

        let versions = self.ensure_versions_index(bucket, key).await?;
        for entry in versions {
            if entry.is_delete_marker {
                continue;
            }

            return self
                .get_object_version(bucket, key, &entry.version_id, encryption)
                .await;
        }

        Err(MaxioError::ObjectNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }

    pub async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        encryption: Option<GetEncryptionOptions>,
    ) -> Result<(ObjectInfo, Bytes)> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        ensure_bucket_exists(self, bucket).await?;

        let (object_info, xl_meta, object_path) = self
            .read_object_version_meta(bucket, key, version_id)
            .await?;
        if xl_meta.is_delete_marker {
            return Err(MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        let data_path = object_path.join(xl_meta.data_dir).join(DATA_PART_FILE_NAME);
        let data = fs::read(data_path)
            .await
            .map_err(|_| MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;

        let plain = self.decrypt_object_data(
            bucket,
            key,
            Some(version_id),
            xl_meta.encryption.as_ref(),
            &data,
            encryption.as_ref(),
        )?;

        Ok((object_info, Bytes::from(plain)))
    }

    pub async fn get_object_info(
        &self,
        bucket: &str,
        key: &str,
        encryption: Option<GetEncryptionOptions>,
    ) -> Result<ObjectInfo> {
        let (object_info, _) = self.get_object(bucket, key, encryption).await?;
        Ok(object_info)
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        ensure_bucket_exists(self, bucket).await?;

        let state = self.read_bucket_versioning(bucket).await?;
        if state != VersioningState::Enabled {
            let object_path = self.object_path(bucket, key);
            if !is_existing_directory(&object_path).await? {
                return Err(MaxioError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }

            fs::remove_dir_all(&object_path).await?;
            self.cleanup_empty_parents(bucket, &object_path).await?;
            return Ok(());
        }

        let object_path = self.object_path(bucket, key);
        if !is_existing_directory(&object_path).await? {
            return Err(MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        let mut versions = self.ensure_versions_index(bucket, key).await?;
        let version_id = Uuid::new_v4().to_string();
        let mod_time = Utc::now();
        let marker_meta = XlMeta {
            version: "1.0".to_string(),
            data_dir: String::new(),
            size: 0,
            etag: String::new(),
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            mod_time,
            metadata: HashMap::new(),
            version_id: Some(version_id.clone()),
            is_delete_marker: true,
            encryption: None,
        };
        let marker_path = object_path.join(&version_id);
        fs::create_dir_all(&marker_path).await?;
        self.write_xl_meta(&marker_path.join(META_FILE_NAME), &marker_meta)
            .await?;

        versions.insert(
            0,
            VersionIndexEntry {
                version_id,
                is_delete_marker: true,
                last_modified: mod_time,
                etag: None,
                size: 0,
            },
        );
        self.write_versions_index(&object_path, &versions).await?;

        Ok(())
    }

    pub async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<()> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        ensure_bucket_exists(self, bucket).await?;
        if version_id.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "version_id cannot be empty".to_string(),
            ));
        }

        let object_path = self.object_path(bucket, key);
        if !is_existing_directory(&object_path).await? {
            return Err(MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        let mut versions = self.ensure_versions_index(bucket, key).await?;
        let original_len = versions.len();
        versions.retain(|entry| entry.version_id != version_id);

        if versions.len() == original_len {
            return Err(MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: format!("{key}?versionId={version_id}"),
            });
        }

        self.remove_version_dir_if_exists(&object_path, version_id)
            .await?;
        if versions.is_empty() {
            self.remove_versions_index_if_exists(&object_path).await?;
            self.cleanup_empty_parents(bucket, &object_path).await?;
        } else {
            self.write_versions_index(&object_path, &versions).await?;
        }

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
        let object_roots = self.collect_object_roots(&bucket_path).await?;
        let mut objects = Vec::new();

        for object_root in object_roots {
            let rel = match object_root.strip_prefix(&bucket_path) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let object_key = rel.to_string_lossy().replace('\\', "/");
            if let Some(object_info) = self
                .latest_visible_object(bucket, &object_key, &object_root)
                .await?
            {
                objects.push(object_info);
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

    pub async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: i32,
    ) -> Result<Vec<ObjectVersion>> {
        validate_bucket_name(bucket)?;
        ensure_bucket_exists(self, bucket).await?;

        let bucket_path = self.bucket_path(bucket);
        let object_roots = self.collect_object_roots(&bucket_path).await?;
        let mut versions = Vec::new();

        for object_root in object_roots {
            let rel = match object_root.strip_prefix(&bucket_path) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let object_key = rel.to_string_lossy().replace('\\', "/");
            if !object_key.starts_with(prefix) {
                continue;
            }

            let entries = self.read_versions_index(&object_root).await?;
            if entries.is_empty() {
                let legacy_meta_path = object_root.join(META_FILE_NAME);
                if let Some(meta) = self.read_xl_meta_if_exists(&legacy_meta_path).await? {
                    versions.push(ObjectVersion {
                        key: object_key,
                        version_id: NULL_VERSION_ID.to_string(),
                        is_latest: true,
                        is_delete_marker: false,
                        last_modified: meta.mod_time,
                        etag: Some(meta.etag),
                        size: meta.size,
                    });
                }
                continue;
            }

            for (idx, entry) in entries.into_iter().enumerate() {
                versions.push(ObjectVersion {
                    key: object_key.clone(),
                    version_id: entry.version_id,
                    is_latest: idx == 0,
                    is_delete_marker: entry.is_delete_marker,
                    last_modified: entry.last_modified,
                    etag: entry.etag,
                    size: entry.size,
                });
            }
        }

        versions.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then(b.last_modified.cmp(&a.last_modified))
                .then(a.version_id.cmp(&b.version_id))
        });

        if max_keys > 0 {
            let limit = usize::try_from(max_keys).unwrap_or(usize::MAX);
            versions.truncate(limit);
        }

        Ok(versions)
    }

    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        metadata: HashMap<String, String>,
    ) -> Result<String> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        ensure_bucket_exists(self, bucket).await?;

        let upload_id = Uuid::new_v4().to_string();
        let upload_path = self.multipart_upload_path(bucket, &upload_id);
        fs::create_dir_all(&upload_path).await?;

        let upload_meta = MultipartUploadMeta {
            key: key.to_string(),
            content_type: content_type.map(str::to_string),
            metadata,
            initiated: Utc::now(),
        };

        let meta_json = serde_json::to_vec(&upload_meta).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize multipart upload meta: {err}"))
        })?;
        fs::write(upload_path.join(MULTIPART_META_FILE_NAME), meta_json).await?;

        Ok(upload_id)
    }

    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        data: Bytes,
    ) -> Result<String> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        validate_part_number(part_number)?;
        ensure_bucket_exists(self, bucket).await?;

        let upload_meta = self.read_multipart_upload_meta(bucket, upload_id).await?;
        if upload_meta.key != key {
            return Err(MaxioError::InvalidArgument(format!(
                "upload id does not match object key: {bucket}/{key}"
            )));
        }

        let etag = format!("{:x}", Md5::digest(&data));
        let part_path = self.multipart_part_path(bucket, upload_id, part_number);
        if let Some(parent) = part_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(part_path, data).await?;

        Ok(etag)
    }

    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletePart>,
    ) -> Result<ObjectInfo> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        ensure_bucket_exists(self, bucket).await?;

        if parts.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "complete multipart upload requires at least one part".to_string(),
            ));
        }

        let upload_meta = self.read_multipart_upload_meta(bucket, upload_id).await?;
        if upload_meta.key != key {
            return Err(MaxioError::InvalidArgument(format!(
                "upload id does not match object key: {bucket}/{key}"
            )));
        }

        let mut all_parts = self.list_parts(bucket, key, upload_id).await?;
        all_parts.sort_by_key(|item| item.part_number);
        let part_map: HashMap<i32, PartInfo> = all_parts
            .into_iter()
            .map(|item| (item.part_number, item))
            .collect();

        let mut previous_part = 0;
        let mut output = Vec::new();
        let mut final_etag_material = Vec::with_capacity(parts.len() * 16);

        for part in &parts {
            validate_part_number(part.part_number)?;
            if part.part_number <= previous_part {
                return Err(MaxioError::InvalidArgument(
                    "complete multipart upload parts must be in ascending order".to_string(),
                ));
            }
            previous_part = part.part_number;

            let provided_etag = normalize_etag(&part.etag);
            let part_info = part_map.get(&part.part_number).ok_or_else(|| {
                MaxioError::InvalidArgument(format!(
                    "missing uploaded part {} for upload id {upload_id}",
                    part.part_number
                ))
            })?;

            if part_info.etag != provided_etag {
                return Err(MaxioError::InvalidArgument(format!(
                    "etag mismatch for part {}",
                    part.part_number
                )));
            }

            let part_path = self.multipart_part_path(bucket, upload_id, part.part_number);
            let bytes = fs::read(part_path).await.map_err(|err| {
                if err.kind() == std::io::ErrorKind::NotFound {
                    MaxioError::InvalidArgument(format!(
                        "missing uploaded part {} for upload id {upload_id}",
                        part.part_number
                    ))
                } else {
                    MaxioError::Io(err)
                }
            })?;
            output.extend_from_slice(&bytes);

            let part_md5 = decode_md5_hex(&part_info.etag)?;
            final_etag_material.extend_from_slice(&part_md5);
        }

        let final_etag = format!("{:x}-{}", Md5::digest(&final_etag_material), parts.len());
        let content_type = upload_meta
            .content_type
            .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_string());

        let mut object_info = self
            .put_object(
                bucket,
                key,
                Bytes::from(output),
                Some(&content_type),
                upload_meta.metadata.clone(),
                None,
            )
            .await?;
        self.update_object_etag(bucket, key, object_info.version_id.as_deref(), &final_etag)
            .await?;

        self.abort_multipart_upload(bucket, key, upload_id).await?;

        object_info.etag = final_etag;
        Ok(object_info)
    }

    pub async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<()> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        ensure_bucket_exists(self, bucket).await?;

        let upload_meta = self.read_multipart_upload_meta(bucket, upload_id).await?;
        if upload_meta.key != key {
            return Err(MaxioError::InvalidArgument(format!(
                "upload id does not match object key: {bucket}/{key}"
            )));
        }

        let upload_path = self.multipart_upload_path(bucket, upload_id);
        fs::remove_dir_all(upload_path).await?;
        Ok(())
    }

    pub async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<PartInfo>> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        ensure_bucket_exists(self, bucket).await?;

        let upload_meta = self.read_multipart_upload_meta(bucket, upload_id).await?;
        if upload_meta.key != key {
            return Err(MaxioError::InvalidArgument(format!(
                "upload id does not match object key: {bucket}/{key}"
            )));
        }

        let mut entries = fs::read_dir(self.multipart_upload_path(bucket, upload_id))
            .await
            .map_err(|err| map_multipart_not_found(err, bucket, key, upload_id))?;
        let mut parts = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name().to_string_lossy().to_string();
            let Some(part_suffix) = file_name.strip_prefix("part_") else {
                continue;
            };

            let Ok(part_number) = part_suffix.parse::<i32>() else {
                continue;
            };
            validate_part_number(part_number)?;

            let bytes = fs::read(entry.path()).await?;
            let size = i64::try_from(bytes.len()).map_err(|_| {
                MaxioError::InvalidArgument(format!(
                    "part is too large to list: {bucket}/{key} part {part_number}"
                ))
            })?;
            let entry_meta = entry.metadata().await?;
            let last_modified =
                filetime_to_utc(entry_meta.modified().ok()).unwrap_or_else(Utc::now);
            let etag = format!("{:x}", Md5::digest(&bytes));

            parts.push(PartInfo {
                part_number,
                size,
                etag,
                last_modified,
            });
        }

        parts.sort_by_key(|part| part.part_number);
        Ok(parts)
    }

    pub async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<MultipartUploadInfo>> {
        validate_bucket_name(bucket)?;
        ensure_bucket_exists(self, bucket).await?;

        let multipart_root = self.multipart_root_path(bucket);
        if !is_existing_directory(&multipart_root).await? {
            return Ok(Vec::new());
        }

        let mut entries = fs::read_dir(multipart_root).await?;
        let mut uploads = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let upload_id = entry.file_name().to_string_lossy().to_string();
            let upload_meta = match self.read_multipart_upload_meta(bucket, &upload_id).await {
                Ok(meta) => meta,
                Err(_) => continue,
            };

            if !upload_meta.key.starts_with(prefix) {
                continue;
            }

            uploads.push(MultipartUploadInfo {
                key: upload_meta.key,
                upload_id,
                initiated: upload_meta.initiated,
            });
        }

        uploads.sort_by(|a, b| a.key.cmp(&b.key).then(a.upload_id.cmp(&b.upload_id)));
        Ok(uploads)
    }

    fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.root_dir.join(bucket)
    }

    fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.bucket_path(bucket).join(key)
    }

    fn multipart_root_path(&self, bucket: &str) -> PathBuf {
        self.bucket_path(bucket).join(MULTIPART_DIR_NAME)
    }

    fn multipart_upload_path(&self, bucket: &str, upload_id: &str) -> PathBuf {
        self.multipart_root_path(bucket).join(upload_id)
    }

    fn multipart_part_path(&self, bucket: &str, upload_id: &str, part_number: i32) -> PathBuf {
        self.multipart_upload_path(bucket, upload_id)
            .join(format!("part_{part_number}"))
    }

    async fn read_multipart_upload_meta(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> Result<MultipartUploadMeta> {
        let upload_meta_path = self
            .multipart_upload_path(bucket, upload_id)
            .join(MULTIPART_META_FILE_NAME);
        let meta_bytes = fs::read(upload_meta_path)
            .await
            .map_err(|err| map_multipart_not_found(err, bucket, "", upload_id))?;
        serde_json::from_slice(&meta_bytes).map_err(|err| {
            MaxioError::InternalError(format!("failed to parse multipart upload metadata: {err}"))
        })
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

        let object_info = self.meta_to_object_info(bucket, key, &xl_meta);
        Ok((object_info, xl_meta, object_path))
    }

    async fn read_object_version_meta(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(ObjectInfo, XlMeta, PathBuf)> {
        let object_path = self.object_path(bucket, key);

        if version_id == NULL_VERSION_ID {
            let legacy_meta_path = object_path.join(META_FILE_NAME);
            if let Some(meta) = self.read_xl_meta_if_exists(&legacy_meta_path).await? {
                let mut info = self.meta_to_object_info(bucket, key, &meta);
                info.version_id = Some(NULL_VERSION_ID.to_string());
                return Ok((info, meta, object_path));
            }
        }

        let version_path = object_path.join(version_id);
        let meta_path = version_path.join(META_FILE_NAME);
        let meta = self
            .read_xl_meta_if_exists(&meta_path)
            .await?
            .ok_or_else(|| MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: format!("{key}?versionId={version_id}"),
            })?;

        let mut info = self.meta_to_object_info(bucket, key, &meta);
        info.version_id = meta
            .version_id
            .clone()
            .or_else(|| Some(version_id.to_string()));
        Ok((info, meta, version_path))
    }

    fn meta_to_object_info(&self, bucket: &str, key: &str, xl_meta: &XlMeta) -> ObjectInfo {
        ObjectInfo {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: xl_meta.size,
            etag: xl_meta.etag.clone(),
            content_type: xl_meta.content_type.clone(),
            last_modified: xl_meta.mod_time,
            metadata: xl_meta.metadata.clone(),
            version_id: xl_meta.version_id.clone(),
            encryption: xl_meta.encryption.clone().map(meta_encryption_to_object),
        }
    }

    fn resolve_put_encryption(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        encryption: Option<&PutEncryptionOptions>,
    ) -> Result<(Option<[u8; 32]>, Option<EncryptionInfo>)> {
        let Some(encryption) = encryption else {
            return Ok((None, None));
        };

        if let Some(customer_key) = encryption.sse_c_key {
            let key_md5 = encryption.sse_c_key_md5.clone().ok_or_else(|| {
                MaxioError::InvalidArgument(
                    "missing SSE-C key MD5 for encrypted put request".to_string(),
                )
            })?;

            return Ok((
                Some(customer_key),
                Some(EncryptionInfo {
                    algorithm: "AES256".to_string(),
                    sse_type: "SSE-C".to_string(),
                    key_md5: Some(key_md5),
                }),
            ));
        }

        if encryption.sse_s3 {
            return Ok((
                Some(self.master_key.derive_object_key(bucket, key, version_id)),
                Some(EncryptionInfo {
                    algorithm: "AES256".to_string(),
                    sse_type: "SSE-S3".to_string(),
                    key_md5: None,
                }),
            ));
        }

        Ok((None, None))
    }

    fn decrypt_object_data(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        encryption_info: Option<&EncryptionInfo>,
        stored_data: &[u8],
        request_encryption: Option<&GetEncryptionOptions>,
    ) -> Result<Vec<u8>> {
        let Some(encryption_info) = encryption_info else {
            return Ok(stored_data.to_vec());
        };

        match encryption_info.sse_type.as_str() {
            "SSE-S3" => {
                let object_key = self.master_key.derive_object_key(bucket, key, version_id);
                match cipher::decrypt(&object_key, stored_data) {
                    Ok(data) => Ok(data),
                    Err(err) if version_id == Some(NULL_VERSION_ID) => {
                        let fallback_key = self.master_key.derive_object_key(bucket, key, None);
                        cipher::decrypt(&fallback_key, stored_data)
                            .map_err(|_| map_crypto_error(err))
                    }
                    Err(err) => Err(map_crypto_error(err)),
                }
            }
            "SSE-C" => {
                let request_encryption = request_encryption.ok_or_else(|| {
                    MaxioError::InvalidArgument(
                        "missing SSE-C headers for encrypted object access".to_string(),
                    )
                })?;
                let customer_key = request_encryption.sse_c_key.ok_or_else(|| {
                    MaxioError::InvalidArgument(
                        "missing SSE-C customer key for encrypted object access".to_string(),
                    )
                })?;
                let request_md5 = request_encryption.sse_c_key_md5.clone().ok_or_else(|| {
                    MaxioError::InvalidArgument(
                        "missing SSE-C customer key MD5 for encrypted object access".to_string(),
                    )
                })?;
                let expected_md5 = encryption_info.key_md5.clone().ok_or_else(|| {
                    MaxioError::InternalError(
                        "encrypted object metadata missing SSE-C key md5".to_string(),
                    )
                })?;

                if request_md5 != expected_md5 {
                    return Err(MaxioError::AccessDenied(
                        "SSE-C customer key MD5 mismatch".to_string(),
                    ));
                }

                cipher::decrypt(&customer_key, stored_data).map_err(map_crypto_error)
            }
            other => Err(MaxioError::InternalError(format!(
                "unsupported encryption type in metadata: {other}"
            ))),
        }
    }

    async fn read_bucket_versioning(&self, bucket: &str) -> Result<VersioningState> {
        let path = self.bucket_path(bucket).join(VERSIONING_FILE_NAME);
        match fs::read(path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|err| {
                MaxioError::InternalError(format!("failed to parse bucket versioning state: {err}"))
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Ok(VersioningState::Unversioned)
            }
            Err(err) => Err(MaxioError::Io(err)),
        }
    }

    async fn read_xl_meta_if_exists(&self, path: &Path) -> Result<Option<XlMeta>> {
        match fs::read(path).await {
            Ok(bytes) => {
                let meta: XlMeta = serde_json::from_slice(&bytes).map_err(|err| {
                    MaxioError::InternalError(format!("failed to parse xl.meta: {err}"))
                })?;
                Ok(Some(meta))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(MaxioError::Io(err)),
        }
    }

    async fn write_xl_meta(&self, path: &Path, meta: &XlMeta) -> Result<()> {
        let bytes = serde_json::to_vec(meta).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize xl.meta: {err}"))
        })?;
        fs::write(path, bytes).await?;
        Ok(())
    }

    async fn read_versions_index(&self, object_path: &Path) -> Result<Vec<VersionIndexEntry>> {
        let path = object_path.join(VERSIONS_INDEX_FILE_NAME);
        match fs::read(path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|err| {
                MaxioError::InternalError(format!("failed to parse versions index: {err}"))
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(err) => Err(MaxioError::Io(err)),
        }
    }

    async fn write_versions_index(
        &self,
        object_path: &Path,
        entries: &[VersionIndexEntry],
    ) -> Result<()> {
        fs::create_dir_all(object_path).await?;
        let bytes = serde_json::to_vec(entries).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize versions index: {err}"))
        })?;
        fs::write(object_path.join(VERSIONS_INDEX_FILE_NAME), bytes).await?;
        Ok(())
    }

    async fn remove_versions_index_if_exists(&self, object_path: &Path) -> Result<()> {
        match fs::remove_file(object_path.join(VERSIONS_INDEX_FILE_NAME)).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(MaxioError::Io(err)),
        }
    }

    async fn remove_version_dir_if_exists(
        &self,
        object_path: &Path,
        version_id: &str,
    ) -> Result<()> {
        match fs::remove_dir_all(object_path.join(version_id)).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(MaxioError::Io(err)),
        }
    }

    async fn ensure_versions_index(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<VersionIndexEntry>> {
        let object_path = self.object_path(bucket, key);
        let entries = self.read_versions_index(&object_path).await?;
        if !entries.is_empty() {
            return Ok(entries);
        }

        let legacy_meta_path = object_path.join(META_FILE_NAME);
        let Some(legacy_meta) = self.read_xl_meta_if_exists(&legacy_meta_path).await? else {
            return Ok(Vec::new());
        };

        let mut migrated_meta = legacy_meta.clone();
        migrated_meta.version_id = Some(NULL_VERSION_ID.to_string());
        migrated_meta.is_delete_marker = false;

        let null_version_path = object_path.join(NULL_VERSION_ID);
        fs::create_dir_all(&null_version_path).await?;
        let src_data = object_path
            .join(&legacy_meta.data_dir)
            .join(DATA_PART_FILE_NAME);
        let dst_data = null_version_path.join(DATA_PART_FILE_NAME);
        let data = fs::read(&src_data)
            .await
            .map_err(|_| MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;
        fs::write(&dst_data, data).await?;
        self.write_xl_meta(&null_version_path.join(META_FILE_NAME), &migrated_meta)
            .await?;

        match fs::remove_dir_all(object_path.join(&legacy_meta.data_dir)).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(MaxioError::Io(err)),
        }
        match fs::remove_file(&legacy_meta_path).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(MaxioError::Io(err)),
        }

        let out = vec![VersionIndexEntry {
            version_id: NULL_VERSION_ID.to_string(),
            is_delete_marker: false,
            last_modified: migrated_meta.mod_time,
            etag: Some(migrated_meta.etag),
            size: migrated_meta.size,
        }];
        self.write_versions_index(&object_path, &out).await?;
        Ok(out)
    }

    async fn latest_visible_object(
        &self,
        bucket: &str,
        key: &str,
        object_root: &Path,
    ) -> Result<Option<ObjectInfo>> {
        let versions = self.read_versions_index(object_root).await?;
        if versions.is_empty() {
            if let Some(meta) = self
                .read_xl_meta_if_exists(&object_root.join(META_FILE_NAME))
                .await?
            {
                return Ok(Some(self.meta_to_object_info(bucket, key, &meta)));
            }
            return Ok(None);
        }

        for entry in versions {
            if entry.is_delete_marker {
                continue;
            }
            let (info, _, _) = self
                .read_object_version_meta(bucket, key, &entry.version_id)
                .await?;
            return Ok(Some(info));
        }

        Ok(None)
    }

    async fn collect_object_roots(&self, bucket_path: &Path) -> Result<Vec<PathBuf>> {
        let mut stack = vec![bucket_path.to_path_buf()];
        let mut roots = Vec::new();

        while let Some(dir) = stack.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(items) => items,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(MaxioError::Io(err)),
            };

            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if !entry.metadata().await?.is_dir() {
                    continue;
                }

                let name = entry.file_name().to_string_lossy().to_string();
                if name == MULTIPART_DIR_NAME {
                    continue;
                }

                let has_versions = fs::metadata(path.join(VERSIONS_INDEX_FILE_NAME))
                    .await
                    .map(|meta| meta.is_file())
                    .unwrap_or(false);
                let has_legacy_meta = fs::metadata(path.join(META_FILE_NAME))
                    .await
                    .map(|meta| meta.is_file())
                    .unwrap_or(false);

                if has_versions || has_legacy_meta {
                    roots.push(path);
                } else {
                    stack.push(path);
                }
            }
        }

        Ok(roots)
    }

    async fn cleanup_empty_parents(&self, bucket: &str, object_path: &Path) -> Result<()> {
        let bucket_path = self.bucket_path(bucket);
        let mut current = object_path.parent().map(Path::to_path_buf);
        while let Some(dir) = current {
            if dir == bucket_path {
                break;
            }
            match fs::read_dir(&dir).await {
                Ok(mut entries) => {
                    if entries.next_entry().await?.is_none() {
                        let _ = fs::remove_dir(&dir).await;
                    } else {
                        break;
                    }
                }
                Err(_) => break,
            }
            current = dir.parent().map(Path::to_path_buf);
        }

        Ok(())
    }

    async fn update_object_etag(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        etag: &str,
    ) -> Result<()> {
        let object_path = self.object_path(bucket, key);

        match version_id {
            Some(version_id) => {
                let meta_path = object_path.join(version_id).join(META_FILE_NAME);
                let mut meta = self
                    .read_xl_meta_if_exists(&meta_path)
                    .await?
                    .ok_or_else(|| MaxioError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: format!("{key}?versionId={version_id}"),
                    })?;
                meta.etag = etag.to_string();
                self.write_xl_meta(&meta_path, &meta).await?;

                let mut versions = self.read_versions_index(&object_path).await?;
                for entry in &mut versions {
                    if entry.version_id == version_id {
                        entry.etag = Some(etag.to_string());
                        break;
                    }
                }
                if !versions.is_empty() {
                    self.write_versions_index(&object_path, &versions).await?;
                }
            }
            None => {
                let meta_path = object_path.join(META_FILE_NAME);
                let mut meta = self
                    .read_xl_meta_if_exists(&meta_path)
                    .await?
                    .ok_or_else(|| MaxioError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    })?;
                meta.etag = etag.to_string();
                self.write_xl_meta(&meta_path, &meta).await?;
            }
        }

        Ok(())
    }
}

fn validate_bucket_name(bucket: &str) -> Result<()> {
    if bucket.is_empty()
        || bucket == SYS_DIR_NAME
        || bucket == CRYPTO_DIR_NAME
        || bucket.contains('/')
        || bucket.contains('\\')
    {
        return Err(MaxioError::InvalidBucketName(bucket.to_string()));
    }
    Ok(())
}

fn meta_encryption_to_object(value: EncryptionInfo) -> ObjectEncryption {
    ObjectEncryption {
        algorithm: value.algorithm,
        sse_type: value.sse_type,
        key_md5: value.key_md5,
    }
}

fn map_crypto_error(err: maxio_crypto::CryptoError) -> MaxioError {
    MaxioError::InternalError(format!("crypto operation failed: {err}"))
}

async fn load_or_create_master_key(root_dir: &Path) -> Result<MasterKey> {
    let crypto_dir = root_dir.join(CRYPTO_DIR_NAME);
    fs::create_dir_all(&crypto_dir).await?;
    let key_path = crypto_dir.join(MASTER_KEY_FILE_NAME);

    match fs::read(&key_path).await {
        Ok(bytes) => MasterKey::from_bytes(&bytes)
            .map_err(|err| MaxioError::InternalError(format!("invalid master key file: {err}"))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let key = MasterKey::generate();
            fs::write(&key_path, key.as_bytes()).await?;
            Ok(key)
        }
        Err(err) => Err(MaxioError::Io(err)),
    }
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
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
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

fn map_multipart_not_found(
    err: std::io::Error,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> MaxioError {
    if err.kind() == std::io::ErrorKind::NotFound {
        let object_key = if key.is_empty() { "<unknown>" } else { key };
        MaxioError::ObjectNotFound {
            bucket: bucket.to_string(),
            key: format!("{object_key}?uploadId={upload_id}"),
        }
    } else {
        MaxioError::Io(err)
    }
}

fn validate_part_number(part_number: i32) -> Result<()> {
    if (1..=10_000).contains(&part_number) {
        Ok(())
    } else {
        Err(MaxioError::InvalidArgument(format!(
            "invalid part number: {part_number}"
        )))
    }
}

fn normalize_etag(etag: &str) -> String {
    let trimmed = etag.trim();
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

fn decode_md5_hex(etag: &str) -> Result<[u8; 16]> {
    if etag.len() != 32 {
        return Err(MaxioError::InvalidArgument(format!(
            "invalid part etag format: {etag}"
        )));
    }

    let mut out = [0_u8; 16];
    for idx in 0..16 {
        let start = idx * 2;
        let end = start + 2;
        let byte = u8::from_str_radix(&etag[start..end], 16).map_err(|_| {
            MaxioError::InvalidArgument(format!("invalid part etag format: {etag}"))
        })?;
        out[idx] = byte;
    }

    Ok(out)
}
