use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use maxio_common::error::Result;
use maxio_common::types::{BucketInfo, ObjectInfo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectsResult {
    pub objects: Vec<ObjectInfo>,
    pub prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_marker: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletePart {
    pub part_number: i32,
    pub etag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartInfo {
    pub part_number: i32,
    pub size: i64,
    pub etag: String,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUploadInfo {
    pub key: String,
    pub upload_id: String,
    pub initiated: DateTime<Utc>,
}

#[async_trait]
pub trait ObjectLayer: Send + Sync {
    async fn make_bucket(&self, bucket: &str) -> Result<()>;
    async fn get_bucket_info(&self, bucket: &str) -> Result<BucketInfo>;
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>>;
    async fn delete_bucket(&self, bucket: &str) -> Result<()>;
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<&str>,
        metadata: HashMap<String, String>,
    ) -> Result<ObjectInfo>;
    async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, Bytes)>;
    async fn get_object_info(&self, bucket: &str, key: &str) -> Result<ObjectInfo>;
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()>;
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        delimiter: &str,
        max_keys: i32,
    ) -> Result<ListObjectsResult>;
    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        metadata: HashMap<String, String>,
    ) -> Result<String>;
    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        data: Bytes,
    ) -> Result<String>;
    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletePart>,
    ) -> Result<ObjectInfo>;
    async fn abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()>;
    async fn list_parts(&self, bucket: &str, key: &str, upload_id: &str) -> Result<Vec<PartInfo>>;
    async fn list_multipart_uploads(&self, bucket: &str, prefix: &str)
    -> Result<Vec<MultipartUploadInfo>>;
}
