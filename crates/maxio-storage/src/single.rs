use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use maxio_common::error::Result;
use maxio_common::types::{BucketInfo, ObjectInfo};

use crate::traits::{
    CompletePart, ListObjectsResult, MultipartUploadInfo, ObjectLayer, ObjectVersion, PartInfo,
    VersioningState,
};
use crate::xl::storage::XlStorage;

#[derive(Debug, Clone)]
pub struct SingleDiskObjectLayer {
    storage: XlStorage,
}

impl SingleDiskObjectLayer {
    pub async fn new(data_dir: PathBuf) -> Result<Self> {
        let storage = XlStorage::new(data_dir).await?;
        Ok(Self { storage })
    }
}

#[async_trait]
impl ObjectLayer for SingleDiskObjectLayer {
    async fn make_bucket(&self, bucket: &str) -> Result<()> {
        self.storage.make_bucket(bucket).await
    }

    async fn get_bucket_info(&self, bucket: &str) -> Result<BucketInfo> {
        self.storage.get_bucket_info(bucket).await
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        self.storage.list_buckets().await
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        self.storage.delete_bucket(bucket).await
    }

    async fn get_bucket_versioning(&self, bucket: &str) -> Result<VersioningState> {
        self.storage.get_bucket_versioning(bucket).await
    }

    async fn set_bucket_versioning(&self, bucket: &str, state: VersioningState) -> Result<()> {
        self.storage.set_bucket_versioning(bucket, state).await
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<&str>,
        metadata: HashMap<String, String>,
    ) -> Result<ObjectInfo> {
        self.storage
            .put_object(bucket, key, data, content_type, metadata)
            .await
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, Bytes)> {
        self.storage.get_object(bucket, key).await
    }

    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(ObjectInfo, Bytes)> {
        self.storage.get_object_version(bucket, key, version_id).await
    }

    async fn get_object_info(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        self.storage.get_object_info(bucket, key).await
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.storage.delete_object(bucket, key).await
    }

    async fn delete_object_version(&self, bucket: &str, key: &str, version_id: &str) -> Result<()> {
        self.storage
            .delete_object_version(bucket, key, version_id)
            .await
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        delimiter: &str,
        max_keys: i32,
    ) -> Result<ListObjectsResult> {
        self.storage
            .list_objects(bucket, prefix, marker, delimiter, max_keys)
            .await
    }

    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: i32,
    ) -> Result<Vec<ObjectVersion>> {
        self.storage
            .list_object_versions(bucket, prefix, max_keys)
            .await
    }

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        metadata: HashMap<String, String>,
    ) -> Result<String> {
        self.storage
            .create_multipart_upload(bucket, key, content_type, metadata)
            .await
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        data: Bytes,
    ) -> Result<String> {
        self.storage
            .upload_part(bucket, key, upload_id, part_number, data)
            .await
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletePart>,
    ) -> Result<ObjectInfo> {
        self.storage
            .complete_multipart_upload(bucket, key, upload_id, parts)
            .await
    }

    async fn abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
        self.storage.abort_multipart_upload(bucket, key, upload_id).await
    }

    async fn list_parts(&self, bucket: &str, key: &str, upload_id: &str) -> Result<Vec<PartInfo>> {
        self.storage.list_parts(bucket, key, upload_id).await
    }

    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<MultipartUploadInfo>> {
        self.storage.list_multipart_uploads(bucket, prefix).await
    }
}
