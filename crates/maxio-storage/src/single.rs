use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use maxio_common::error::Result;
use maxio_common::types::{BucketInfo, ObjectInfo};

use crate::traits::{ListObjectsResult, ObjectLayer};
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

    async fn get_object_info(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        self.storage.get_object_info(bucket, key).await
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.storage.delete_object(bucket, key).await
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
}
