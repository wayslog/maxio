//! Virtual filesystem layer mapping FTP paths to S3 operations

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use maxio_common::error::MaxioError;
use maxio_iam::IAMSys;
use maxio_storage::traits::ObjectLayer;

use crate::{FtpError, Result};

/// Virtual file entry
#[derive(Debug, Clone)]
pub struct VfsEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: i64,
    pub modified: DateTime<Utc>,
}

/// Virtual filesystem backed by S3 storage
pub struct Vfs {
    storage: Arc<dyn ObjectLayer>,
    iam: Arc<IAMSys>,
}

impl Vfs {
    pub fn new(storage: Arc<dyn ObjectLayer>, iam: Arc<IAMSys>) -> Self {
        Self { storage, iam }
    }

    /// Authenticate user with access key and secret key
    pub fn authenticate(&self, username: &str, password: &str) -> Result<()> {
        match self.iam.user_secret_key(username) {
            Some(secret) if secret == password => Ok(()),
            Some(_) => Err(FtpError::AuthFailed),
            None => Err(FtpError::AuthFailed),
        }
    }

    /// List directory contents
    /// - "/" lists buckets
    /// - "/<bucket>" lists objects in bucket
    /// - "/<bucket>/<prefix>" lists objects with prefix
    pub async fn list_dir(&self, path: &str) -> Result<Vec<VfsEntry>> {
        let path = normalize_path(path);

        if path == "/" {
            // List buckets
            let buckets = self
                .storage
                .list_buckets()
                .await
                .map_err(|e| FtpError::Internal(e.to_string()))?;

            Ok(buckets
                .into_iter()
                .map(|b| VfsEntry {
                    name: b.name,
                    is_dir: true,
                    size: 0,
                    modified: b.created,
                })
                .collect())
        } else {
            // List objects in bucket
            let (bucket, prefix) = parse_path(&path)?;
            let prefix = prefix.unwrap_or_default();
            let delimiter = "/";

            let result = self
                .storage
                .list_objects(&bucket, &prefix, "", delimiter, 1000)
                .await
                .map_err(map_storage_error)?;

            let mut entries = Vec::new();

            // Add common prefixes as directories
            for prefix_str in result.prefixes {
                let name = prefix_str
                    .strip_prefix(&prefix)
                    .unwrap_or(&prefix_str)
                    .trim_end_matches('/');
                if !name.is_empty() {
                    entries.push(VfsEntry {
                        name: name.to_string(),
                        is_dir: true,
                        size: 0,
                        modified: Utc::now(),
                    });
                }
            }

            // Add objects as files
            for obj in result.objects {
                let name = obj
                    .key
                    .strip_prefix(&prefix)
                    .unwrap_or(&obj.key)
                    .to_string();
                if !name.is_empty() && !name.contains('/') {
                    entries.push(VfsEntry {
                        name,
                        is_dir: false,
                        size: obj.size,
                        modified: obj.last_modified,
                    });
                }
            }

            Ok(entries)
        }
    }

    /// Get file/directory info
    pub async fn stat(&self, path: &str) -> Result<VfsEntry> {
        let path = normalize_path(path);

        if path == "/" {
            return Ok(VfsEntry {
                name: "/".to_string(),
                is_dir: true,
                size: 0,
                modified: Utc::now(),
            });
        }

        let (bucket, key) = parse_path(&path)?;

        if key.is_none() {
            // Check if bucket exists
            let info = self
                .storage
                .get_bucket_info(&bucket)
                .await
                .map_err(map_storage_error)?;

            return Ok(VfsEntry {
                name: bucket,
                is_dir: true,
                size: 0,
                modified: info.created,
            });
        }

        let key = key.unwrap();

        // Try as object first
        match self.storage.get_object_info(&bucket, &key, None).await {
            Ok(info) => Ok(VfsEntry {
                name: key.rsplit('/').next().unwrap_or(&key).to_string(),
                is_dir: false,
                size: info.size,
                modified: info.last_modified,
            }),
            Err(_) => {
                // Try as directory (prefix)
                let result = self
                    .storage
                    .list_objects(&bucket, &format!("{}/", key), "", "/", 1)
                    .await
                    .map_err(map_storage_error)?;

                if !result.objects.is_empty() || !result.prefixes.is_empty() {
                    Ok(VfsEntry {
                        name: key.rsplit('/').next().unwrap_or(&key).to_string(),
                        is_dir: true,
                        size: 0,
                        modified: Utc::now(),
                    })
                } else {
                    Err(FtpError::NotFound(path))
                }
            }
        }
    }

    /// Read file contents
    pub async fn read_file(&self, path: &str) -> Result<Bytes> {
        let path = normalize_path(path);
        let (bucket, key) = parse_path(&path)?;
        let key = key.ok_or_else(|| FtpError::InvalidPath("cannot read bucket as file".to_string()))?;

        let (_, data) = self
            .storage
            .get_object(&bucket, &key, None)
            .await
            .map_err(map_storage_error)?;

        Ok(data)
    }

    /// Write file contents
    pub async fn write_file(&self, path: &str, data: Bytes) -> Result<()> {
        let path = normalize_path(path);
        let (bucket, key) = parse_path(&path)?;
        let key = key.ok_or_else(|| FtpError::InvalidPath("cannot write bucket as file".to_string()))?;

        self.storage
            .put_object(&bucket, &key, data, None, HashMap::new(), None)
            .await
            .map_err(map_storage_error)?;

        Ok(())
    }

    /// Delete file
    pub async fn delete_file(&self, path: &str) -> Result<()> {
        let path = normalize_path(path);
        let (bucket, key) = parse_path(&path)?;
        let key = key.ok_or_else(|| FtpError::InvalidPath("cannot delete bucket via FTP".to_string()))?;

        self.storage
            .delete_object(&bucket, &key)
            .await
            .map_err(map_storage_error)?;

        Ok(())
    }

    /// Create directory (bucket or prefix)
    pub async fn mkdir(&self, path: &str) -> Result<()> {
        let path = normalize_path(path);
        let (bucket, key) = parse_path(&path)?;

        if key.is_none() {
            // Create bucket
            self.storage
                .make_bucket(&bucket)
                .await
                .map_err(map_storage_error)?;
        } else {
            // Create empty object as directory marker
            let key = format!("{}/", key.unwrap());
            self.storage
                .put_object(&bucket, &key, Bytes::new(), None, HashMap::new(), None)
                .await
                .map_err(map_storage_error)?;
        }

        Ok(())
    }

    /// Remove directory
    pub async fn rmdir(&self, path: &str) -> Result<()> {
        let path = normalize_path(path);
        let (bucket, key) = parse_path(&path)?;

        if key.is_none() {
            // Delete bucket
            self.storage
                .delete_bucket(&bucket)
                .await
                .map_err(map_storage_error)?;
        } else {
            // Delete directory marker
            let key = format!("{}/", key.unwrap());
            let _ = self.storage.delete_object(&bucket, &key).await;
        }

        Ok(())
    }

    /// Rename/move file
    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let from = normalize_path(from);
        let to = normalize_path(to);

        let (src_bucket, src_key) = parse_path(&from)?;
        let (dst_bucket, dst_key) = parse_path(&to)?;

        let src_key = src_key.ok_or_else(|| FtpError::InvalidPath("cannot rename bucket".to_string()))?;
        let dst_key = dst_key.ok_or_else(|| FtpError::InvalidPath("cannot rename to bucket".to_string()))?;

        // Copy then delete
        self.storage
            .copy_object(&src_bucket, &src_key, &dst_bucket, &dst_key, None, HashMap::new())
            .await
            .map_err(map_storage_error)?;

        self.storage
            .delete_object(&src_bucket, &src_key)
            .await
            .map_err(map_storage_error)?;

        Ok(())
    }
}

fn normalize_path(path: &str) -> String {
    let path = path.trim();
    if path.is_empty() {
        return "/".to_string();
    }

    let mut normalized = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    };

    // Remove trailing slash except for root
    if normalized.len() > 1 && normalized.ends_with('/') {
        normalized.pop();
    }

    normalized
}

fn parse_path(path: &str) -> Result<(String, Option<String>)> {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return Err(FtpError::InvalidPath("empty path".to_string()));
    }

    let parts: Vec<&str> = path.splitn(2, '/').collect();
    let bucket = parts[0].to_string();
    let key = parts.get(1).map(|s| s.to_string());

    Ok((bucket, key))
}

fn map_storage_error(e: MaxioError) -> FtpError {
    match e {
        MaxioError::BucketNotFound(b) => FtpError::NotFound(b),
        MaxioError::ObjectNotFound { bucket, key } => FtpError::NotFound(format!("{}/{}", bucket, key)),
        MaxioError::AccessDenied(msg) => FtpError::PermissionDenied(msg),
        other => FtpError::Internal(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path(""), "/");
        assert_eq!(normalize_path("/"), "/");
        assert_eq!(normalize_path("/bucket"), "/bucket");
        assert_eq!(normalize_path("/bucket/"), "/bucket");
        assert_eq!(normalize_path("bucket"), "/bucket");
        assert_eq!(normalize_path("/bucket/key"), "/bucket/key");
    }

    #[test]
    fn test_parse_path() {
        let (bucket, key) = parse_path("/mybucket").unwrap();
        assert_eq!(bucket, "mybucket");
        assert!(key.is_none());

        let (bucket, key) = parse_path("/mybucket/mykey").unwrap();
        assert_eq!(bucket, "mybucket");
        assert_eq!(key, Some("mykey".to_string()));

        let (bucket, key) = parse_path("/mybucket/path/to/key").unwrap();
        assert_eq!(bucket, "mybucket");
        assert_eq!(key, Some("path/to/key".to_string()));
    }
}
