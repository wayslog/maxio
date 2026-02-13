use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub created: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub bucket: String,
    pub key: String,
    pub size: i64,
    pub etag: String,
    pub content_type: String,
    pub last_modified: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
    pub version_id: Option<String>,
}
