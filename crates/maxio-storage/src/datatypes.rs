use std::collections::HashMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectPartInfo {
    pub number: i32,
    pub etag: String,
    pub size: i64,
    pub actual_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub volume: String,
    pub name: String,
    pub version_id: String,
    pub is_latest: bool,
    pub deleted: bool,
    pub data_dir: String,
    pub size: i64,
    pub mod_time: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
    pub parts: Vec<ObjectPartInfo>,
    pub data: Option<Bytes>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolInfo {
    pub name: String,
    pub created: DateTime<Utc>,
}
