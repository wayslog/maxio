use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationTarget {
    pub arn: String,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateObjectInfo {
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub size: u64,
    pub retry_count: u32,
    pub targets: Vec<ReplicationTarget>,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
}

impl ReplicateObjectInfo {
    pub fn object_key(&self) -> String {
        match self.version_id.as_ref() {
            Some(version_id) if !version_id.is_empty() => {
                format!("{}/{}/{}", self.bucket, self.object, version_id)
            }
            _ => format!("{}/{}", self.bucket, self.object),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletedObjectReplicationInfo {
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub retry_count: u32,
    pub targets: Vec<ReplicationTarget>,
}
