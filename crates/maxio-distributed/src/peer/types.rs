use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub endpoint: String,
    pub node_id: String,
    pub version: String,
    pub uptime: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PeerRequest {
    Ping,
    GetServerInfo,
    HealBucket {
        bucket: String,
        prefix: Option<String>,
        opts: HealOpts,
    },
    GetBgHealStatus,
    ReplicateObject {
        bucket: String,
        object: String,
        version_id: Option<String>,
    },
    DeleteObject {
        bucket: String,
        object: String,
        version_id: Option<String>,
    },
    SyncIAM {
        item: IAMSyncItem,
    },
    SyncBucket {
        bucket: String,
        operation: BucketSyncOp,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PeerResponse {
    Pong {
        timestamp: DateTime<Utc>,
    },
    ServerInfo(PeerInfo),
    HealResult {
        success: bool,
        items_healed: u64,
        errors: Vec<String>,
    },
    BgHealStatus {
        active: bool,
        last_activity: Option<DateTime<Utc>>,
    },
    ReplicateResult {
        success: bool,
        error: Option<String>,
    },
    DeleteResult {
        success: bool,
        error: Option<String>,
    },
    SyncResult {
        success: bool,
        error: Option<String>,
    },
    Error {
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealOpts {
    pub dry_run: bool,
    pub remove: bool,
    pub recursive: bool,
    pub scan_mode: String,
}

impl Default for HealOpts {
    fn default() -> Self {
        Self {
            dry_run: false,
            remove: false,
            recursive: true,
            scan_mode: "normal".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IAMSyncItem {
    User { access_key: String, data: Vec<u8> },
    Group { name: String, data: Vec<u8> },
    Policy { name: String, data: Vec<u8> },
    ServiceAccount { access_key: String, data: Vec<u8> },
    DeleteUser { access_key: String },
    DeleteGroup { name: String },
    DeletePolicy { name: String },
    DeleteServiceAccount { access_key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketSyncOp {
    Create,
    Delete,
    UpdatePolicy { policy: Vec<u8> },
    UpdateVersioning { enabled: bool },
    UpdateReplication { config: Vec<u8> },
    UpdateLifecycle { config: Vec<u8> },
    UpdateTags { tags: Vec<u8> },
    UpdateEncryption { config: Vec<u8> },
    UpdateObjectLock { config: Vec<u8> },
}

#[derive(Debug, Clone)]
pub struct PeerEndpoint {
    pub address: String,
    pub secure: bool,
}

impl PeerEndpoint {
    pub fn new(address: impl Into<String>, secure: bool) -> Self {
        Self {
            address: address.into(),
            secure,
        }
    }

    pub fn url(&self, path: &str) -> String {
        let scheme = if self.secure { "https" } else { "http" };
        format!("{}://{}{}", scheme, self.address, path)
    }
}
