use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PoolStatus {
    Active,
    Decommissioning,
    Decommissioned,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolInfo {
    pub id: String,
    pub endpoints: Vec<String>,
    pub status: PoolStatus,
    pub capacity: u64,
    pub used_space: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecommissionStatus {
    pub pool_id: String,
    pub progress: u8,
    pub objects_moved: u64,
    pub bytes_moved: u64,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceStatus {
    pub progress: u8,
    pub bytes_moved: u64,
    pub pools_touched: usize,
    pub started_at: DateTime<Utc>,
}
