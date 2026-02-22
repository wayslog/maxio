use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{AdminSys, handlers::AdminApiError};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealOpts {
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub remove: bool,
    #[serde(default)]
    pub recursive: bool,
    #[serde(default)]
    pub scan_mode: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealStartSuccess {
    pub client_token: String,
    pub client_address: String,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealResultItem {
    pub result_type: String,
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub detail: String,
    pub disk_count: i32,
    pub set_count: i32,
    pub before: HealDriveInfo,
    pub after: HealDriveInfo,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealDriveInfo {
    pub drives: Vec<HealDriveState>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealDriveState {
    pub uuid: String,
    pub endpoint: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BgHealState {
    pub last_heal_activity: Option<DateTime<Utc>>,
    pub next_heal_round: Option<DateTime<Utc>>,
    pub heal_disks_finished: bool,
    pub items_healed: u64,
    pub items_failed: u64,
    pub bytes_done: u64,
    pub bytes_failed: u64,
    pub started: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealQuery {
    #[serde(default)]
    pub client_token: Option<String>,
    #[serde(default)]
    pub force_start: Option<bool>,
    #[serde(default)]
    pub force_stop: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealPathParams {
    pub bucket: Option<String>,
    pub prefix: Option<String>,
}

pub async fn heal_handler(
    State(_admin): State<Arc<AdminSys>>,
    Path(params): Path<HealPathParams>,
    Query(_query): Query<HealQuery>,
    Json(_opts): Json<HealOpts>,
) -> Result<Json<HealStartSuccess>, AdminApiError> {
    let _bucket = params.bucket.unwrap_or_default();
    let _prefix = params.prefix.unwrap_or_default();

    Ok(Json(HealStartSuccess {
        client_token: uuid::Uuid::new_v4().to_string(),
        client_address: String::new(),
        started_at: Utc::now(),
    }))
}

pub async fn heal_root(
    State(_admin): State<Arc<AdminSys>>,
    Query(_query): Query<HealQuery>,
    Json(_opts): Json<HealOpts>,
) -> Result<Json<HealStartSuccess>, AdminApiError> {
    Ok(Json(HealStartSuccess {
        client_token: uuid::Uuid::new_v4().to_string(),
        client_address: String::new(),
        started_at: Utc::now(),
    }))
}

pub async fn background_heal_status(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<BgHealState>, AdminApiError> {
    Ok(Json(BgHealState {
        last_heal_activity: None,
        next_heal_round: None,
        heal_disks_finished: true,
        items_healed: 0,
        items_failed: 0,
        bytes_done: 0,
        bytes_failed: 0,
        started: None,
    }))
}
