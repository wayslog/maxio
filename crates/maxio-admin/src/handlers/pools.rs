use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{AdminSys, handlers::AdminApiError};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolInfo {
    pub id: i32,
    pub endpoints: Vec<String>,
    pub set_count: i32,
    pub drives_per_set: i32,
    pub decommission_status: Option<PoolDecommissionInfo>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolDecommissionInfo {
    pub start_time: DateTime<Utc>,
    pub start_size: u64,
    pub current_size: u64,
    pub complete: bool,
    pub failed: bool,
    pub canceled: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolStatus {
    pub id: i32,
    pub cmd_line: String,
    pub last_update: DateTime<Utc>,
    pub decommission: Option<PoolDecommissionInfo>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebalanceInfo {
    pub id: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub status: String,
    pub pools: Vec<RebalancePoolProgress>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebalancePoolProgress {
    pub pool: i32,
    pub percent: f64,
    pub bytes: u64,
    pub objects: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PoolQuery {
    pub pool: String,
}

pub async fn list_pools(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<Vec<PoolInfo>>, AdminApiError> {
    // Return empty list - pool management requires distributed setup
    Ok(Json(vec![]))
}

pub async fn status_pool(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<PoolQuery>,
) -> Result<Json<PoolStatus>, AdminApiError> {
    let pool_id: i32 = query.pool.parse().unwrap_or(0);
    Ok(Json(PoolStatus {
        id: pool_id,
        cmd_line: String::new(),
        last_update: Utc::now(),
        decommission: None,
    }))
}

pub async fn start_decommission(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<PoolQuery>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let _pool_id: i32 = query.pool.parse().unwrap_or(0);
    Ok(Json(serde_json::json!({
        "message": "decommission started"
    })))
}

pub async fn cancel_decommission(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<PoolQuery>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let _pool_id: i32 = query.pool.parse().unwrap_or(0);
    Ok(Json(serde_json::json!({
        "message": "decommission canceled"
    })))
}

pub async fn rebalance_start(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    Ok(Json(serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "message": "rebalance started"
    })))
}

pub async fn rebalance_status(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<RebalanceInfo>, AdminApiError> {
    Ok(Json(RebalanceInfo {
        id: String::new(),
        start_time: None,
        end_time: None,
        status: "not-started".to_string(),
        pools: vec![],
    }))
}

pub async fn rebalance_stop(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    Ok(Json(serde_json::json!({
        "message": "rebalance stopped"
    })))
}
