use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
};
use serde::{Deserialize, Serialize};

use crate::{AdminSys, handlers::AdminApiError};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TierConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub tier_type: TierType,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub region: Option<String>,
    pub storage_class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TierType {
    S3,
    Azure,
    GCS,
    MinIO,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TierInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub tier_type: TierType,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TierStats {
    pub tiers: Vec<TierStatsInfo>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TierStatsInfo {
    pub name: String,
    pub total_size: u64,
    pub num_objects: u64,
    pub num_versions: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TierPathParam {
    pub tier: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EditTierRequest {
    pub creds: Option<TierCredentials>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TierCredentials {
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

pub async fn add_tier(
    State(_admin): State<Arc<AdminSys>>,
    Json(config): Json<TierConfig>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    Ok(Json(serde_json::json!({
        "message": format!("tier {} added", config.name)
    })))
}

pub async fn edit_tier(
    State(_admin): State<Arc<AdminSys>>,
    Path(params): Path<TierPathParam>,
    Json(_req): Json<EditTierRequest>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    Ok(Json(serde_json::json!({
        "message": format!("tier {} updated", params.tier)
    })))
}

pub async fn list_tiers(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<Vec<TierInfo>>, AdminApiError> {
    Ok(Json(vec![]))
}

pub async fn remove_tier(
    State(_admin): State<Arc<AdminSys>>,
    Path(params): Path<TierPathParam>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    Ok(Json(serde_json::json!({
        "message": format!("tier {} removed", params.tier)
    })))
}

pub async fn verify_tier(
    State(_admin): State<Arc<AdminSys>>,
    Path(params): Path<TierPathParam>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    Ok(Json(serde_json::json!({
        "tier": params.tier,
        "status": "online"
    })))
}

pub async fn tier_stats(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<TierStats>, AdminApiError> {
    Ok(Json(TierStats { tiers: vec![] }))
}
