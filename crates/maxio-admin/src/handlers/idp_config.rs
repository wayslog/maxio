use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
};
use serde::{Deserialize, Serialize};

use crate::{AdminSys, handlers::AdminApiError};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IDPConfig {
    #[serde(rename = "type")]
    pub idp_type: String,
    pub name: String,
    pub enabled: bool,
    pub info: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IDPPathParams {
    #[serde(rename = "type")]
    pub idp_type: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IDPTypeParam {
    #[serde(rename = "type")]
    pub idp_type: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IDPListResponse {
    pub configs: Vec<IDPConfigInfo>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IDPConfigInfo {
    pub name: String,
    pub enabled: bool,
}

pub async fn add_identity_provider_cfg(
    State(_admin): State<Arc<AdminSys>>,
    Path(params): Path<IDPPathParams>,
    Json(config): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let name = params.name.unwrap_or_default();
    Ok(Json(serde_json::json!({
        "message": format!("IDP config {} of type {} added", name, params.idp_type),
        "config": config
    })))
}

pub async fn update_identity_provider_cfg(
    State(_admin): State<Arc<AdminSys>>,
    Path(params): Path<IDPPathParams>,
    Json(config): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let name = params.name.unwrap_or_default();
    Ok(Json(serde_json::json!({
        "message": format!("IDP config {} of type {} updated", name, params.idp_type),
        "config": config
    })))
}

pub async fn list_identity_provider_cfg(
    State(_admin): State<Arc<AdminSys>>,
    Path(params): Path<IDPTypeParam>,
) -> Result<Json<IDPListResponse>, AdminApiError> {
    let _ = params.idp_type;
    Ok(Json(IDPListResponse { configs: vec![] }))
}

pub async fn get_identity_provider_cfg(
    State(_admin): State<Arc<AdminSys>>,
    Path(params): Path<IDPPathParams>,
) -> Result<Json<IDPConfig>, AdminApiError> {
    let name = params.name.unwrap_or_default();
    Ok(Json(IDPConfig {
        idp_type: params.idp_type,
        name,
        enabled: false,
        info: serde_json::json!({}),
    }))
}

pub async fn delete_identity_provider_cfg(
    State(_admin): State<Arc<AdminSys>>,
    Path(params): Path<IDPPathParams>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let name = params.name.unwrap_or_default();
    Ok(Json(serde_json::json!({
        "message": format!("IDP config {} of type {} deleted", name, params.idp_type)
    })))
}
