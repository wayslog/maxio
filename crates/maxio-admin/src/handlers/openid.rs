use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{AdminSys, handlers::AdminApiError};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAccessKeysBulkQuery {
    pub list_type: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAccessKeysOpenIDResp {
    pub users: Vec<OpenIDUserAccessKeys>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenIDUserAccessKeys {
    pub user: String,
    pub access_keys: Vec<ServiceAccountInfo>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountInfo {
    pub access_key: String,
    pub parent_user: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub expiration: Option<DateTime<Utc>>,
}

pub async fn list_access_keys_openid_bulk(
    State(_admin): State<Arc<AdminSys>>,
    Query(_query): Query<ListAccessKeysBulkQuery>,
) -> Result<Json<ListAccessKeysOpenIDResp>, AdminApiError> {
    // Return empty - would need to iterate all OpenID users
    Ok(Json(ListAccessKeysOpenIDResp { users: vec![] }))
}
