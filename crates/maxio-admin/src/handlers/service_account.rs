use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
};
use chrono::{DateTime, Utc};
use maxio_common::error::MaxioError;
use serde::{Deserialize, Serialize};

use crate::{AdminSys, handlers::AdminApiError};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddServiceAccountRequest {
    pub target_user: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub policy: Option<serde_json::Value>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub expiration: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateServiceAccountRequest {
    pub new_secret_key: Option<String>,
    pub new_policy: Option<serde_json::Value>,
    pub new_name: Option<String>,
    pub new_description: Option<String>,
    pub new_expiration: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountInfo {
    pub access_key: String,
    pub parent_user: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub expiration: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddServiceAccountResponse {
    pub access_key: String,
    pub secret_key: String,
    pub parent_user: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountQuery {
    pub access_key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListServiceAccountsQuery {
    pub user: Option<String>,
}

pub async fn add_service_account(
    State(admin): State<Arc<AdminSys>>,
    Json(req): Json<AddServiceAccountRequest>,
) -> Result<Json<AddServiceAccountResponse>, AdminApiError> {
    let target_user = req.target_user.ok_or_else(|| {
        AdminApiError(MaxioError::InvalidArgument(
            "targetUser is required".to_string(),
        ))
    })?;

    let session_policy = if let Some(policy_json) = req.policy {
        let policy: maxio_iam::Policy = serde_json::from_value(policy_json).map_err(|e| {
            AdminApiError(MaxioError::InvalidArgument(format!(
                "invalid policy: {}",
                e
            )))
        })?;
        Some(policy)
    } else {
        None
    };

    let sa = admin
        .iam()
        .create_service_account(
            &target_user,
            req.access_key.as_deref(),
            req.secret_key.as_deref(),
            session_policy,
            req.name,
            req.description,
            req.expiration,
        )
        .await
        .map_err(AdminApiError::from)?;

    Ok(Json(AddServiceAccountResponse {
        access_key: sa.access_key,
        secret_key: sa.secret_key,
        parent_user: sa.parent_user,
    }))
}

pub async fn update_service_account(
    State(admin): State<Arc<AdminSys>>,
    Query(query): Query<ServiceAccountQuery>,
    Json(req): Json<UpdateServiceAccountRequest>,
) -> Result<Json<ServiceAccountInfo>, AdminApiError> {
    let session_policy = if let Some(policy_json) = req.new_policy {
        let policy: maxio_iam::Policy = serde_json::from_value(policy_json).map_err(|e| {
            AdminApiError(MaxioError::InvalidArgument(format!(
                "invalid policy: {}",
                e
            )))
        })?;
        Some(Some(policy))
    } else {
        None
    };

    let sa = admin
        .iam()
        .update_service_account(
            &query.access_key,
            req.new_secret_key,
            session_policy,
            req.new_name.map(Some),
            req.new_description.map(Some),
            req.new_expiration.map(Some),
        )
        .await
        .map_err(AdminApiError::from)?;

    Ok(Json(to_service_account_info(&sa)))
}

pub async fn info_service_account(
    State(admin): State<Arc<AdminSys>>,
    Query(query): Query<ServiceAccountQuery>,
) -> Result<Json<ServiceAccountInfo>, AdminApiError> {
    let sa = admin
        .iam()
        .get_service_account(&query.access_key)
        .await
        .map_err(AdminApiError::from)?
        .ok_or_else(|| {
            AdminApiError(MaxioError::InvalidArgument(format!(
                "service account not found: {}",
                query.access_key
            )))
        })?;

    Ok(Json(to_service_account_info(&sa)))
}

pub async fn list_service_accounts(
    State(admin): State<Arc<AdminSys>>,
    Query(query): Query<ListServiceAccountsQuery>,
) -> Result<Json<Vec<ServiceAccountInfo>>, AdminApiError> {
    let user = query.user.ok_or_else(|| {
        AdminApiError(MaxioError::InvalidArgument("user is required".to_string()))
    })?;

    let accounts = admin
        .iam()
        .list_service_accounts(&user)
        .await
        .map_err(AdminApiError::from)?;

    Ok(Json(
        accounts.iter().map(to_service_account_info).collect(),
    ))
}

pub async fn delete_service_account(
    State(admin): State<Arc<AdminSys>>,
    Query(query): Query<ServiceAccountQuery>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    admin
        .iam()
        .delete_service_account(&query.access_key)
        .await
        .map_err(AdminApiError::from)?;

    Ok(Json(serde_json::json!({
        "message": "service account deleted"
    })))
}

fn to_service_account_info(sa: &maxio_iam::ServiceAccount) -> ServiceAccountInfo {
    ServiceAccountInfo {
        access_key: sa.access_key.clone(),
        parent_user: sa.parent_user.clone(),
        name: sa.name.clone(),
        description: sa.description.clone(),
        expiration: sa.expiration,
        created_at: sa.created_at,
        updated_at: sa.updated_at,
    }
}
