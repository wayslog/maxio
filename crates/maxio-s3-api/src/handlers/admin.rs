use std::sync::Arc;

use axum::{
    Json,
    extract::{Extension, Query},
    http::StatusCode,
    response::IntoResponse,
};
use maxio_common::error::MaxioError;
use maxio_iam::{IAMSys, Policy, User};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::S3Error;

#[derive(Debug, Deserialize)]
pub struct AddUserRequest {
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
}

#[derive(Debug, Deserialize)]
pub struct RemoveUserQuery {
    #[serde(rename = "accessKey")]
    pub access_key: String,
}

#[derive(Debug, Deserialize)]
pub struct AddCannedPolicyRequest {
    pub name: String,
    pub policy: Value,
}

#[derive(Debug, Deserialize)]
pub struct SetUserPolicyQuery {
    #[serde(rename = "userOrGroup")]
    pub user_or_group: String,
    #[serde(rename = "policyName")]
    pub policy_name: String,
}

#[derive(Debug, Serialize)]
pub struct AdminUserInfo {
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "policyNames")]
    pub policy_names: Vec<String>,
    #[serde(rename = "createdAt")]
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct MessageResponse {
    pub message: String,
}

pub async fn add_user(
    Extension(iam): Extension<Arc<IAMSys>>,
    Json(payload): Json<AddUserRequest>,
) -> Result<impl IntoResponse, S3Error> {
    let user = iam.create_user(&payload.access_key, &payload.secret_key).await?;
    Ok((StatusCode::OK, Json(admin_user_info(&user))))
}

pub async fn remove_user(
    Extension(iam): Extension<Arc<IAMSys>>,
    Query(query): Query<RemoveUserQuery>,
) -> Result<impl IntoResponse, S3Error> {
    iam.delete_user(&query.access_key).await?;
    Ok((
        StatusCode::OK,
        Json(MessageResponse {
            message: "user removed".to_string(),
        }),
    ))
}

pub async fn list_users(
    Extension(iam): Extension<Arc<IAMSys>>,
) -> Result<impl IntoResponse, S3Error> {
    let users = iam.list_users().await?;
    let response = users.iter().map(admin_user_info).collect::<Vec<_>>();
    Ok((StatusCode::OK, Json(response)))
}

pub async fn add_canned_policy(
    Extension(iam): Extension<Arc<IAMSys>>,
    Json(payload): Json<AddCannedPolicyRequest>,
) -> Result<impl IntoResponse, S3Error> {
    let mut policy: Policy = serde_json::from_value(payload.policy).map_err(|err| {
        S3Error::from(MaxioError::InvalidArgument(format!(
            "failed to parse policy document: {err}"
        )))
    })?;

    if policy.name.is_empty() {
        policy.name = payload.name;
    }

    if policy.name.is_empty() {
        return Err(S3Error::from(MaxioError::InvalidArgument(
            "policy name is required".to_string(),
        )));
    }

    iam.create_policy(policy).await?;
    Ok((
        StatusCode::OK,
        Json(MessageResponse {
            message: "policy stored".to_string(),
        }),
    ))
}

pub async fn set_user_or_group_policy(
    Extension(iam): Extension<Arc<IAMSys>>,
    Query(query): Query<SetUserPolicyQuery>,
) -> Result<impl IntoResponse, S3Error> {
    iam.attach_policy(&query.user_or_group, &query.policy_name)
        .await?;
    Ok((
        StatusCode::OK,
        Json(MessageResponse {
            message: "policy attached".to_string(),
        }),
    ))
}

fn admin_user_info(user: &User) -> AdminUserInfo {
    AdminUserInfo {
        access_key: user.access_key.clone(),
        policy_names: user.policy_names.clone(),
        created_at: user.created_at,
    }
}
