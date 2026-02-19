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

#[derive(Debug, Serialize)]
pub struct ServerInfoResponse {
    pub mode: String,
    pub region: String,
    #[serde(rename = "sqsARN")]
    pub sqs_arn: Vec<String>,
    #[serde(rename = "deploymentID")]
    pub deployment_id: String,
    pub buckets: BucketUsageInfo,
    pub objects: ObjectsUsageInfo,
    pub versions: ObjectsUsageInfo,
    #[serde(rename = "deleteMarkers")]
    pub delete_markers: ObjectsUsageInfo,
    pub usage: UsageInfo,
    pub services: ServicesInfo,
    pub backend: BackendInfo,
    pub servers: Vec<ServerProperties>,
}

#[derive(Debug, Serialize)]
pub struct BucketUsageInfo {
    pub count: u64,
}

#[derive(Debug, Serialize)]
pub struct ObjectsUsageInfo {
    pub count: u64,
}

#[derive(Debug, Serialize)]
pub struct UsageInfo {
    pub size: u64,
}

#[derive(Debug, Serialize)]
pub struct ServicesInfo {
    pub vault: ServiceStatus,
    pub ldap: ServiceStatus,
    pub notifications: Vec<Value>,
    pub audit: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct ServiceStatus {
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct BackendInfo {
    #[serde(rename = "backendType")]
    pub backend_type: String,
    #[serde(rename = "rrSCParity")]
    pub rr_sc_parity: i32,
    #[serde(rename = "standardSCParity")]
    pub standard_sc_parity: i32,
}

#[derive(Debug, Serialize)]
pub struct ServerProperties {
    pub state: String,
    pub endpoint: String,
    pub uptime: u64,
    pub version: String,
    #[serde(rename = "commitID")]
    pub commit_id: String,
    pub network: std::collections::HashMap<String, String>,
    pub drives: Vec<DriveInfo>,
}

#[derive(Debug, Serialize)]
pub struct DriveInfo {
    pub uuid: String,
    pub endpoint: String,
    pub state: String,
    #[serde(rename = "rootDisk")]
    pub root_disk: bool,
    #[serde(rename = "drivePath")]
    pub drive_path: String,
    #[serde(rename = "totalSpace")]
    pub total_space: u64,
    #[serde(rename = "usedSpace")]
    pub used_space: u64,
    #[serde(rename = "availableSpace")]
    pub available_space: u64,
}

pub async fn server_info() -> Result<impl IntoResponse, S3Error> {
    let uptime = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let info = ServerInfoResponse {
        mode: "local".to_string(),
        region: "us-east-1".to_string(),
        sqs_arn: vec![],
        deployment_id: "maxio-deployment".to_string(),
        buckets: BucketUsageInfo { count: 0 },
        objects: ObjectsUsageInfo { count: 0 },
        versions: ObjectsUsageInfo { count: 0 },
        delete_markers: ObjectsUsageInfo { count: 0 },
        usage: UsageInfo { size: 0 },
        services: ServicesInfo {
            vault: ServiceStatus {
                status: "disabled".to_string(),
            },
            ldap: ServiceStatus {
                status: "disabled".to_string(),
            },
            notifications: vec![],
            audit: vec![],
        },
        backend: BackendInfo {
            backend_type: "Erasure".to_string(),
            rr_sc_parity: 2,
            standard_sc_parity: 2,
        },
        servers: vec![ServerProperties {
            state: "ok".to_string(),
            endpoint: "localhost:9000".to_string(),
            uptime,
            version: env!("CARGO_PKG_VERSION").to_string(),
            commit_id: "maxio".to_string(),
            network: std::collections::HashMap::new(),
            drives: vec![],
        }],
    };

    Ok((StatusCode::OK, Json(info)))
}

pub async fn add_user(
    Extension(iam): Extension<Arc<IAMSys>>,
    Json(payload): Json<AddUserRequest>,
) -> Result<impl IntoResponse, S3Error> {
    let user = iam
        .create_user(&payload.access_key, &payload.secret_key)
        .await?;
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
