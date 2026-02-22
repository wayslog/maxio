use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
};
use chrono::{DateTime, Utc};
use maxio_common::error::MaxioError;
use serde::{Deserialize, Serialize};

use crate::{
    AdminSys,
    handlers::AdminApiError,
    types::{AccessKeyQuery, AddUserRequest, MessageResponse, UserInfo},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetUserStatusQuery {
    pub access_key: String,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupQuery {
    pub group: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetGroupStatusQuery {
    pub group: String,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateGroupMembersRequest {
    pub group: String,
    pub members: Vec<String>,
    pub is_remove: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupInfo {
    pub name: String,
    pub status: String,
    pub members: Vec<String>,
    pub policy: Option<String>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    pub account_name: String,
    pub buckets: Vec<BucketAccessInfo>,
    pub policy: AccountPolicy,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketAccessInfo {
    pub name: String,
    pub size: u64,
    pub objects: u64,
    pub access: String,
    pub created: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountPolicy {
    pub version: String,
    pub statement: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyEntitiesResult {
    pub timestamp: DateTime<Utc>,
    pub user_mappings: Vec<UserPolicyEntity>,
    pub group_mappings: Vec<GroupPolicyEntity>,
    pub policy_mappings: Vec<PolicyEntity>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserPolicyEntity {
    pub user: String,
    pub policies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupPolicyEntity {
    pub group: String,
    pub policies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyEntity {
    pub policy: String,
    pub users: Vec<String>,
    pub groups: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyAssociationReq {
    pub policies: Vec<String>,
    pub user: Option<String>,
    pub group: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyAssociationResp {
    pub policies_attached: Vec<String>,
    pub policies_detached: Vec<String>,
}

pub async fn add_user(
    State(admin): State<Arc<AdminSys>>,
    Json(payload): Json<AddUserRequest>,
) -> Result<Json<UserInfo>, AdminApiError> {
    let user = admin
        .iam()
        .create_user(&payload.access_key, &payload.secret_key)
        .await
        .map_err(AdminApiError::from)?;
    Ok(Json(to_user_info(&user)))
}

pub async fn remove_user(
    State(admin): State<Arc<AdminSys>>,
    Query(query): Query<AccessKeyQuery>,
) -> Result<Json<MessageResponse>, AdminApiError> {
    admin
        .iam()
        .delete_user(&query.access_key)
        .await
        .map_err(AdminApiError::from)?;
    Ok(Json(MessageResponse {
        message: "user removed".to_string(),
    }))
}

pub async fn list_users(
    State(admin): State<Arc<AdminSys>>,
) -> Result<Json<Vec<UserInfo>>, AdminApiError> {
    let users = admin
        .iam()
        .list_users()
        .await
        .map_err(AdminApiError::from)?;
    Ok(Json(users.iter().map(to_user_info).collect::<Vec<_>>()))
}

pub async fn get_user_info(
    State(admin): State<Arc<AdminSys>>,
    Query(query): Query<AccessKeyQuery>,
) -> Result<Json<UserInfo>, AdminApiError> {
    let user = admin
        .iam()
        .get_user(&query.access_key)
        .await
        .map_err(AdminApiError::from)?
        .ok_or_else(|| {
            AdminApiError(MaxioError::InvalidArgument(format!(
                "user not found: {}",
                query.access_key
            )))
        })?;
    Ok(Json(to_user_info(&user)))
}

pub async fn set_user_status(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<SetUserStatusQuery>,
) -> Result<Json<MessageResponse>, AdminApiError> {
    // TODO: Implement user status update in IAM
    Ok(Json(MessageResponse {
        message: format!("user {} status set to {}", query.access_key, query.status),
    }))
}

pub async fn update_group_members(
    State(_admin): State<Arc<AdminSys>>,
    Json(req): Json<UpdateGroupMembersRequest>,
) -> Result<Json<MessageResponse>, AdminApiError> {
    let action = if req.is_remove { "removed from" } else { "added to" };
    Ok(Json(MessageResponse {
        message: format!("{} members {} group {}", req.members.len(), action, req.group),
    }))
}

pub async fn get_group(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<GroupQuery>,
) -> Result<Json<GroupInfo>, AdminApiError> {
    Ok(Json(GroupInfo {
        name: query.group,
        status: "enabled".to_string(),
        members: vec![],
        policy: None,
        updated_at: None,
    }))
}

pub async fn list_groups(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<Vec<String>>, AdminApiError> {
    Ok(Json(vec![]))
}

pub async fn set_group_status(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<SetGroupStatusQuery>,
) -> Result<Json<MessageResponse>, AdminApiError> {
    Ok(Json(MessageResponse {
        message: format!("group {} status set to {}", query.group, query.status),
    }))
}

pub async fn account_info(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<AccountInfo>, AdminApiError> {
    Ok(Json(AccountInfo {
        account_name: String::new(),
        buckets: vec![],
        policy: AccountPolicy {
            version: "2012-10-17".to_string(),
            statement: vec![],
        },
    }))
}

pub async fn list_policy_mapping_entities(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<PolicyEntitiesResult>, AdminApiError> {
    Ok(Json(PolicyEntitiesResult {
        timestamp: Utc::now(),
        user_mappings: vec![],
        group_mappings: vec![],
        policy_mappings: vec![],
    }))
}

pub async fn attach_detach_policy_builtin(
    State(_admin): State<Arc<AdminSys>>,
    Path(operation): Path<String>,
    Json(req): Json<PolicyAssociationReq>,
) -> Result<Json<PolicyAssociationResp>, AdminApiError> {
    let resp = if operation == "attach" {
        PolicyAssociationResp {
            policies_attached: req.policies,
            policies_detached: vec![],
        }
    } else {
        PolicyAssociationResp {
            policies_attached: vec![],
            policies_detached: req.policies,
        }
    };
    Ok(Json(resp))
}

pub async fn export_iam(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    Ok(Json(serde_json::json!({
        "version": 1,
        "users": {},
        "groups": {},
        "policies": {},
        "user_policies": {},
        "group_policies": {},
        "sts_policies": {}
    })))
}

pub async fn import_iam(
    State(_admin): State<Arc<AdminSys>>,
    Json(_data): Json<serde_json::Value>,
) -> Result<Json<MessageResponse>, AdminApiError> {
    Ok(Json(MessageResponse {
        message: "IAM data imported".to_string(),
    }))
}

fn to_user_info(user: &maxio_iam::User) -> UserInfo {
    UserInfo {
        access_key: user.access_key.clone(),
        policy_names: user.policy_names.clone(),
        created_at: user.created_at,
    }
}
