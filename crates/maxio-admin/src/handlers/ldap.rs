use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{AdminSys, handlers::AdminApiError};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyEntitiesResult {
    pub timestamp: DateTime<Utc>,
    pub user_mappings: Vec<UserPolicyMapping>,
    pub group_mappings: Vec<GroupPolicyMapping>,
    pub policy_mappings: Vec<PolicyMapping>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserPolicyMapping {
    pub user: String,
    pub policies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupPolicyMapping {
    pub group: String,
    pub policies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyMapping {
    pub policy: String,
    pub users: Vec<String>,
    pub groups: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyEntitiesQuery {
    pub user: Option<Vec<String>>,
    pub group: Option<Vec<String>>,
    pub policy: Option<Vec<String>>,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddServiceAccountLDAPReq {
    pub target_user: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub policy: Option<serde_json::Value>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub expiration: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddServiceAccountResp {
    pub access_key: String,
    pub secret_key: String,
    pub parent_user: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAccessKeysQuery {
    pub user_dn: String,
    pub list_type: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAccessKeysBulkQuery {
    pub list_type: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAccessKeysLDAPResp {
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

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAccessKeysLDAPBulkResp {
    pub users: Vec<LDAPUserAccessKeys>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LDAPUserAccessKeys {
    pub user_dn: String,
    pub access_keys: Vec<ServiceAccountInfo>,
}

pub async fn list_ldap_policy_mapping_entities(
    State(_admin): State<Arc<AdminSys>>,
    Query(_query): Query<PolicyEntitiesQuery>,
) -> Result<Json<PolicyEntitiesResult>, AdminApiError> {
    Ok(Json(PolicyEntitiesResult {
        timestamp: Utc::now(),
        user_mappings: vec![],
        group_mappings: vec![],
        policy_mappings: vec![],
    }))
}

pub async fn attach_detach_policy_ldap(
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

pub async fn add_service_account_ldap(
    State(admin): State<Arc<AdminSys>>,
    Json(req): Json<AddServiceAccountLDAPReq>,
) -> Result<Json<AddServiceAccountResp>, AdminApiError> {
    let session_policy = if let Some(policy_json) = req.policy {
        let policy: maxio_iam::Policy = serde_json::from_value(policy_json).map_err(|e| {
            AdminApiError(maxio_common::error::MaxioError::InvalidArgument(format!(
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
            &req.target_user,
            req.access_key.as_deref(),
            req.secret_key.as_deref(),
            session_policy,
            req.name,
            req.description,
            req.expiration,
        )
        .await
        .map_err(AdminApiError::from)?;

    Ok(Json(AddServiceAccountResp {
        access_key: sa.access_key,
        secret_key: sa.secret_key,
        parent_user: sa.parent_user,
    }))
}

pub async fn list_access_keys_ldap(
    State(admin): State<Arc<AdminSys>>,
    Query(query): Query<ListAccessKeysQuery>,
) -> Result<Json<ListAccessKeysLDAPResp>, AdminApiError> {
    let accounts = admin
        .iam()
        .list_service_accounts(&query.user_dn)
        .await
        .map_err(AdminApiError::from)?;

    Ok(Json(ListAccessKeysLDAPResp {
        access_keys: accounts
            .into_iter()
            .map(|sa| ServiceAccountInfo {
                access_key: sa.access_key,
                parent_user: sa.parent_user,
                name: sa.name,
                description: sa.description,
                expiration: sa.expiration,
            })
            .collect(),
    }))
}

pub async fn list_access_keys_ldap_bulk(
    State(_admin): State<Arc<AdminSys>>,
    Query(_query): Query<ListAccessKeysBulkQuery>,
) -> Result<Json<ListAccessKeysLDAPBulkResp>, AdminApiError> {
    // Return empty - would need to iterate all LDAP users
    Ok(Json(ListAccessKeysLDAPBulkResp { users: vec![] }))
}
