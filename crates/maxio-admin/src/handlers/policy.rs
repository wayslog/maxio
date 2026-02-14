use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
};
use maxio_common::error::MaxioError;
use serde::Deserialize;

use crate::{
    AdminSys,
    handlers::AdminApiError,
    types::{MessageResponse, PolicyPutRequest},
};

#[derive(Debug, Deserialize)]
pub struct PolicyNameQuery {
    #[serde(rename = "policyName")]
    pub policy_name: String,
}

pub async fn add_policy(
    State(admin): State<Arc<AdminSys>>,
    Json(payload): Json<PolicyPutRequest>,
) -> Result<Json<MessageResponse>, AdminApiError> {
    let mut policy: maxio_iam::Policy =
        serde_json::from_value(payload.policy).map_err(|err| {
            AdminApiError(MaxioError::InvalidArgument(format!(
                "failed to parse policy document: {err}"
            )))
        })?;

    if policy.name.is_empty() {
        policy.name = payload.name;
    }

    if policy.name.is_empty() {
        return Err(AdminApiError(MaxioError::InvalidArgument(
            "policy name is required".to_string(),
        )));
    }

    admin
        .iam()
        .create_policy(policy.clone())
        .await
        .map_err(AdminApiError::from)?;
    admin.remember_policy(policy).map_err(AdminApiError::from)?;

    Ok(Json(MessageResponse {
        message: "policy stored".to_string(),
    }))
}

pub async fn remove_policy(
    State(admin): State<Arc<AdminSys>>,
    Query(query): Query<PolicyNameQuery>,
) -> Result<Json<MessageResponse>, AdminApiError> {
    admin
        .iam()
        .delete_policy(&query.policy_name)
        .await
        .map_err(AdminApiError::from)?;
    admin
        .remove_remembered_policy(&query.policy_name)
        .map_err(AdminApiError::from)?;

    Ok(Json(MessageResponse {
        message: "policy removed".to_string(),
    }))
}

pub async fn list_policies(
    State(admin): State<Arc<AdminSys>>,
) -> Result<Json<Vec<maxio_iam::Policy>>, AdminApiError> {
    let policies = admin.list_remembered_policies().map_err(AdminApiError::from)?;
    Ok(Json(policies))
}
