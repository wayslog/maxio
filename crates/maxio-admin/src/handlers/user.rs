use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
};
use maxio_common::error::MaxioError;

use crate::{
    AdminSys,
    handlers::AdminApiError,
    types::{AccessKeyQuery, AddUserRequest, MessageResponse, UserInfo},
};

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

fn to_user_info(user: &maxio_iam::User) -> UserInfo {
    UserInfo {
        access_key: user.access_key.clone(),
        policy_names: user.policy_names.clone(),
        created_at: user.created_at,
    }
}
