use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
};
use maxio_common::error::MaxioError;

use crate::{
    AdminSys,
    handlers::AdminApiError,
    types::{ConfigKV, ConfigKVSetRequest, ConfigSetRequest, KeyQuery, MessageResponse},
};

pub async fn get_config(
    State(admin): State<Arc<AdminSys>>,
) -> Result<Json<Vec<ConfigKV>>, AdminApiError> {
    let mut values = admin
        .get_config_map()
        .map_err(AdminApiError::from)?
        .into_iter()
        .map(|(key, value)| ConfigKV { key, value })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(Json(values))
}

pub async fn set_config(
    State(admin): State<Arc<AdminSys>>,
    Json(payload): Json<ConfigSetRequest>,
) -> Result<Json<MessageResponse>, AdminApiError> {
    for key in payload.values.keys() {
        validate_kv_key(key)?;
    }

    admin
        .set_config_map(payload.values)
        .map_err(AdminApiError::from)?;

    Ok(Json(MessageResponse {
        message: "config updated".to_string(),
    }))
}

pub async fn get_config_kv(
    State(admin): State<Arc<AdminSys>>,
    Query(query): Query<KeyQuery>,
) -> Result<Json<ConfigKV>, AdminApiError> {
    validate_kv_key(&query.key)?;

    let value = admin
        .get_config_value(&query.key)
        .map_err(AdminApiError::from)?
        .ok_or_else(|| {
            AdminApiError(MaxioError::InvalidArgument(format!("config key not found: {}", query.key)))
        })?;

    Ok(Json(ConfigKV {
        key: query.key,
        value,
    }))
}

pub async fn set_config_kv(
    State(admin): State<Arc<AdminSys>>,
    Json(payload): Json<ConfigKVSetRequest>,
) -> Result<Json<MessageResponse>, AdminApiError> {
    validate_kv_key(&payload.key)?;
    admin
        .set_config_value(&payload.key, payload.value)
        .map_err(AdminApiError::from)?;

    Ok(Json(MessageResponse {
        message: "config value updated".to_string(),
    }))
}

pub async fn delete_config_kv(
    State(admin): State<Arc<AdminSys>>,
    Query(query): Query<KeyQuery>,
) -> Result<Json<MessageResponse>, AdminApiError> {
    validate_kv_key(&query.key)?;
    admin
        .delete_config_value(&query.key)
        .map_err(AdminApiError::from)?;

    Ok(Json(MessageResponse {
        message: "config value removed".to_string(),
    }))
}

fn validate_kv_key(key: &str) -> Result<(), AdminApiError> {
    if key.split_once(':').is_some_and(|(subsystem, name)| {
        !subsystem.is_empty() && !name.is_empty() && !name.contains(':')
    }) {
        return Ok(());
    }

    Err(AdminApiError(MaxioError::InvalidArgument(
        "config key must use subsystem:key format".to_string(),
    )))
}
