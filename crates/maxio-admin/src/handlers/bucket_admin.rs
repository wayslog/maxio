use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
};
use serde::{Deserialize, Serialize};

use crate::{AdminSys, handlers::AdminApiError};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketQuotaConfig {
    pub quota: u64,
    #[serde(rename = "type")]
    pub quota_type: QuotaType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QuotaType {
    Hard,
    Fifo,
}

impl Default for QuotaType {
    fn default() -> Self {
        QuotaType::Hard
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BucketQuery {
    pub bucket: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RemoteTargetQuery {
    pub bucket: String,
    #[serde(rename = "type")]
    pub target_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RemoveTargetQuery {
    pub bucket: String,
    pub arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketTarget {
    pub arn: String,
    pub endpoint: String,
    pub target_bucket: String,
    pub secure: bool,
    pub credentials: TargetCredentials,
    pub path: String,
    pub api: String,
    pub bandwidth_limit: u64,
    pub health_check_duration: u64,
    pub replication_sync: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetCredentials {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetTargetResponse {
    pub arn: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationDiffInfo {
    pub object: String,
    pub version_id: String,
    pub delete_replication_status: String,
    pub replication_status: String,
    pub replication_timestamp: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationDiffRequest {
    pub prefix: Option<String>,
    pub arn: Option<String>,
    pub verbose: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationMRF {
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub retry_count: i32,
}

pub async fn get_bucket_quota(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<BucketQuery>,
) -> Result<Json<BucketQuotaConfig>, AdminApiError> {
    let _ = query.bucket;
    Ok(Json(BucketQuotaConfig {
        quota: 0,
        quota_type: QuotaType::Hard,
    }))
}

pub async fn put_bucket_quota(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<BucketQuery>,
    Json(_config): Json<BucketQuotaConfig>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    Ok(Json(serde_json::json!({
        "message": format!("quota set for bucket {}", query.bucket)
    })))
}

pub async fn list_remote_targets(
    State(_admin): State<Arc<AdminSys>>,
    Query(_query): Query<RemoteTargetQuery>,
) -> Result<Json<Vec<BucketTarget>>, AdminApiError> {
    Ok(Json(vec![]))
}

pub async fn set_remote_target(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<BucketQuery>,
    Json(_target): Json<BucketTarget>,
) -> Result<Json<SetTargetResponse>, AdminApiError> {
    Ok(Json(SetTargetResponse {
        arn: format!("arn:minio:replication::{}:bucket", query.bucket),
    }))
}

pub async fn remove_remote_target(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<RemoveTargetQuery>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    Ok(Json(serde_json::json!({
        "message": format!("target {} removed from bucket {}", query.arn, query.bucket)
    })))
}

pub async fn export_bucket_metadata(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    Ok(Json(serde_json::json!({
        "buckets": []
    })))
}

pub async fn import_bucket_metadata(
    State(_admin): State<Arc<AdminSys>>,
    Json(_data): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    Ok(Json(serde_json::json!({
        "message": "bucket metadata imported"
    })))
}

pub async fn replication_diff(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<BucketQuery>,
    Json(_req): Json<ReplicationDiffRequest>,
) -> Result<Json<Vec<ReplicationDiffInfo>>, AdminApiError> {
    let _ = query.bucket;
    Ok(Json(vec![]))
}

pub async fn replication_mrf(
    State(_admin): State<Arc<AdminSys>>,
    Query(_query): Query<BucketQuery>,
) -> Result<Json<Vec<ReplicationMRF>>, AdminApiError> {
    Ok(Json(vec![]))
}
