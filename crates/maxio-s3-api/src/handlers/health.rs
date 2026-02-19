use std::sync::Arc;

use axum::{Json, extract::Extension, http::StatusCode, response::IntoResponse};
use maxio_distributed::DistributedSys;

pub async fn health_live() -> impl IntoResponse {
    StatusCode::OK
}

pub async fn health_ready() -> impl IntoResponse {
    StatusCode::OK
}

pub async fn health_cluster(
    Extension(distributed): Extension<Arc<DistributedSys>>,
) -> impl IntoResponse {
    Json(distributed.get_cluster_status())
}

pub async fn health_cluster_read() -> impl IntoResponse {
    StatusCode::OK
}
