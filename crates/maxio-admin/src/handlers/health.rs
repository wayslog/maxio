use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
};
use serde::Serialize;

use crate::router::AdminState;

pub async fn health_live() -> impl IntoResponse {
    StatusCode::OK
}

pub async fn health_ready(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    match state.object_layer.list_buckets().await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

pub async fn health_cluster(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let status = state.distributed.get_cluster_status();
    let write_quorum = calculate_write_quorum(status.total_nodes);
    let has_quorum = status.online_nodes >= write_quorum;

    let code = if has_quorum {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        code,
        Json(ClusterHealthResponse {
            online_nodes: status.online_nodes,
            total_nodes: status.total_nodes,
            write_quorum,
            has_write_quorum: has_quorum,
        }),
    )
}

fn calculate_write_quorum(total_nodes: usize) -> usize {
    if total_nodes == 0 {
        0
    } else {
        (total_nodes / 2) + 1
    }
}

#[derive(Serialize)]
struct ClusterHealthResponse {
    online_nodes: usize,
    total_nodes: usize,
    write_quorum: usize,
    has_write_quorum: bool,
}
