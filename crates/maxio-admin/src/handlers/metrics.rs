use std::sync::Arc;

use axum::{
    body::Body,
    extract::Request,
    extract::State,
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::time::Instant;

use crate::router::AdminState;

pub async fn prometheus_metrics(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    state.system_metrics.refresh();
    let payload = state.registry.render_prometheus();

    let mut response = Response::new(Body::from(payload));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );

    response
}

pub async fn track_api_metrics(
    State(state): State<Arc<AdminState>>,
    request: Request,
    next: Next,
) -> Response {
    let started_at = Instant::now();
    let method = request.method().as_str().to_string();

    let response = next.run(request).await;
    let status = response.status().as_u16();
    state
        .api_metrics
        .record_request(&method, status, started_at.elapsed());

    response
}
