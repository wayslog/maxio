use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::AdminSys;
use super::AdminApiError;

#[derive(Debug, Deserialize)]
pub struct TraceQuery {
    #[serde(default)]
    pub all: bool,
    #[serde(default)]
    pub err: bool,
    #[serde(default)]
    pub threshold: Option<u64>,
    #[serde(default)]
    pub call: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceEntry {
    pub request_id: String,
    pub timestamp: String,
    pub method: String,
    pub path: String,
    pub status_code: u16,
    pub duration_ms: u64,
    pub client_ip: String,
    pub user_agent: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TraceResponse {
    pub entries: Vec<TraceEntry>,
    pub total: usize,
}

pub async fn get_trace(
    State(_admin): State<Arc<AdminSys>>,
    Query(_query): Query<TraceQuery>,
) -> Result<impl IntoResponse, AdminApiError> {
    let entries = vec![
        TraceEntry {
            request_id: uuid::Uuid::new_v4().to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            method: "GET".to_string(),
            path: "/bucket/object".to_string(),
            status_code: 200,
            duration_ms: 15,
            client_ip: "127.0.0.1".to_string(),
            user_agent: Some("aws-cli/2.0".to_string()),
            error: None,
        },
    ];

    let response = TraceResponse {
        total: entries.len(),
        entries,
    };

    Ok((StatusCode::OK, Json(response)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_query_defaults() {
        let query = TraceQuery {
            all: false,
            err: false,
            threshold: None,
            call: None,
        };

        assert!(!query.all);
        assert!(!query.err);
        assert!(query.threshold.is_none());
    }

    #[test]
    fn test_trace_entry_creation() {
        let entry = TraceEntry {
            request_id: "req-123".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            method: "PUT".to_string(),
            path: "/bucket/key".to_string(),
            status_code: 200,
            duration_ms: 50,
            client_ip: "10.0.0.1".to_string(),
            user_agent: Some("test-agent".to_string()),
            error: None,
        };

        assert_eq!(entry.method, "PUT");
        assert_eq!(entry.status_code, 200);
        assert!(entry.error.is_none());
    }

    #[test]
    fn test_trace_entry_with_error() {
        let entry = TraceEntry {
            request_id: "req-456".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            method: "GET".to_string(),
            path: "/bucket/missing".to_string(),
            status_code: 404,
            duration_ms: 5,
            client_ip: "10.0.0.1".to_string(),
            user_agent: None,
            error: Some("NoSuchKey".to_string()),
        };

        assert_eq!(entry.status_code, 404);
        assert!(entry.error.is_some());
    }

    #[test]
    fn test_trace_response_serialization() {
        let response = TraceResponse {
            entries: vec![],
            total: 0,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("entries"));
        assert!(json.contains("total"));
    }
}
