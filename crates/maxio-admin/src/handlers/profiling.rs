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
pub struct ProfilingQuery {
    #[serde(default = "default_duration")]
    pub duration: u64,
    #[serde(default)]
    pub profile_type: ProfileType,
}

fn default_duration() -> u64 {
    30
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProfileType {
    #[default]
    Cpu,
    Heap,
    Goroutine,
    Block,
    Mutex,
    Trace,
}

#[derive(Debug, Serialize)]
pub struct ProfilingResult {
    pub profile_type: ProfileType,
    pub duration_seconds: u64,
    pub data: String,
    pub timestamp: String,
}

pub async fn start_profiling(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<ProfilingQuery>,
) -> Result<impl IntoResponse, AdminApiError> {
    let duration = query.duration.min(300).max(1);
    let profile_type = query.profile_type;

    let result = ProfilingResult {
        profile_type,
        duration_seconds: duration,
        data: format!("profiling_data_placeholder_{:?}_{}", profile_type, duration),
        timestamp: chrono::Utc::now().to_rfc3339(),
    };

    Ok((StatusCode::OK, Json(result)))
}

pub async fn stop_profiling(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<impl IntoResponse, AdminApiError> {
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "stopped",
            "message": "Profiling stopped"
        })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_types() {
        let types = [
            ProfileType::Cpu,
            ProfileType::Heap,
            ProfileType::Goroutine,
            ProfileType::Block,
            ProfileType::Mutex,
            ProfileType::Trace,
        ];

        for t in &types {
            assert!(matches!(
                t,
                ProfileType::Cpu
                    | ProfileType::Heap
                    | ProfileType::Goroutine
                    | ProfileType::Block
                    | ProfileType::Mutex
                    | ProfileType::Trace
            ));
        }
    }

    #[test]
    fn test_default_duration() {
        assert_eq!(default_duration(), 30);
    }

    #[test]
    fn test_profiling_query_defaults() {
        let query = ProfilingQuery {
            duration: default_duration(),
            profile_type: ProfileType::default(),
        };

        assert_eq!(query.duration, 30);
        assert_eq!(query.profile_type, ProfileType::Cpu);
    }

    #[test]
    fn test_profiling_result_serialization() {
        let result = ProfilingResult {
            profile_type: ProfileType::Cpu,
            duration_seconds: 30,
            data: "test_data".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("cpu"));
        assert!(json.contains("30"));
    }
}
