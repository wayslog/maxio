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
pub struct SpeedtestQuery {
    #[serde(default = "default_size")]
    pub size: u64,
    #[serde(default = "default_concurrent")]
    pub concurrent: u32,
    #[serde(default = "default_duration")]
    pub duration: u64,
    #[serde(default)]
    pub autotune: bool,
}

fn default_size() -> u64 {
    64 * 1024 * 1024
}

fn default_concurrent() -> u32 {
    32
}

fn default_duration() -> u64 {
    10
}

#[derive(Debug, Serialize)]
pub struct SpeedtestResult {
    pub upload_speed_mbps: f64,
    pub download_speed_mbps: f64,
    pub latency_ms: f64,
    pub concurrent: u32,
    pub object_size: u64,
    pub duration_seconds: u64,
    pub timestamp: String,
}

#[derive(Debug, Serialize)]
pub struct DriveSpeedResult {
    pub drive: String,
    pub read_speed_mbps: f64,
    pub write_speed_mbps: f64,
    pub latency_ms: f64,
}

#[derive(Debug, Serialize)]
pub struct NetSpeedResult {
    pub peer: String,
    pub latency_ms: f64,
    pub throughput_mbps: f64,
}

pub async fn run_speedtest(
    State(_admin): State<Arc<AdminSys>>,
    Query(query): Query<SpeedtestQuery>,
) -> Result<impl IntoResponse, AdminApiError> {
    let result = SpeedtestResult {
        upload_speed_mbps: 1000.0,
        download_speed_mbps: 1200.0,
        latency_ms: 1.5,
        concurrent: query.concurrent,
        object_size: query.size,
        duration_seconds: query.duration,
        timestamp: chrono::Utc::now().to_rfc3339(),
    };

    Ok((StatusCode::OK, Json(result)))
}

pub async fn run_drivespeed(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<impl IntoResponse, AdminApiError> {
    let results = vec![DriveSpeedResult {
        drive: "/data1".to_string(),
        read_speed_mbps: 500.0,
        write_speed_mbps: 450.0,
        latency_ms: 0.5,
    }];

    Ok((StatusCode::OK, Json(results)))
}

pub async fn run_netspeed(
    State(_admin): State<Arc<AdminSys>>,
) -> Result<impl IntoResponse, AdminApiError> {
    let results = vec![NetSpeedResult {
        peer: "node2:9000".to_string(),
        latency_ms: 0.3,
        throughput_mbps: 9500.0,
    }];

    Ok((StatusCode::OK, Json(results)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_speedtest_defaults() {
        assert_eq!(default_size(), 64 * 1024 * 1024);
        assert_eq!(default_concurrent(), 32);
        assert_eq!(default_duration(), 10);
    }

    #[test]
    fn test_speedtest_query() {
        let query = SpeedtestQuery {
            size: default_size(),
            concurrent: default_concurrent(),
            duration: default_duration(),
            autotune: false,
        };

        assert_eq!(query.size, 64 * 1024 * 1024);
        assert_eq!(query.concurrent, 32);
        assert!(!query.autotune);
    }

    #[test]
    fn test_speedtest_result_serialization() {
        let result = SpeedtestResult {
            upload_speed_mbps: 1000.0,
            download_speed_mbps: 1200.0,
            latency_ms: 1.5,
            concurrent: 32,
            object_size: 64 * 1024 * 1024,
            duration_seconds: 10,
            timestamp: "2024-01-01T00:00:00Z".to_string(),
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("upload_speed_mbps"));
        assert!(json.contains("download_speed_mbps"));
    }

    #[test]
    fn test_drive_speed_result() {
        let result = DriveSpeedResult {
            drive: "/data1".to_string(),
            read_speed_mbps: 500.0,
            write_speed_mbps: 450.0,
            latency_ms: 0.5,
        };

        assert_eq!(result.drive, "/data1");
        assert!(result.read_speed_mbps > result.write_speed_mbps);
    }

    #[test]
    fn test_net_speed_result() {
        let result = NetSpeedResult {
            peer: "node2:9000".to_string(),
            latency_ms: 0.3,
            throughput_mbps: 9500.0,
        };

        assert!(result.latency_ms < 1.0);
        assert!(result.throughput_mbps > 1000.0);
    }
}
