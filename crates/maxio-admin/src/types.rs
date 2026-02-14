use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::batch::{ExpirationJobConfig, JobType};

#[derive(Debug, Clone, Serialize)]
pub struct AdminInfo {
    pub version: String,
    pub uptime_seconds: u64,
    pub boot_time: DateTime<Utc>,
    pub server: ServerProperties,
    pub storage: StorageInfo,
    pub services: ServiceStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerProperties {
    pub endpoint: String,
    pub region: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageInfo {
    pub used_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceStatus {
    pub iam: String,
    pub storage: String,
    pub distributed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigKV {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSetRequest {
    pub values: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigKVSetRequest {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageResponse {
    pub message: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KeyQuery {
    pub key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccessKeyQuery {
    #[serde(rename = "accessKey")]
    pub access_key: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct UserInfo {
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "policyNames")]
    pub policy_names: Vec<String>,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AddUserRequest {
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolicyPutRequest {
    pub name: String,
    pub policy: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchJobSubmitRequest {
    pub job_type: JobType,
    pub expiration: Option<ExpirationJobConfig>,
}
