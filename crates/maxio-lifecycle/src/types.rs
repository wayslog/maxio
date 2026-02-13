use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename = "LifecycleConfiguration")]
pub struct LifecycleConfiguration {
    #[serde(rename = "Rule", default)]
    pub rules: Vec<LifecycleRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "Status")]
    pub status: RuleStatus,
    #[serde(rename = "Filter", default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<LifecycleFilter>,
    #[serde(
        rename = "Expiration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub expiration: Option<Expiration>,
    #[serde(
        rename = "NoncurrentVersionExpiration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpiration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleStatus {
    #[serde(rename = "Enabled")]
    Enabled,
    #[serde(rename = "Disabled")]
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleFilter {
    #[serde(rename = "Prefix", default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Expiration {
    #[serde(rename = "Days", default, skip_serializing_if = "Option::is_none")]
    pub days: Option<i32>,
    #[serde(rename = "Date", default, skip_serializing_if = "Option::is_none")]
    pub date: Option<DateTime<Utc>>,
    #[serde(
        rename = "ExpiredObjectDeleteMarker",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub expired_object_delete_marker: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoncurrentVersionExpiration {
    #[serde(rename = "NoncurrentDays")]
    pub noncurrent_days: i32,
}
