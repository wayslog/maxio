use maxio_common::error::{MaxioError, Result};
use quick_xml::{de::from_str as xml_from_str, se::to_string as xml_to_string};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename = "ReplicationConfiguration")]
pub struct ReplicationConfig {
    #[serde(rename = "Role", default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(rename = "Rule", default)]
    pub rules: Vec<ReplicationRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationRule {
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "Status")]
    pub status: RuleStatus,
    #[serde(rename = "Priority", default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u32>,
    #[serde(rename = "Filter", default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<ReplicationFilter>,
    #[serde(rename = "Destination")]
    pub destination: ReplicationDestination,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleStatus {
    #[serde(rename = "Enabled")]
    Enabled,
    #[serde(rename = "Disabled")]
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationFilter {
    #[serde(rename = "Prefix", default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationDestination {
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(
        rename = "StorageClass",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub storage_class: Option<String>,
    #[serde(rename = "Account", default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
}

impl ReplicationConfig {
    pub fn from_xml(xml: &str) -> Result<Self> {
        xml_from_str(xml)
            .map_err(|err| MaxioError::InvalidArgument(format!("invalid replication xml: {err}")))
    }

    pub fn to_xml(&self) -> Result<String> {
        xml_to_string(self).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize replication xml: {err}"))
        })
    }

    pub fn enabled_rules(&self) -> impl Iterator<Item = &ReplicationRule> {
        self.rules
            .iter()
            .filter(|rule| rule.status == RuleStatus::Enabled)
    }
}
