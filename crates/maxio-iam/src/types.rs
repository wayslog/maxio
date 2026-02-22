use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub access_key: String,
    pub secret_key: String,
    #[serde(default)]
    pub policy_names: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub name: String,
    #[serde(default = "default_version", alias = "Version")]
    pub version: String,
    #[serde(
        default,
        alias = "Statement",
        deserialize_with = "policy_statements_from_single_or_many"
    )]
    pub statements: Vec<PolicyStatement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyStatement {
    #[serde(alias = "Effect")]
    pub effect: Effect,
    #[serde(alias = "Action", deserialize_with = "string_or_vec")]
    pub actions: Vec<String>,
    #[serde(alias = "Resource", deserialize_with = "string_or_vec")]
    pub resources: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

fn default_version() -> String {
    "2012-10-17".to_string()
}

fn policy_statements_from_single_or_many<'de, D>(
    deserializer: D,
) -> Result<Vec<PolicyStatement>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StatementValue {
        One(PolicyStatement),
        Many(Vec<PolicyStatement>),
    }

    match StatementValue::deserialize(deserializer)? {
        StatementValue::One(statement) => Ok(vec![statement]),
        StatementValue::Many(statements) => Ok(statements),
    }
}

fn string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrVec {
        One(String),
        Many(Vec<String>),
    }

    match StringOrVec::deserialize(deserializer)? {
        StringOrVec::One(value) => Ok(vec![value]),
        StringOrVec::Many(values) => Ok(values),
    }
}

/// Service account - a programmatic access credential tied to a parent user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceAccount {
    pub access_key: String,
    pub secret_key: String,
    pub parent_user: String,
    #[serde(default)]
    pub session_policy: Option<Policy>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub expiration: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
}

impl ServiceAccount {
    pub fn is_expired(&self) -> bool {
        self.expiration.map_or(false, |exp| exp < Utc::now())
    }
}
