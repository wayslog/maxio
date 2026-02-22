use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditEventType {
    GetObject,
    PutObject,
    DeleteObject,
    CopyObject,
    ListObjects,
    CreateBucket,
    DeleteBucket,
    ListBuckets,
    GetBucketPolicy,
    PutBucketPolicy,
    DeleteBucketPolicy,
    Login,
    Logout,
    CreateUser,
    DeleteUser,
    CreatePolicy,
    DeletePolicy,
    AdminAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: String,
    pub event_type: AuditEventType,
    pub timestamp: DateTime<Utc>,
    pub source_ip: String,
    pub user_agent: Option<String>,
    pub access_key: Option<String>,
    pub bucket: Option<String>,
    pub object: Option<String>,
    pub request_id: String,
    pub status_code: u16,
    pub error_message: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl AuditEvent {
    pub fn new(event_type: AuditEventType, request_id: String, source_ip: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            event_type,
            timestamp: Utc::now(),
            source_ip,
            user_agent: None,
            access_key: None,
            bucket: None,
            object: None,
            request_id,
            status_code: 200,
            error_message: None,
            metadata: HashMap::new(),
        }
    }

    pub fn with_bucket(mut self, bucket: &str) -> Self {
        self.bucket = Some(bucket.to_string());
        self
    }

    pub fn with_object(mut self, object: &str) -> Self {
        self.object = Some(object.to_string());
        self
    }

    pub fn with_access_key(mut self, access_key: &str) -> Self {
        self.access_key = Some(access_key.to_string());
        self
    }

    pub fn with_status(mut self, status_code: u16) -> Self {
        self.status_code = status_code;
        self
    }

    pub fn with_error(mut self, message: &str) -> Self {
        self.error_message = Some(message.to_string());
        self
    }
}
