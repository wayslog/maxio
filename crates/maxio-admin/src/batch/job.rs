use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::batch::types::{JobStatus, JobType};

#[derive(Debug, Clone, Serialize)]
pub struct BatchJob {
    pub id: String,
    pub job_type: JobType,
    pub status: JobStatus,
    pub progress: u8,
    pub created_at: DateTime<Utc>,
    pub error: Option<String>,
}
