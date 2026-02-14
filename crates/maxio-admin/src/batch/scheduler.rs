use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use maxio_common::error::{MaxioError, Result};
use maxio_storage::traits::ObjectLayer;
use tokio::{sync::RwLock, task::JoinHandle};
use uuid::Uuid;

use crate::batch::{
    expiration::{ExpirationJobConfig, collect_expired_keys},
    job::BatchJob,
    types::{JobStatus, JobType},
};

#[derive(Clone)]
pub struct JobScheduler {
    object_layer: Arc<dyn ObjectLayer>,
    jobs: Arc<RwLock<HashMap<String, BatchJob>>>,
    tasks: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
}

impl JobScheduler {
    pub fn new(object_layer: Arc<dyn ObjectLayer>) -> Self {
        Self {
            object_layer,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn submit_job(
        &self,
        job_type: JobType,
        expiration: Option<ExpirationJobConfig>,
    ) -> Result<BatchJob> {
        if job_type == JobType::Expiration {
            expiration
                .as_ref()
                .ok_or_else(|| {
                    MaxioError::InvalidArgument(
                        "expiration payload is required for expiration jobs".to_string(),
                    )
                })?
                .validate()?;
        }

        let id = Uuid::new_v4().to_string();
        let job = BatchJob {
            id: id.clone(),
            job_type,
            status: JobStatus::Pending,
            progress: 0,
            created_at: Utc::now(),
            error: None,
        };

        self.jobs.write().await.insert(id.clone(), job.clone());

        let scheduler = self.clone();
        let handle = tokio::spawn(async move {
            scheduler.run_job(id, job_type, expiration).await;
        });
        self.tasks.write().await.insert(job.id.clone(), handle);

        Ok(job)
    }

    pub async fn get_job(&self, id: &str) -> Option<BatchJob> {
        self.jobs.read().await.get(id).cloned()
    }

    pub async fn list_jobs(&self) -> Vec<BatchJob> {
        let mut jobs: Vec<BatchJob> = self.jobs.read().await.values().cloned().collect();
        jobs.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        jobs
    }

    pub async fn cancel_job(&self, id: &str) -> Result<BatchJob> {
        let handle = self.tasks.write().await.remove(id);
        if let Some(handle) = handle {
            handle.abort();
        }

        let mut jobs = self.jobs.write().await;
        let job = jobs.get_mut(id).ok_or_else(|| {
            MaxioError::InvalidArgument(format!("batch job not found: {id}"))
        })?;
        if job.status == JobStatus::Completed || job.status == JobStatus::Failed {
            return Ok(job.clone());
        }

        job.status = JobStatus::Failed;
        job.error = Some("job cancelled".to_string());
        Ok(job.clone())
    }

    async fn run_job(&self, id: String, job_type: JobType, expiration: Option<ExpirationJobConfig>) {
        self.update_status(&id, JobStatus::Running).await;

        let result = match job_type {
            JobType::Expiration => {
                self.run_expiration_job(&id, expiration).await
            }
            JobType::Replication | JobType::KeyRotation => Err(MaxioError::NotImplemented(
                "batch job type is not implemented yet".to_string(),
            )),
        };

        match result {
            Ok(()) => {
                self.update_progress(&id, 100).await;
                self.update_status(&id, JobStatus::Completed).await;
                self.clear_error(&id).await;
            }
            Err(err) => {
                self.update_status(&id, JobStatus::Failed).await;
                self.set_error(&id, err.to_string()).await;
            }
        }

        self.tasks.write().await.remove(&id);
    }

    async fn run_expiration_job(
        &self,
        id: &str,
        expiration: Option<ExpirationJobConfig>,
    ) -> Result<()> {
        let config = expiration.ok_or_else(|| {
            MaxioError::InvalidArgument("expiration payload is required".to_string())
        })?;
        let keys = collect_expired_keys(self.object_layer.as_ref(), &config).await?;

        let total = keys.len();
        if total == 0 {
            self.update_progress(id, 100).await;
            return Ok(());
        }

        for (index, key) in keys.into_iter().enumerate() {
            self.object_layer.delete_object(&config.bucket, &key).await?;
            let progress = (((index + 1) * 100) / total) as u8;
            self.update_progress(id, progress).await;
        }

        Ok(())
    }

    async fn update_status(&self, id: &str, status: JobStatus) {
        if let Some(job) = self.jobs.write().await.get_mut(id) {
            job.status = status;
        }
    }

    async fn update_progress(&self, id: &str, progress: u8) {
        if let Some(job) = self.jobs.write().await.get_mut(id) {
            job.progress = progress.min(100);
        }
    }

    async fn set_error(&self, id: &str, message: String) {
        if let Some(job) = self.jobs.write().await.get_mut(id) {
            job.error = Some(message);
        }
    }

    async fn clear_error(&self, id: &str) {
        if let Some(job) = self.jobs.write().await.get_mut(id) {
            job.error = None;
        }
    }
}
