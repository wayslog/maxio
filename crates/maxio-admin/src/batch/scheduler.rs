use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use maxio_common::error::{MaxioError, Result};
use maxio_storage::traits::ObjectLayer;
use tokio::{sync::RwLock, task::JoinHandle};
use uuid::Uuid;

use crate::batch::{
    expiration::{ExpirationJobConfig, collect_expired_keys},
    job::BatchJob,
    key_rotation::{KeyRotationJobConfig, collect_keys_for_rotation, rotate_object_key},
    replication::{ReplicationJobConfig, collect_keys_for_replication, replicate_object},
    types::{JobStatus, JobType},
};

#[derive(Debug, Clone)]
pub enum BatchJobConfig {
    Expiration(ExpirationJobConfig),
    Replication(ReplicationJobConfig),
    KeyRotation(KeyRotationJobConfig),
}

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
        let config = match job_type {
            JobType::Expiration => {
                let cfg = expiration.ok_or_else(|| {
                    MaxioError::InvalidArgument(
                        "expiration payload is required for expiration jobs".to_string(),
                    )
                })?;
                cfg.validate()?;
                BatchJobConfig::Expiration(cfg)
            }
            JobType::Replication => {
                return Err(MaxioError::InvalidArgument(
                    "use submit_replication_job for replication jobs".to_string(),
                ));
            }
            JobType::KeyRotation => {
                return Err(MaxioError::InvalidArgument(
                    "use submit_key_rotation_job for key rotation jobs".to_string(),
                ));
            }
        };

        self.submit_job_internal(job_type, config).await
    }

    pub async fn submit_replication_job(
        &self,
        config: ReplicationJobConfig,
    ) -> Result<BatchJob> {
        config.validate()?;
        self.submit_job_internal(JobType::Replication, BatchJobConfig::Replication(config))
            .await
    }

    pub async fn submit_key_rotation_job(
        &self,
        config: KeyRotationJobConfig,
    ) -> Result<BatchJob> {
        config.validate()?;
        self.submit_job_internal(JobType::KeyRotation, BatchJobConfig::KeyRotation(config))
            .await
    }

    async fn submit_job_internal(
        &self,
        job_type: JobType,
        config: BatchJobConfig,
    ) -> Result<BatchJob> {
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
            scheduler.run_job(id, config).await;
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
        let job = jobs
            .get_mut(id)
            .ok_or_else(|| MaxioError::InvalidArgument(format!("batch job not found: {id}")))?;
        if job.status == JobStatus::Completed || job.status == JobStatus::Failed {
            return Ok(job.clone());
        }

        job.status = JobStatus::Failed;
        job.error = Some("job cancelled".to_string());
        Ok(job.clone())
    }

    async fn run_job(&self, id: String, config: BatchJobConfig) {
        self.update_status(&id, JobStatus::Running).await;

        let result = match config {
            BatchJobConfig::Expiration(cfg) => self.run_expiration_job(&id, cfg).await,
            BatchJobConfig::Replication(cfg) => self.run_replication_job(&id, cfg).await,
            BatchJobConfig::KeyRotation(cfg) => self.run_key_rotation_job(&id, cfg).await,
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

    async fn run_expiration_job(&self, id: &str, config: ExpirationJobConfig) -> Result<()> {
        let keys = collect_expired_keys(self.object_layer.as_ref(), &config).await?;

        let total = keys.len();
        if total == 0 {
            self.update_progress(id, 100).await;
            return Ok(());
        }

        for (index, key) in keys.into_iter().enumerate() {
            self.object_layer
                .delete_object(&config.bucket, &key)
                .await?;
            let progress = (((index + 1) * 100) / total) as u8;
            self.update_progress(id, progress).await;
        }

        Ok(())
    }

    async fn run_replication_job(&self, id: &str, config: ReplicationJobConfig) -> Result<()> {
        let keys = collect_keys_for_replication(self.object_layer.as_ref(), &config).await?;

        let total = keys.len();
        if total == 0 {
            self.update_progress(id, 100).await;
            return Ok(());
        }

        for (index, key) in keys.into_iter().enumerate() {
            replicate_object(self.object_layer.as_ref(), &config, &key).await?;
            let progress = (((index + 1) * 100) / total) as u8;
            self.update_progress(id, progress).await;
        }

        Ok(())
    }

    async fn run_key_rotation_job(&self, id: &str, config: KeyRotationJobConfig) -> Result<()> {
        let keys = collect_keys_for_rotation(self.object_layer.as_ref(), &config).await?;

        let total = keys.len();
        if total == 0 {
            self.update_progress(id, 100).await;
            return Ok(());
        }

        for (index, key) in keys.into_iter().enumerate() {
            rotate_object_key(self.object_layer.as_ref(), &config, &key).await?;
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
