pub mod batch;
pub mod handlers;
pub mod metrics;
pub mod middleware;
pub mod router;
pub mod types;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Instant,
};

use chrono::Utc;
use maxio_auth::credentials::CredentialProvider;
use maxio_common::error::{MaxioError, Result};
use maxio_distributed::DistributedSys;
use maxio_iam::{IAMSys, Policy};
use maxio_storage::traits::ObjectLayer;

use crate::batch::scheduler::JobScheduler;

#[derive(Clone)]
pub struct AdminSys {
    iam: Arc<IAMSys>,
    credentials: Arc<dyn CredentialProvider>,
    object_layer: Arc<dyn ObjectLayer>,
    distributed: Arc<DistributedSys>,
    endpoint: String,
    region: String,
    started_at: Instant,
    boot_time: chrono::DateTime<Utc>,
    config: Arc<RwLock<HashMap<String, String>>>,
    policies: Arc<RwLock<HashMap<String, Policy>>>,
    job_scheduler: JobScheduler,
}

impl AdminSys {
    pub fn new(
        iam: Arc<IAMSys>,
        credentials: Arc<dyn CredentialProvider>,
        object_layer: Arc<dyn ObjectLayer>,
        distributed: Arc<DistributedSys>,
        endpoint: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        let mut policies = HashMap::new();
        policies.insert("readwrite".to_string(), builtin_readwrite_policy());
        policies.insert("readonly".to_string(), builtin_readonly_policy());
        let job_scheduler = JobScheduler::new(Arc::clone(&object_layer));

        Self {
            iam,
            credentials,
            object_layer,
            distributed,
            endpoint: endpoint.into(),
            region: region.into(),
            started_at: Instant::now(),
            boot_time: Utc::now(),
            config: Arc::new(RwLock::new(HashMap::new())),
            policies: Arc::new(RwLock::new(policies)),
            job_scheduler,
        }
    }

    pub fn iam(&self) -> Arc<IAMSys> {
        Arc::clone(&self.iam)
    }

    pub fn credentials(&self) -> Arc<dyn CredentialProvider> {
        Arc::clone(&self.credentials)
    }

    pub fn object_layer(&self) -> Arc<dyn ObjectLayer> {
        Arc::clone(&self.object_layer)
    }

    pub fn distributed(&self) -> Arc<DistributedSys> {
        Arc::clone(&self.distributed)
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    pub fn boot_time(&self) -> chrono::DateTime<Utc> {
        self.boot_time
    }

    pub fn get_config_map(&self) -> Result<HashMap<String, String>> {
        Ok(self.config_read()?.clone())
    }

    pub fn set_config_map(&self, values: HashMap<String, String>) -> Result<()> {
        *self.config_write()? = values;
        Ok(())
    }

    pub fn get_config_value(&self, key: &str) -> Result<Option<String>> {
        Ok(self.config_read()?.get(key).cloned())
    }

    pub fn set_config_value(&self, key: &str, value: String) -> Result<()> {
        validate_config_key(key)?;
        self.config_write()?.insert(key.to_string(), value);
        Ok(())
    }

    pub fn delete_config_value(&self, key: &str) -> Result<()> {
        self.config_write()?.remove(key);
        Ok(())
    }

    pub fn remember_policy(&self, policy: Policy) -> Result<()> {
        self.policies_write()?.insert(policy.name.clone(), policy);
        Ok(())
    }

    pub fn remove_remembered_policy(&self, name: &str) -> Result<()> {
        self.policies_write()?.remove(name);
        Ok(())
    }

    pub fn list_remembered_policies(&self) -> Result<Vec<Policy>> {
        let mut policies: Vec<Policy> = self.policies_read()?.values().cloned().collect();
        policies.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(policies)
    }

    pub fn job_scheduler(&self) -> JobScheduler {
        self.job_scheduler.clone()
    }

    fn config_read(&self) -> Result<std::sync::RwLockReadGuard<'_, HashMap<String, String>>> {
        self.config
            .read()
            .map_err(|_| MaxioError::InternalError("admin config lock poisoned".to_string()))
    }

    fn config_write(&self) -> Result<std::sync::RwLockWriteGuard<'_, HashMap<String, String>>> {
        self.config
            .write()
            .map_err(|_| MaxioError::InternalError("admin config lock poisoned".to_string()))
    }

    fn policies_read(&self) -> Result<std::sync::RwLockReadGuard<'_, HashMap<String, Policy>>> {
        self.policies
            .read()
            .map_err(|_| MaxioError::InternalError("admin policies lock poisoned".to_string()))
    }

    fn policies_write(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, HashMap<String, Policy>>> {
        self.policies
            .write()
            .map_err(|_| MaxioError::InternalError("admin policies lock poisoned".to_string()))
    }
}

fn validate_config_key(key: &str) -> Result<()> {
    if key.split_once(':').is_some_and(|(subsystem, name)| {
        !subsystem.is_empty() && !name.is_empty() && !name.contains(':')
    }) {
        return Ok(());
    }

    Err(MaxioError::InvalidArgument(
        "config key must use subsystem:key format".to_string(),
    ))
}

fn builtin_readwrite_policy() -> Policy {
    use maxio_iam::{Effect, PolicyStatement};

    Policy {
        name: "readwrite".to_string(),
        version: "2012-10-17".to_string(),
        statements: vec![PolicyStatement {
            effect: Effect::Allow,
            actions: vec!["s3:*".to_string(), "admin:*".to_string()],
            resources: vec!["*".to_string()],
        }],
    }
}

fn builtin_readonly_policy() -> Policy {
    use maxio_iam::{Effect, PolicyStatement};

    Policy {
        name: "readonly".to_string(),
        version: "2012-10-17".to_string(),
        statements: vec![PolicyStatement {
            effect: Effect::Allow,
            actions: vec!["s3:Get*".to_string(), "s3:List*".to_string()],
            resources: vec!["*".to_string()],
        }],
    }
}
