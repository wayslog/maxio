use std::{
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use maxio_common::error::{MaxioError, Result};
use tracing::warn;

use super::{client::DsyncClient, lock_args::LockArgs};

static UID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct DRWMutex {
    client: Arc<DsyncClient>,
    owner: String,
    source: String,
    resources: Vec<String>,
    write_locks: Arc<RwLock<Vec<bool>>>,
    read_locks: Arc<RwLock<Vec<bool>>>,
    write_args: Arc<RwLock<Option<LockArgs>>>,
    read_args: Arc<RwLock<Option<LockArgs>>>,
    write_refresh_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    read_refresh_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl DRWMutex {
    pub fn new(
        client: Arc<DsyncClient>,
        resources: Vec<String>,
        owner: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        let nodes = client.total_nodes();
        Self {
            client,
            owner: owner.into(),
            source: source.into(),
            resources,
            write_locks: Arc::new(RwLock::new(vec![false; nodes])),
            read_locks: Arc::new(RwLock::new(vec![false; nodes])),
            write_args: Arc::new(RwLock::new(None)),
            read_args: Arc::new(RwLock::new(None)),
            write_refresh_task: Arc::new(RwLock::new(None)),
            read_refresh_task: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn lock(&self) -> Result<bool> {
        self.acquire(false).await
    }

    pub async fn rlock(&self) -> Result<bool> {
        self.acquire(true).await
    }

    pub async fn unlock(&self) -> Result<()> {
        self.release(false).await
    }

    pub async fn runlock(&self) -> Result<()> {
        self.release(true).await
    }

    #[allow(non_snake_case)]
    pub async fn Lock(&self) -> Result<bool> {
        self.lock().await
    }

    #[allow(non_snake_case)]
    pub async fn RLock(&self) -> Result<bool> {
        self.rlock().await
    }

    #[allow(non_snake_case)]
    pub async fn Unlock(&self) -> Result<()> {
        self.unlock().await
    }

    #[allow(non_snake_case)]
    pub async fn RUnlock(&self) -> Result<()> {
        self.runlock().await
    }

    async fn acquire(&self, read_lock: bool) -> Result<bool> {
        let quorum = self.client.quorum(!read_lock);
        if quorum == 0 {
            return Err(MaxioError::InvalidArgument(
                "cannot acquire dsync lock with zero nodes".to_string(),
            ));
        }

        let args = LockArgs::new(
            next_uid(),
            self.resources.clone(),
            self.owner.clone(),
            self.source.clone(),
            quorum,
        );

        let outcome = if read_lock {
            self.client.rlock(&args).await
        } else {
            self.client.lock(&args).await
        };

        if !outcome.succeeded {
            return Ok(false);
        }

        if read_lock {
            self.abort_refresh_task(&self.read_refresh_task)?;
            self.store_locks(&self.read_locks, outcome.granted.clone())?;
            self.store_args(&self.read_args, Some(args.clone()))?;
            let task = self.spawn_refresh_task(
                true,
                args,
                Arc::clone(&self.read_locks),
                Arc::clone(&self.read_args),
            );
            self.store_refresh_task(&self.read_refresh_task, Some(task))?;
            return Ok(true);
        }

        self.abort_refresh_task(&self.write_refresh_task)?;
        self.store_locks(&self.write_locks, outcome.granted.clone())?;
        self.store_args(&self.write_args, Some(args.clone()))?;
        let task = self.spawn_refresh_task(
            false,
            args,
            Arc::clone(&self.write_locks),
            Arc::clone(&self.write_args),
        );
        self.store_refresh_task(&self.write_refresh_task, Some(task))?;

        Ok(true)
    }

    async fn release(&self, read_lock: bool) -> Result<()> {
        let (args_lock, granted_lock, refresh_lock) = if read_lock {
            (&self.read_args, &self.read_locks, &self.read_refresh_task)
        } else {
            (&self.write_args, &self.write_locks, &self.write_refresh_task)
        };

        self.abort_refresh_task(refresh_lock)?;

        let args = self.take_args(args_lock)?;
        let granted = self.take_granted(granted_lock)?;

        if let Some(args) = args {
            self.client.unlock_with_retry(&args, granted, read_lock).await;
        }

        Ok(())
    }

    fn spawn_refresh_task(
        &self,
        read_lock: bool,
        args: LockArgs,
        granted: Arc<RwLock<Vec<bool>>>,
        args_store: Arc<RwLock<Option<LockArgs>>>,
    ) -> tokio::task::JoinHandle<()> {
        let client = Arc::clone(&self.client);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(10));
            loop {
                ticker.tick().await;

                let grants = match granted.read() {
                    Ok(state) => state.clone(),
                    Err(poisoned) => poisoned.into_inner().clone(),
                };

                if grants.iter().all(|value| !value) {
                    break;
                }

                let refreshed = client.refresh(&args, &grants).await;
                if refreshed.quorum_lost {
                    warn!(uid = %args.uid, read_lock, "dsync refresh lost quorum; force unlocking");
                    client.force_unlock(&args).await;

                    match granted.write() {
                        Ok(mut state) => {
                            for value in state.iter_mut() {
                                *value = false;
                            }
                        }
                        Err(poisoned) => {
                            let mut state = poisoned.into_inner();
                            for value in state.iter_mut() {
                                *value = false;
                            }
                        }
                    }

                    match args_store.write() {
                        Ok(mut state) => {
                            *state = None;
                        }
                        Err(poisoned) => {
                            let mut state = poisoned.into_inner();
                            *state = None;
                        }
                    }

                    break;
                }
            }
        })
    }

    fn abort_refresh_task(&self, task_lock: &RwLock<Option<tokio::task::JoinHandle<()>>>) -> Result<()> {
        let mut guard = task_lock
            .write()
            .map_err(|_| MaxioError::InternalError("dsync refresh lock poisoned".to_string()))?;

        if let Some(handle) = guard.take() {
            handle.abort();
        }

        Ok(())
    }

    fn store_refresh_task(
        &self,
        task_lock: &RwLock<Option<tokio::task::JoinHandle<()>>>,
        task: Option<tokio::task::JoinHandle<()>>,
    ) -> Result<()> {
        let mut guard = task_lock
            .write()
            .map_err(|_| MaxioError::InternalError("dsync refresh lock poisoned".to_string()))?;
        *guard = task;
        Ok(())
    }

    fn store_locks(&self, lock: &RwLock<Vec<bool>>, values: Vec<bool>) -> Result<()> {
        let mut guard = lock
            .write()
            .map_err(|_| MaxioError::InternalError("dsync lock state poisoned".to_string()))?;
        *guard = values;
        Ok(())
    }

    fn store_args(&self, lock: &RwLock<Option<LockArgs>>, value: Option<LockArgs>) -> Result<()> {
        let mut guard = lock
            .write()
            .map_err(|_| MaxioError::InternalError("dsync args state poisoned".to_string()))?;
        *guard = value;
        Ok(())
    }

    fn take_args(&self, lock: &RwLock<Option<LockArgs>>) -> Result<Option<LockArgs>> {
        let mut guard = lock
            .write()
            .map_err(|_| MaxioError::InternalError("dsync args state poisoned".to_string()))?;
        Ok(guard.take())
    }

    fn take_granted(&self, lock: &RwLock<Vec<bool>>) -> Result<Vec<bool>> {
        let mut guard = lock
            .write()
            .map_err(|_| MaxioError::InternalError("dsync lock state poisoned".to_string()))?;
        let mut granted = vec![false; guard.len()];
        std::mem::swap(&mut granted, &mut *guard);
        Ok(granted)
    }
}

fn next_uid() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let counter = UID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos}-{counter}")
}
