use std::{
    path::PathBuf,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use maxio_common::error::{MaxioError, Result};
use tokio::{
    sync::{RwLock, mpsc},
    task::JoinHandle,
};

use super::{
    mrf::{DEFAULT_MRF_CAPACITY, DEFAULT_MRF_RETRY_LIMIT, MrfEntry, MrfQueue},
    state::{ReplicationState, StatusType},
    types::ReplicateObjectInfo,
    worker::ReplicationWorker,
};

pub const DEFAULT_NORMAL_WORKERS: usize = 100;
pub const DEFAULT_LARGE_WORKERS: usize = 10;
pub const DEFAULT_MRF_WORKERS: usize = 4;
pub const DEFAULT_LARGE_OBJECT_THRESHOLD: u64 = 128 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ReplicationPoolConfig {
    pub normal_workers: usize,
    pub large_workers: usize,
    pub mrf_workers: usize,
    pub large_object_threshold: u64,
    pub worker_queue_capacity: usize,
    pub mrf_queue_capacity: usize,
    pub mrf_retry_limit: u32,
    pub mrf_persistence_interval: Duration,
    pub mrf_persistence_dir: PathBuf,
}

impl Default for ReplicationPoolConfig {
    fn default() -> Self {
        Self {
            normal_workers: DEFAULT_NORMAL_WORKERS,
            large_workers: DEFAULT_LARGE_WORKERS,
            mrf_workers: DEFAULT_MRF_WORKERS,
            large_object_threshold: DEFAULT_LARGE_OBJECT_THRESHOLD,
            worker_queue_capacity: 4_096,
            mrf_queue_capacity: DEFAULT_MRF_CAPACITY,
            mrf_retry_limit: DEFAULT_MRF_RETRY_LIMIT,
            mrf_persistence_interval: Duration::from_secs(30),
            mrf_persistence_dir: PathBuf::from(".minio.sys/replication/mrf"),
        }
    }
}

#[derive(Debug)]
struct StandardWorker {
    sender: mpsc::Sender<ReplicateObjectInfo>,
    handle: JoinHandle<()>,
}

#[derive(Debug)]
struct StandardTier {
    name: &'static str,
    workers: Arc<RwLock<Vec<StandardWorker>>>,
    next_worker: AtomicUsize,
    queue_capacity: usize,
}

impl StandardTier {
    fn new(name: &'static str, queue_capacity: usize) -> Self {
        Self {
            name,
            workers: Arc::new(RwLock::new(Vec::new())),
            next_worker: AtomicUsize::new(0),
            queue_capacity,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationPool {
    config: Arc<RwLock<ReplicationPoolConfig>>,
    state: Arc<ReplicationState>,
    normal_tier: Arc<StandardTier>,
    large_tier: Arc<StandardTier>,
    mrf_queue: Arc<MrfQueue>,
    mrf_workers: Arc<RwLock<Vec<JoinHandle<()>>>>,
    _mrf_persist_handle: Arc<JoinHandle<()>>,
}

impl ReplicationPool {
    pub async fn new(config: ReplicationPoolConfig) -> Result<Self> {
        let worker_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|err| {
                MaxioError::InternalError(format!("failed to create replication client: {err}"))
            })?;

        let worker = Arc::new(ReplicationWorker::new(worker_client));
        let state = Arc::new(ReplicationState::new());
        let mrf_queue = Arc::new(
            MrfQueue::load_or_new(
                &config.mrf_persistence_dir,
                config.mrf_queue_capacity,
                config.mrf_retry_limit,
            )
            .await?,
        );

        let mrf_persist_handle = mrf_queue
            .clone()
            .start_persistence_loop(config.mrf_persistence_interval);

        let pool = Self {
            config: Arc::new(RwLock::new(config.clone())),
            state,
            normal_tier: Arc::new(StandardTier::new("normal", config.worker_queue_capacity)),
            large_tier: Arc::new(StandardTier::new("large", config.worker_queue_capacity)),
            mrf_queue,
            mrf_workers: Arc::new(RwLock::new(Vec::new())),
            _mrf_persist_handle: Arc::new(mrf_persist_handle),
        };

        pool.resize_standard_tier(&pool.normal_tier, config.normal_workers, worker.clone())
            .await;
        pool.resize_standard_tier(&pool.large_tier, config.large_workers, worker.clone())
            .await;
        pool.resize_mrf_workers(config.mrf_workers, worker).await;

        Ok(pool)
    }

    pub fn state(&self) -> Arc<ReplicationState> {
        self.state.clone()
    }

    pub fn mrf_queue(&self) -> Arc<MrfQueue> {
        self.mrf_queue.clone()
    }

    pub async fn submit(&self, info: ReplicateObjectInfo) -> Result<()> {
        self.state.mark_targets_pending(&info).await;

        let threshold = self.config.read().await.large_object_threshold;
        let tier = if info.size >= threshold {
            &self.large_tier
        } else {
            &self.normal_tier
        };

        self.dispatch_to_tier(tier, info).await
    }

    pub async fn resize(&self, normal_workers: usize, large_workers: usize, mrf_workers: usize) {
        let worker_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let worker = Arc::new(ReplicationWorker::new(worker_client));

        self.resize_standard_tier(&self.normal_tier, normal_workers, worker.clone())
            .await;
        self.resize_standard_tier(&self.large_tier, large_workers, worker.clone())
            .await;
        self.resize_mrf_workers(mrf_workers, worker).await;

        let mut config = self.config.write().await;
        config.normal_workers = normal_workers;
        config.large_workers = large_workers;
        config.mrf_workers = mrf_workers;
    }

    async fn dispatch_to_tier(&self, tier: &StandardTier, info: ReplicateObjectInfo) -> Result<()> {
        let sender = {
            let workers = tier.workers.read().await;
            if workers.is_empty() {
                return Err(MaxioError::InternalError(format!(
                    "replication {} tier has no workers",
                    tier.name
                )));
            }
            let idx = tier.next_worker.fetch_add(1, Ordering::Relaxed) % workers.len();
            workers[idx].sender.clone()
        };

        sender.send(info).await.map_err(|_| {
            MaxioError::InternalError(format!("replication {} tier channel closed", tier.name))
        })
    }

    async fn resize_standard_tier(
        &self,
        tier: &Arc<StandardTier>,
        desired: usize,
        worker_impl: Arc<ReplicationWorker>,
    ) {
        let current = tier.workers.read().await.len();
        if desired > current {
            for _ in current..desired {
                let (sender, mut receiver) = mpsc::channel(tier.queue_capacity);
                let state = self.state.clone();
                let mrf_queue = self.mrf_queue.clone();
                let worker = worker_impl.clone();

                let handle = tokio::spawn(async move {
                    while let Some(info) = receiver.recv().await {
                        replicate_to_targets(&worker, &state, &mrf_queue, info).await;
                    }
                });

                let mut workers = tier.workers.write().await;
                workers.push(StandardWorker { sender, handle });
            }
            return;
        }

        if desired < current {
            let mut workers = tier.workers.write().await;
            while workers.len() > desired {
                if let Some(worker) = workers.pop() {
                    worker.handle.abort();
                }
            }
        }
    }

    async fn resize_mrf_workers(&self, desired: usize, worker_impl: Arc<ReplicationWorker>) {
        let current = self.mrf_workers.read().await.len();
        if desired > current {
            for _ in current..desired {
                let state = self.state.clone();
                let mrf_queue = self.mrf_queue.clone();
                let worker = worker_impl.clone();

                let handle = tokio::spawn(async move {
                    loop {
                        let Some(entry) = mrf_queue.dequeue().await else {
                            break;
                        };

                        let info = entry.info.clone();
                        let target = entry.target.clone();
                        let result = worker.replicate_object(&info, &target).await;
                        match result {
                            Ok(()) => {
                                state
                                    .set_target_status(
                                        &info.bucket,
                                        &info.object,
                                        info.version_id.as_deref(),
                                        &target.arn,
                                        StatusType::Completed,
                                    )
                                    .await;
                            }
                            Err(err) => {
                                let err_msg = err.to_string();
                                state
                                    .set_target_status(
                                        &info.bucket,
                                        &info.object,
                                        info.version_id.as_deref(),
                                        &target.arn,
                                        StatusType::Failed,
                                    )
                                    .await;

                                let retry_entry = entry.next_retry(err_msg);
                                if mrf_queue.should_retry(&retry_entry) {
                                    let _ = mrf_queue.enqueue(retry_entry).await;
                                }
                            }
                        }
                    }
                });

                let mut workers = self.mrf_workers.write().await;
                workers.push(handle);
            }
            return;
        }

        if desired < current {
            let mut workers = self.mrf_workers.write().await;
            while workers.len() > desired {
                if let Some(handle) = workers.pop() {
                    handle.abort();
                }
            }
        }
    }
}

async fn replicate_to_targets(
    worker: &ReplicationWorker,
    state: &ReplicationState,
    mrf_queue: &MrfQueue,
    info: ReplicateObjectInfo,
) {
    for target in &info.targets {
        let result = worker.replicate_object(&info, target).await;
        match result {
            Ok(()) => {
                state
                    .set_target_status(
                        &info.bucket,
                        &info.object,
                        info.version_id.as_deref(),
                        &target.arn,
                        StatusType::Completed,
                    )
                    .await;
            }
            Err(err) => {
                state
                    .set_target_status(
                        &info.bucket,
                        &info.object,
                        info.version_id.as_deref(),
                        &target.arn,
                        StatusType::Failed,
                    )
                    .await;

                let retry_info = ReplicateObjectInfo {
                    retry_count: info.retry_count.saturating_add(1),
                    targets: vec![target.clone()],
                    ..info.clone()
                };

                let retry_entry = MrfEntry {
                    info: retry_info,
                    target: target.clone(),
                    last_error: Some(err.to_string()),
                    queued_at: chrono::Utc::now(),
                };
                let _ = mrf_queue.enqueue(retry_entry).await;
            }
        }
    }
}
