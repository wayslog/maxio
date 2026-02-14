use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use maxio_common::error::{MaxioError, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, mpsc};

use super::types::{ReplicateObjectInfo, ReplicationTarget};

pub const DEFAULT_MRF_CAPACITY: usize = 100_000;
pub const DEFAULT_MRF_RETRY_LIMIT: u32 = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MrfEntry {
    pub info: ReplicateObjectInfo,
    pub target: ReplicationTarget,
    pub last_error: Option<String>,
    pub queued_at: DateTime<Utc>,
}

impl MrfEntry {
    pub fn next_retry(mut self, last_error: String) -> Self {
        self.info.retry_count = self.info.retry_count.saturating_add(1);
        self.last_error = Some(last_error);
        self.queued_at = Utc::now();
        self
    }
}

#[derive(Debug)]
pub struct MrfQueue {
    sender: mpsc::Sender<MrfEntry>,
    receiver: Mutex<mpsc::Receiver<MrfEntry>>,
    pending: Arc<RwLock<VecDeque<MrfEntry>>>,
    persistence_path: PathBuf,
    retry_limit: u32,
}

impl MrfQueue {
    pub async fn load_or_new(
        persistence_dir: impl AsRef<Path>,
        capacity: usize,
        retry_limit: u32,
    ) -> Result<Self> {
        let capacity = if capacity == 0 {
            DEFAULT_MRF_CAPACITY
        } else {
            capacity
        };
        let retry_limit = if retry_limit == 0 {
            DEFAULT_MRF_RETRY_LIMIT
        } else {
            retry_limit
        };

        let persistence_path = persistence_dir.as_ref().join("mrf-queue.json");
        let mut persisted_entries = VecDeque::new();

        match tokio::fs::read(&persistence_path).await {
            Ok(bytes) => {
                persisted_entries =
                    serde_json::from_slice::<VecDeque<MrfEntry>>(&bytes).map_err(|err| {
                        MaxioError::InternalError(format!(
                            "failed to parse persisted MRF queue {}: {err}",
                            persistence_path.display()
                        ))
                    })?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(MaxioError::Io(err)),
        }

        let (sender, receiver) = mpsc::channel(capacity);
        let mut active_entries = VecDeque::new();
        for entry in persisted_entries {
            if sender.try_send(entry.clone()).is_ok() {
                active_entries.push_back(entry);
            } else {
                break;
            }
        }

        Ok(Self {
            sender,
            receiver: Mutex::new(receiver),
            pending: Arc::new(RwLock::new(active_entries)),
            persistence_path,
            retry_limit,
        })
    }

    pub async fn enqueue(&self, entry: MrfEntry) -> Result<()> {
        if !self.should_retry(&entry) {
            return Err(MaxioError::InternalError(format!(
                "mrf retry limit reached for {}/{}",
                entry.info.bucket, entry.info.object
            )));
        }

        {
            let mut pending = self.pending.write().await;
            pending.push_back(entry.clone());
        }

        match self.sender.try_send(entry) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                let mut pending = self.pending.write().await;
                let _ = pending.pop_back();
                Err(MaxioError::InternalError(
                    "MRF queue is full; entry dropped".to_string(),
                ))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                let mut pending = self.pending.write().await;
                let _ = pending.pop_back();
                Err(MaxioError::InternalError(
                    "MRF queue is closed; entry dropped".to_string(),
                ))
            }
        }
    }

    pub async fn dequeue(&self) -> Option<MrfEntry> {
        let mut receiver = self.receiver.lock().await;
        let item = receiver.recv().await;
        drop(receiver);

        if item.is_some() {
            let mut pending = self.pending.write().await;
            let _ = pending.pop_front();
        }
        item
    }

    pub async fn len(&self) -> usize {
        self.pending.read().await.len()
    }

    pub fn should_retry(&self, entry: &MrfEntry) -> bool {
        entry.info.retry_count < self.retry_limit
    }

    pub async fn persist(&self) -> Result<()> {
        let snapshot = self.pending.read().await.clone();
        let payload = serde_json::to_vec(&snapshot).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize MRF queue: {err}"))
        })?;

        if let Some(parent) = self.persistence_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let tmp_path = self.persistence_path.with_extension("tmp");
        tokio::fs::write(&tmp_path, payload).await?;
        tokio::fs::rename(&tmp_path, &self.persistence_path).await?;
        Ok(())
    }

    pub fn start_persistence_loop(
        self: Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if self.persist().await.is_err() {
                    continue;
                }
            }
        })
    }
}
