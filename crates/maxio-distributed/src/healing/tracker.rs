use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use maxio_common::error::{MaxioError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HealingTrackerSnapshot {
    pub items_healed: u64,
    pub bytes_done: u64,
    pub items_failed: u64,
    pub current_bucket: Option<String>,
    pub current_object: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
struct TrackerPosition {
    current_bucket: Option<String>,
    current_object: Option<String>,
}

#[derive(Debug)]
pub struct HealingTracker {
    items_healed: AtomicU64,
    bytes_done: AtomicU64,
    items_failed: AtomicU64,
    position: RwLock<TrackerPosition>,
    state_path: PathBuf,
}

impl HealingTracker {
    pub async fn load_or_new(state_path: impl AsRef<Path>) -> Result<Self> {
        let state_path = state_path.as_ref().to_path_buf();
        let snapshot = match tokio::fs::read(&state_path).await {
            Ok(bytes) => Some(
                serde_json::from_slice::<HealingTrackerSnapshot>(&bytes).map_err(|err| {
                    MaxioError::InternalError(format!(
                        "failed to parse healing tracker state {}: {err}",
                        state_path.display()
                    ))
                })?,
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                return Err(MaxioError::Io(err));
            }
        };

        Ok(Self::from_snapshot(state_path, snapshot))
    }

    fn from_snapshot(state_path: PathBuf, snapshot: Option<HealingTrackerSnapshot>) -> Self {
        let snapshot = match snapshot {
            Some(snapshot) => snapshot,
            None => HealingTrackerSnapshot {
                updated_at: Utc::now(),
                ..HealingTrackerSnapshot::default()
            },
        };

        Self {
            items_healed: AtomicU64::new(snapshot.items_healed),
            bytes_done: AtomicU64::new(snapshot.bytes_done),
            items_failed: AtomicU64::new(snapshot.items_failed),
            position: RwLock::new(TrackerPosition {
                current_bucket: snapshot.current_bucket,
                current_object: snapshot.current_object,
            }),
            state_path,
        }
    }

    pub fn set_position(&self, current_bucket: Option<String>, current_object: Option<String>) {
        let guard = self.position.write();
        match guard {
            Ok(mut state) => {
                state.current_bucket = current_bucket;
                state.current_object = current_object;
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.current_bucket = current_bucket;
                state.current_object = current_object;
            }
        }
    }

    pub fn mark_item_healed(&self, bytes_done: u64) {
        self.items_healed.fetch_add(1, Ordering::Relaxed);
        self.bytes_done.fetch_add(bytes_done, Ordering::Relaxed);
    }

    pub fn mark_item_failed(&self) {
        self.items_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> HealingTrackerSnapshot {
        let position = match self.position.read() {
            Ok(state) => state.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };

        HealingTrackerSnapshot {
            items_healed: self.items_healed.load(Ordering::Relaxed),
            bytes_done: self.bytes_done.load(Ordering::Relaxed),
            items_failed: self.items_failed.load(Ordering::Relaxed),
            current_bucket: position.current_bucket,
            current_object: position.current_object,
            updated_at: Utc::now(),
        }
    }

    pub async fn persist(&self) -> Result<()> {
        let snapshot = self.snapshot();
        let payload = serde_json::to_vec(&snapshot).map_err(|err| {
            MaxioError::InternalError(format!("failed to serialize healing tracker state: {err}"))
        })?;

        if let Some(parent) = self.state_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let tmp_path = self.state_path.with_extension("tmp");
        tokio::fs::write(&tmp_path, payload).await?;
        tokio::fs::rename(&tmp_path, &self.state_path).await?;
        Ok(())
    }

    pub fn start_persistence_loop(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            loop {
                ticker.tick().await;
                if self.persist().await.is_err() {
                    continue;
                }
            }
        })
    }
}
