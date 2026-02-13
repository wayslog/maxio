use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use chrono::{DateTime, Utc};
use maxio_common::error::{MaxioError, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};

pub const DEFAULT_MRF_CAPACITY: usize = 100_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartialOperationKind {
    PutObject,
    DeleteObject,
    CopyObject,
    CompleteMultipart,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialOperation {
    pub bucket: String,
    pub object: String,
    pub operation: PartialOperationKind,
    pub failed_disk_indices: Vec<usize>,
    pub attempt_count: u32,
    pub last_error: Option<String>,
    pub queued_at: DateTime<Utc>,
}

impl PartialOperation {
    pub fn new(
        bucket: impl Into<String>,
        object: impl Into<String>,
        operation: PartialOperationKind,
        failed_disk_indices: Vec<usize>,
        last_error: Option<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            object: object.into(),
            operation,
            failed_disk_indices,
            attempt_count: 0,
            last_error,
            queued_at: Utc::now(),
        }
    }
}

#[derive(Debug)]
pub struct MrfQueue {
    sender: mpsc::Sender<PartialOperation>,
    receiver: Mutex<mpsc::Receiver<PartialOperation>>,
    queued: AtomicUsize,
    dropped: AtomicU64,
}

impl MrfQueue {
    pub fn new(capacity: usize) -> Self {
        let bounded_capacity = if capacity == 0 {
            DEFAULT_MRF_CAPACITY
        } else {
            capacity
        };
        let (sender, receiver) = mpsc::channel(bounded_capacity);
        Self {
            sender,
            receiver: Mutex::new(receiver),
            queued: AtomicUsize::new(0),
            dropped: AtomicU64::new(0),
        }
    }

    pub fn with_default_capacity() -> Self {
        Self::new(DEFAULT_MRF_CAPACITY)
    }

    pub fn enqueue(&self, operation: PartialOperation) -> Result<()> {
        match self.sender.try_send(operation) {
            Ok(()) => {
                self.queued.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                Err(MaxioError::InternalError(
                    "MRF queue is full; operation dropped".to_string(),
                ))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(MaxioError::InternalError(
                "MRF queue is closed".to_string(),
            )),
        }
    }

    pub async fn dequeue(&self) -> Option<PartialOperation> {
        let mut receiver = self.receiver.lock().await;
        let item = receiver.recv().await;
        if item.is_some() {
            self.queued.fetch_sub(1, Ordering::Relaxed);
        }
        item
    }

    pub fn len(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Default for MrfQueue {
    fn default() -> Self {
        Self::with_default_capacity()
    }
}
