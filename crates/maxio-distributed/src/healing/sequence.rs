use std::sync::Arc;
use std::sync::RwLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::healing::mrf::{MrfQueue, PartialOperation};
use crate::healing::tracker::HealingTracker;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum HealSequenceStatus {
    Idle,
    Running,
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealSequenceState {
    pub session_id: String,
    pub status: HealSequenceStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub current_bucket: Option<String>,
    pub current_object: Option<String>,
    pub healed_items: u64,
    pub failed_items: u64,
}

#[derive(Debug)]
pub struct HealSequence {
    tracker: Arc<HealingTracker>,
    mrf: Arc<MrfQueue>,
    state: RwLock<HealSequenceState>,
}

impl HealSequence {
    pub fn new(tracker: Arc<HealingTracker>, mrf: Arc<MrfQueue>) -> Self {
        let started_at = Utc::now();
        let session_id = format!("heal-{}", started_at.timestamp_millis());

        Self {
            tracker,
            mrf,
            state: RwLock::new(HealSequenceState {
                session_id,
                status: HealSequenceStatus::Idle,
                started_at,
                finished_at: None,
                current_bucket: None,
                current_object: None,
                healed_items: 0,
                failed_items: 0,
            }),
        }
    }

    pub fn start_bucket(&self, bucket: impl Into<String>) {
        let bucket = bucket.into();
        self.tracker.set_position(Some(bucket.clone()), None);

        let guard = self.state.write();
        match guard {
            Ok(mut state) => {
                state.status = HealSequenceStatus::Running;
                state.current_bucket = Some(bucket);
                state.current_object = None;
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.status = HealSequenceStatus::Running;
                state.current_bucket = Some(bucket);
                state.current_object = None;
            }
        }
    }

    pub fn start_object(&self, bucket: impl Into<String>, object: impl Into<String>) {
        let bucket = bucket.into();
        let object = object.into();
        self.tracker
            .set_position(Some(bucket.clone()), Some(object.clone()));

        let guard = self.state.write();
        match guard {
            Ok(mut state) => {
                state.status = HealSequenceStatus::Running;
                state.current_bucket = Some(bucket);
                state.current_object = Some(object);
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.status = HealSequenceStatus::Running;
                state.current_bucket = Some(bucket);
                state.current_object = Some(object);
            }
        }
    }

    pub fn mark_object_healed(&self, bytes_done: u64) {
        self.tracker.mark_item_healed(bytes_done);
        let guard = self.state.write();
        match guard {
            Ok(mut state) => {
                state.healed_items += 1;
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.healed_items += 1;
            }
        }
    }

    pub fn mark_object_failed(&self) {
        self.tracker.mark_item_failed();
        let guard = self.state.write();
        match guard {
            Ok(mut state) => {
                state.failed_items += 1;
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.failed_items += 1;
            }
        }
    }

    pub fn complete(&self) {
        let guard = self.state.write();
        match guard {
            Ok(mut state) => {
                state.status = HealSequenceStatus::Completed;
                state.finished_at = Some(Utc::now());
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.status = HealSequenceStatus::Completed;
                state.finished_at = Some(Utc::now());
            }
        }
    }

    pub fn cancel(&self) {
        let guard = self.state.write();
        match guard {
            Ok(mut state) => {
                state.status = HealSequenceStatus::Cancelled;
                state.finished_at = Some(Utc::now());
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.status = HealSequenceStatus::Cancelled;
                state.finished_at = Some(Utc::now());
            }
        }
    }

    pub fn snapshot(&self) -> HealSequenceState {
        let guard = self.state.read();
        match guard {
            Ok(state) => state.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    pub async fn next_partial_operation(&self) -> Option<PartialOperation> {
        self.mrf.dequeue().await
    }
}
