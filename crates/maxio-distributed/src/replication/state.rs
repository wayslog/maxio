use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::types::{ReplicateObjectInfo, ReplicationStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusType {
    Pending,
    Completed,
    Failed,
    Replica,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectReplicationState {
    pub targets: HashMap<String, StatusType>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct ReplicationState {
    objects: Arc<RwLock<HashMap<String, ObjectReplicationState>>>,
}

impl ReplicationState {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn mark_targets_pending(&self, info: &ReplicateObjectInfo) {
        let key = object_key(&info.bucket, &info.object, info.version_id.as_deref());
        let mut targets = HashMap::new();
        for target in &info.targets {
            targets.insert(target.arn.clone(), StatusType::Pending);
        }

        let mut state = self.objects.write().await;
        state.insert(
            key,
            ObjectReplicationState {
                targets,
                updated_at: Utc::now(),
            },
        );
    }

    pub async fn set_target_status(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        target_arn: &str,
        status: StatusType,
    ) {
        let key = object_key(bucket, object, version_id);
        let mut state = self.objects.write().await;
        let entry = state.entry(key).or_insert_with(|| ObjectReplicationState {
            targets: HashMap::new(),
            updated_at: Utc::now(),
        });
        entry.targets.insert(target_arn.to_string(), status);
        entry.updated_at = Utc::now();
    }

    pub async fn get_object_state(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
    ) -> Option<ObjectReplicationState> {
        let key = object_key(bucket, object, version_id);
        let state = self.objects.read().await;
        state.get(&key).cloned()
    }

    pub async fn get_overall_status(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
    ) -> Option<ReplicationStatus> {
        let object_state = self.get_object_state(bucket, object, version_id).await?;
        if object_state
            .targets
            .values()
            .all(|status| matches!(status, StatusType::Completed | StatusType::Replica))
        {
            return Some(ReplicationStatus::Completed);
        }

        if object_state
            .targets
            .values()
            .any(|status| matches!(status, StatusType::Failed))
        {
            return Some(ReplicationStatus::Failed);
        }

        Some(ReplicationStatus::Pending)
    }

    pub async fn remove_object(&self, bucket: &str, object: &str, version_id: Option<&str>) {
        let key = object_key(bucket, object, version_id);
        let mut state = self.objects.write().await;
        state.remove(&key);
    }
}

fn object_key(bucket: &str, object: &str, version_id: Option<&str>) -> String {
    match version_id {
        Some(version_id) if !version_id.is_empty() => format!("{bucket}/{object}/{version_id}"),
        _ => format!("{bucket}/{object}"),
    }
}
