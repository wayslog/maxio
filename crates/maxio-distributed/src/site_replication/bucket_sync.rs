use std::sync::Arc;

use chrono::Utc;
use maxio_common::error::{MaxioError, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{error, info};

use super::config::{PeerSite, SiteReplicationConfig, SyncStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketSyncPayload {
    pub buckets: Vec<BucketMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketMetadata {
    pub name: String,
    pub versioning: Option<String>,
    pub lifecycle: Option<String>,
    pub replication: Option<String>,
    pub encryption: Option<String>,
    pub policy: Option<String>,
    pub tags: Option<String>,
    pub object_lock: Option<String>,
}

pub struct BucketSyncManager {
    config: Arc<RwLock<SiteReplicationConfig>>,
    local_deployment_id: String,
    client: Client,
}

impl BucketSyncManager {
    pub fn new(config: Arc<RwLock<SiteReplicationConfig>>, local_deployment_id: String) -> Self {
        Self {
            config,
            local_deployment_id,
            client: Client::new(),
        }
    }

    pub async fn sync_to_peers(&self, payload: &BucketSyncPayload) -> Result<()> {
        let config = self.config.read().await;
        if !config.enabled {
            return Ok(());
        }

        let peers = config.peer_sites(&self.local_deployment_id);
        for peer in peers {
            if let Err(e) = self.sync_to_peer(peer, payload).await {
                error!("Failed to sync buckets to peer {}: {}", peer.name, e);
            }
        }

        Ok(())
    }

    async fn sync_to_peer(&self, peer: &PeerSite, payload: &BucketSyncPayload) -> Result<()> {
        let url = format!(
            "{}/minio/admin/v3/site-replication/bucket-sync",
            peer.endpoint
        );

        let response = self
            .client
            .post(&url)
            .basic_auth(&peer.access_key, Some(&peer.secret_key))
            .json(payload)
            .send()
            .await
            .map_err(|e| MaxioError::InternalError(format!("Bucket sync request failed: {e}")))?;

        if !response.status().is_success() {
            return Err(MaxioError::InternalError(format!(
                "Bucket sync failed with status: {}",
                response.status()
            )));
        }

        info!("Bucket sync to peer {} completed", peer.name);
        Ok(())
    }

    pub async fn receive_sync(&self, payload: BucketSyncPayload) -> Result<()> {
        info!("Received bucket sync: {} buckets", payload.buckets.len());
        Ok(())
    }

    pub async fn sync_single_bucket(&self, bucket: &BucketMetadata) -> Result<()> {
        let payload = BucketSyncPayload {
            buckets: vec![bucket.clone()],
        };
        self.sync_to_peers(&payload).await
    }

    pub async fn update_sync_state(&self, deployment_id: &str, success: bool) {
        let mut config = self.config.write().await;
        if let Some(site) = config
            .sites
            .iter_mut()
            .find(|s| s.deployment_id == deployment_id)
        {
            site.sync_state.last_bucket_sync = Some(Utc::now());
            site.sync_state.bucket_sync_status = if success {
                SyncStatus::Success
            } else {
                SyncStatus::Failed
            };
        }
    }
}
