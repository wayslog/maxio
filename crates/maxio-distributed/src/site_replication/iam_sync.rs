use std::sync::Arc;

use chrono::Utc;
use maxio_common::error::{MaxioError, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{error, info};

use super::config::{PeerSite, SiteReplicationConfig, SyncStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IAMSyncPayload {
    pub users: Vec<UserInfo>,
    pub policies: Vec<PolicyInfo>,
    pub user_policy_mappings: Vec<UserPolicyMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub access_key: String,
    pub secret_key: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyInfo {
    pub name: String,
    pub policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPolicyMapping {
    pub user: String,
    pub policy: String,
}

pub struct IAMSyncManager {
    config: Arc<RwLock<SiteReplicationConfig>>,
    local_deployment_id: String,
    client: Client,
}

impl IAMSyncManager {
    pub fn new(config: Arc<RwLock<SiteReplicationConfig>>, local_deployment_id: String) -> Self {
        Self {
            config,
            local_deployment_id,
            client: Client::new(),
        }
    }

    pub async fn sync_to_peers(&self, payload: &IAMSyncPayload) -> Result<()> {
        let config = self.config.read().await;
        if !config.enabled {
            return Ok(());
        }

        let peers = config.peer_sites(&self.local_deployment_id);
        for peer in peers {
            if let Err(e) = self.sync_to_peer(peer, payload).await {
                error!("Failed to sync IAM to peer {}: {}", peer.name, e);
            }
        }

        Ok(())
    }

    async fn sync_to_peer(&self, peer: &PeerSite, payload: &IAMSyncPayload) -> Result<()> {
        let url = format!("{}/minio/admin/v3/site-replication/iam-sync", peer.endpoint);
        
        let response = self
            .client
            .post(&url)
            .basic_auth(&peer.access_key, Some(&peer.secret_key))
            .json(payload)
            .send()
            .await
            .map_err(|e| MaxioError::InternalError(format!("IAM sync request failed: {e}")))?;

        if !response.status().is_success() {
            return Err(MaxioError::InternalError(format!(
                "IAM sync failed with status: {}",
                response.status()
            )));
        }

        info!("IAM sync to peer {} completed", peer.name);
        Ok(())
    }

    pub async fn receive_sync(&self, payload: IAMSyncPayload) -> Result<()> {
        info!(
            "Received IAM sync: {} users, {} policies",
            payload.users.len(),
            payload.policies.len()
        );
        Ok(())
    }

    pub async fn update_sync_state(&self, deployment_id: &str, success: bool) {
        let mut config = self.config.write().await;
        if let Some(site) = config.sites.iter_mut().find(|s| s.deployment_id == deployment_id) {
            site.sync_state.last_iam_sync = Some(Utc::now());
            site.sync_state.iam_sync_status = if success {
                SyncStatus::Success
            } else {
                SyncStatus::Failed
            };
        }
    }
}
