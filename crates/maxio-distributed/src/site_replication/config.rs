use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteReplicationConfig {
    pub enabled: bool,
    pub sites: Vec<PeerSite>,
    pub service_account: Option<ServiceAccountCredentials>,
}

impl Default for SiteReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sites: Vec::new(),
            service_account: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerSite {
    pub name: String,
    pub endpoint: String,
    pub deployment_id: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default)]
    pub sync_state: SiteSyncState,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SiteSyncState {
    pub last_iam_sync: Option<DateTime<Utc>>,
    pub last_bucket_sync: Option<DateTime<Utc>>,
    pub last_policy_sync: Option<DateTime<Utc>>,
    pub iam_sync_status: SyncStatus,
    pub bucket_sync_status: SyncStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStatus {
    #[default]
    Pending,
    InProgress,
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceAccountCredentials {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteReplicationStatus {
    pub enabled: bool,
    pub sites: Vec<SiteStatus>,
    pub max_bucket_count: usize,
    pub max_user_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteStatus {
    pub name: String,
    pub endpoint: String,
    pub deployment_id: String,
    pub online: bool,
    pub last_sync: Option<DateTime<Utc>>,
    pub bucket_count: usize,
    pub user_count: usize,
    pub policy_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteReplicationAdd {
    pub sites: Vec<PeerSiteInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerSiteInput {
    pub name: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteReplicationInfo {
    pub enabled: bool,
    pub name: String,
    pub sites: Vec<PeerSiteInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerSiteInfo {
    pub deployment_id: String,
    pub name: String,
    pub endpoint: String,
}

impl SiteReplicationConfig {
    pub fn add_site(&mut self, site: PeerSite) {
        if !self
            .sites
            .iter()
            .any(|s| s.deployment_id == site.deployment_id)
        {
            self.sites.push(site);
        }
        if self.sites.len() > 1 {
            self.enabled = true;
        }
    }

    pub fn remove_site(&mut self, deployment_id: &str) {
        self.sites.retain(|s| s.deployment_id != deployment_id);
        if self.sites.len() <= 1 {
            self.enabled = false;
        }
    }

    pub fn get_site(&self, deployment_id: &str) -> Option<&PeerSite> {
        self.sites.iter().find(|s| s.deployment_id == deployment_id)
    }

    pub fn peer_sites(&self, exclude_deployment_id: &str) -> Vec<&PeerSite> {
        self.sites
            .iter()
            .filter(|s| s.deployment_id != exclude_deployment_id)
            .collect()
    }
}
