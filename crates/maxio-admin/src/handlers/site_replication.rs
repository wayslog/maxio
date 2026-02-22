use std::sync::Arc;

use axum::{
    Extension, Json,
    body::Body,
    http::{Response, StatusCode},
};
use maxio_distributed::site_replication::{
    PeerSite, SiteReplicationConfig, SiteReplicationStatus, SiteStatus,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct SiteAddRequest {
    pub sites: Vec<SiteInput>,
}

#[derive(Debug, Deserialize)]
pub struct SiteInput {
    pub name: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Serialize)]
pub struct SiteAddResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct SiteInfoResponse {
    pub enabled: bool,
    pub name: String,
    pub sites: Vec<SiteInfoEntry>,
}

#[derive(Debug, Serialize)]
pub struct SiteInfoEntry {
    pub deployment_id: String,
    pub name: String,
    pub endpoint: String,
}

pub async fn site_replication_add(
    Extension(config): Extension<Arc<RwLock<SiteReplicationConfig>>>,
    Json(request): Json<SiteAddRequest>,
) -> Response<Body> {
    let mut config = config.write().await;
    let site_count = request.sites.len();

    for site_input in request.sites {
        let site = PeerSite {
            name: site_input.name,
            endpoint: site_input.endpoint,
            deployment_id: Uuid::new_v4().to_string(),
            access_key: site_input.access_key,
            secret_key: site_input.secret_key,
            sync_state: Default::default(),
        };
        config.add_site(site);
    }

    let response = SiteAddResponse {
        success: true,
        message: format!("Added {} sites to replication", site_count),
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap()
}

pub async fn site_replication_remove(
    Extension(config): Extension<Arc<RwLock<SiteReplicationConfig>>>,
    Json(deployment_ids): Json<Vec<String>>,
) -> Response<Body> {
    let mut config = config.write().await;

    for deployment_id in &deployment_ids {
        config.remove_site(deployment_id);
    }

    let response = SiteAddResponse {
        success: true,
        message: format!("Removed {} sites from replication", deployment_ids.len()),
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap()
}

pub async fn site_replication_info(
    Extension(config): Extension<Arc<RwLock<SiteReplicationConfig>>>,
) -> Response<Body> {
    let config = config.read().await;

    let response = SiteInfoResponse {
        enabled: config.enabled,
        name: "site-replication".to_string(),
        sites: config
            .sites
            .iter()
            .map(|s| SiteInfoEntry {
                deployment_id: s.deployment_id.clone(),
                name: s.name.clone(),
                endpoint: s.endpoint.clone(),
            })
            .collect(),
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap()
}

pub async fn site_replication_status(
    Extension(config): Extension<Arc<RwLock<SiteReplicationConfig>>>,
) -> Response<Body> {
    let config = config.read().await;

    let status = SiteReplicationStatus {
        enabled: config.enabled,
        sites: config
            .sites
            .iter()
            .map(|s| SiteStatus {
                name: s.name.clone(),
                endpoint: s.endpoint.clone(),
                deployment_id: s.deployment_id.clone(),
                online: true,
                last_sync: s.sync_state.last_iam_sync,
                bucket_count: 0,
                user_count: 0,
                policy_count: 0,
            })
            .collect(),
        max_bucket_count: 0,
        max_user_count: 0,
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&status).unwrap()))
        .unwrap()
}
