use std::sync::Arc;

use axum::{Json, Router, routing::post};
use chrono::Utc;
use tracing::{debug, info};

use super::types::{PeerInfo, PeerRequest, PeerResponse};

pub type PeerHandler = Arc<dyn Fn(PeerRequest) -> PeerResponse + Send + Sync>;

pub struct PeerServer {
    node_id: String,
    version: String,
    start_time: chrono::DateTime<Utc>,
}

impl PeerServer {
    pub fn new(node_id: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            version: version.into(),
            start_time: Utc::now(),
        }
    }

    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/minio/peer/v1/rpc", post(Self::handle_rpc))
            .with_state(self)
    }

    async fn handle_rpc(
        axum::extract::State(server): axum::extract::State<Arc<PeerServer>>,
        Json(request): Json<PeerRequest>,
    ) -> Json<PeerResponse> {
        debug!("Received peer request: {:?}", request);
        let response = server.process_request(request).await;
        Json(response)
    }

    async fn process_request(&self, request: PeerRequest) -> PeerResponse {
        match request {
            PeerRequest::Ping => PeerResponse::Pong {
                timestamp: Utc::now(),
            },

            PeerRequest::GetServerInfo => PeerResponse::ServerInfo(PeerInfo {
                endpoint: String::new(), // Will be filled by caller
                node_id: self.node_id.clone(),
                version: self.version.clone(),
                uptime: (Utc::now() - self.start_time).num_seconds() as u64,
            }),

            PeerRequest::HealBucket { bucket, prefix, opts: _ } => {
                info!("Heal request for bucket: {}, prefix: {:?}", bucket, prefix);
                // TODO: Implement actual healing logic
                PeerResponse::HealResult {
                    success: true,
                    items_healed: 0,
                    errors: vec![],
                }
            }

            PeerRequest::GetBgHealStatus => {
                // TODO: Get actual background heal status
                PeerResponse::BgHealStatus {
                    active: false,
                    last_activity: None,
                }
            }

            PeerRequest::ReplicateObject { bucket, object, version_id } => {
                debug!("Replicate object: {}/{} (version: {:?})", bucket, object, version_id);
                // TODO: Implement actual replication
                PeerResponse::ReplicateResult {
                    success: true,
                    error: None,
                }
            }

            PeerRequest::DeleteObject { bucket, object, version_id } => {
                debug!("Delete object: {}/{} (version: {:?})", bucket, object, version_id);
                // TODO: Implement actual deletion
                PeerResponse::DeleteResult {
                    success: true,
                    error: None,
                }
            }

            PeerRequest::SyncIAM { item } => {
                debug!("Sync IAM item: {:?}", item);
                // TODO: Implement actual IAM sync
                PeerResponse::SyncResult {
                    success: true,
                    error: None,
                }
            }

            PeerRequest::SyncBucket { bucket, operation } => {
                debug!("Sync bucket: {} with operation: {:?}", bucket, operation);
                // TODO: Implement actual bucket sync
                PeerResponse::SyncResult {
                    success: true,
                    error: None,
                }
            }
        }
    }
}
