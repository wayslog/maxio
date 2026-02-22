use std::time::Duration;


use reqwest::Client;
use tracing::debug;

use super::types::{PeerEndpoint, PeerRequest, PeerResponse};
use crate::errors::{GridError, Result};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const PEER_API_PATH: &str = "/minio/peer/v1/rpc";

pub struct PeerClient {
    client: Client,
    endpoints: Vec<PeerEndpoint>,
    timeout: Duration,
}

impl PeerClient {
    pub fn new(endpoints: Vec<PeerEndpoint>) -> Self {
        let client = Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .pool_max_idle_per_host(10)
            .build()
            .expect("failed to create HTTP client");

        Self {
            client,
            endpoints,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub async fn call(&self, endpoint: &PeerEndpoint, request: &PeerRequest) -> Result<PeerResponse> {
        let url = endpoint.url(PEER_API_PATH);
        debug!("Sending peer request to {}: {:?}", url, request);

        let response = self
            .client
            .post(&url)
            .timeout(self.timeout)
            .json(request)
            .send()
            .await
            .map_err(|e| GridError::ConnectionFailed(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(GridError::RequestFailed(format!(
                "peer request failed: {} - {}",
                status, body
            )));
        }

        let peer_response: PeerResponse = response
            .json()
            .await
            .map_err(|e| GridError::InvalidResponse(e.to_string()))?;

        Ok(peer_response)
    }

    pub async fn broadcast(&self, request: &PeerRequest) -> Vec<(String, Result<PeerResponse>)> {
        let mut results = Vec::with_capacity(self.endpoints.len());

        for endpoint in &self.endpoints {
            let result = self.call(endpoint, request).await;
            results.push((endpoint.address.clone(), result));
        }

        results
    }

    pub async fn ping(&self, endpoint: &PeerEndpoint) -> Result<()> {
        match self.call(endpoint, &PeerRequest::Ping).await? {
            PeerResponse::Pong { .. } => Ok(()),
            PeerResponse::Error { code, message } => {
                Err(GridError::RequestFailed(format!("{}: {}", code, message)))
            }
            _ => Err(GridError::InvalidResponse("unexpected response".to_string())),
        }
    }

    pub async fn get_server_info(&self, endpoint: &PeerEndpoint) -> Result<super::types::PeerInfo> {
        match self.call(endpoint, &PeerRequest::GetServerInfo).await? {
            PeerResponse::ServerInfo(info) => Ok(info),
            PeerResponse::Error { code, message } => {
                Err(GridError::RequestFailed(format!("{}: {}", code, message)))
            }
            _ => Err(GridError::InvalidResponse("unexpected response".to_string())),
        }
    }

    pub async fn heal_bucket(
        &self,
        endpoint: &PeerEndpoint,
        bucket: &str,
        prefix: Option<&str>,
        opts: super::types::HealOpts,
    ) -> Result<(bool, u64, Vec<String>)> {
        let request = PeerRequest::HealBucket {
            bucket: bucket.to_string(),
            prefix: prefix.map(|s| s.to_string()),
            opts,
        };

        match self.call(endpoint, &request).await? {
            PeerResponse::HealResult { success, items_healed, errors } => {
                Ok((success, items_healed, errors))
            }
            PeerResponse::Error { code, message } => {
                Err(GridError::RequestFailed(format!("{}: {}", code, message)))
            }
            _ => Err(GridError::InvalidResponse("unexpected response".to_string())),
        }
    }

    pub async fn sync_iam(&self, item: super::types::IAMSyncItem) -> Vec<(String, Result<()>)> {
        let request = PeerRequest::SyncIAM { item };
        let results = self.broadcast(&request).await;

        results
            .into_iter()
            .map(|(addr, result)| {
                let sync_result = result.and_then(|resp| match resp {
                    PeerResponse::SyncResult { success, error } => {
                        if success {
                            Ok(())
                        } else {
                            Err(GridError::RequestFailed(
                                error.unwrap_or_else(|| "sync failed".to_string()),
                            ))
                        }
                    }
                    PeerResponse::Error { code, message } => {
                        Err(GridError::RequestFailed(format!("{}: {}", code, message)))
                    }
                    _ => Err(GridError::InvalidResponse("unexpected response".to_string())),
                });
                (addr, sync_result)
            })
            .collect()
    }

    pub async fn sync_bucket(&self, bucket: &str, operation: super::types::BucketSyncOp) -> Vec<(String, Result<()>)> {
        let request = PeerRequest::SyncBucket {
            bucket: bucket.to_string(),
            operation,
        };
        let results = self.broadcast(&request).await;

        results
            .into_iter()
            .map(|(addr, result)| {
                let sync_result = result.and_then(|resp| match resp {
                    PeerResponse::SyncResult { success, error } => {
                        if success {
                            Ok(())
                        } else {
                            Err(GridError::RequestFailed(
                                error.unwrap_or_else(|| "sync failed".to_string()),
                            ))
                        }
                    }
                    PeerResponse::Error { code, message } => {
                        Err(GridError::RequestFailed(format!("{}: {}", code, message)))
                    }
                    _ => Err(GridError::InvalidResponse("unexpected response".to_string())),
                });
                (addr, sync_result)
            })
            .collect()
    }
}

impl Clone for PeerClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            endpoints: self.endpoints.clone(),
            timeout: self.timeout,
        }
    }
}
