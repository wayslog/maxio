use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use chrono::Utc;
use tracing::{info, warn};

use crate::types::{ClusterConfig, NodeInfo, NodeStatus, derive_node_id, normalize_endpoint};

#[derive(Clone)]
pub struct NodeDiscovery {
    config: ClusterConfig,
    nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
    client: reqwest::Client,
}

impl NodeDiscovery {
    pub async fn new(config: ClusterConfig) -> Self {
        let mut initial_nodes = HashMap::new();
        let now = Utc::now();
        for endpoint in &config.nodes {
            let normalized_endpoint = normalize_endpoint(endpoint);
            let id = derive_node_id(&normalized_endpoint);
            let status = if normalized_endpoint == config.this_node {
                NodeStatus::Online
            } else {
                NodeStatus::Unknown
            };
            initial_nodes.insert(
                id.clone(),
                NodeInfo {
                    id,
                    endpoint: normalized_endpoint,
                    status,
                    last_seen: now,
                },
            );
        }

        Self {
            config,
            nodes: Arc::new(RwLock::new(initial_nodes)),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    pub async fn start_health_checks(&self) {
        self.run_health_check_once().await;

        let discovery = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                discovery.run_health_check_once().await;
            }
        });
    }

    pub fn get_nodes(&self) -> Vec<NodeInfo> {
        let nodes = match self.nodes.read() {
            Ok(guard) => guard,
            Err(_) => return Vec::new(),
        };
        let mut result = nodes.values().cloned().collect::<Vec<_>>();
        result.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        result
    }

    pub fn get_online_nodes(&self) -> Vec<NodeInfo> {
        self.get_nodes()
            .into_iter()
            .filter(|node| node.status == NodeStatus::Online)
            .collect()
    }

    pub fn this_node(&self) -> &str {
        &self.config.this_node
    }

    pub fn is_distributed(&self) -> bool {
        self.config.nodes.len() > 1
    }

    async fn run_health_check_once(&self) {
        let nodes_snapshot = {
            let guard = match self.nodes.read() {
                Ok(guard) => guard,
                Err(_) => return,
            };
            guard.values().cloned().collect::<Vec<_>>()
        };

        let mut updated_nodes = Vec::with_capacity(nodes_snapshot.len());
        let now = Utc::now();

        for mut node in nodes_snapshot {
            if node.endpoint == self.config.this_node {
                node.status = NodeStatus::Online;
                node.last_seen = now;
                updated_nodes.push(node);
                continue;
            }

            let health_url = format!("{}/minio/health/live", ensure_http_scheme(&node.endpoint));
            let status = match self.client.get(health_url).send().await {
                Ok(response) if response.status().is_success() => NodeStatus::Online,
                Ok(_) => NodeStatus::Offline,
                Err(_) => NodeStatus::Offline,
            };

            if status == NodeStatus::Online {
                node.last_seen = now;
            }
            node.status = status;
            updated_nodes.push(node);
        }

        let mut nodes = match self.nodes.write() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        for updated in updated_nodes {
            if let Some(previous) = nodes.get(&updated.id)
                && previous.status != updated.status
            {
                if updated.status == NodeStatus::Online {
                    info!(node = %updated.endpoint, "node status changed to online");
                } else {
                    warn!(node = %updated.endpoint, "node status changed to offline");
                }
            }
            nodes.insert(updated.id.clone(), updated);
        }
    }
}

fn ensure_http_scheme(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}
