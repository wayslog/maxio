use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Online,
    Offline,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub endpoint: String,
    pub status: NodeStatus,
    pub last_seen: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub nodes: Vec<String>,
    pub this_node: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterStatus {
    pub this_node: String,
    pub total_nodes: usize,
    pub online_nodes: usize,
    pub nodes: Vec<NodeInfo>,
}

impl ClusterConfig {
    pub fn single(this_node: String) -> Self {
        let this_node = normalize_endpoint(&this_node);
        Self {
            nodes: vec![this_node.clone()],
            this_node,
        }
    }

    pub fn from_env() -> Option<Self> {
        let nodes_var = std::env::var("MAXIO_DISTRIBUTED_NODES").ok();
        let this_node_var = std::env::var("MAXIO_THIS_NODE").ok();

        let this_node = this_node_var
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(normalize_endpoint)?;

        let mut nodes = nodes_var
            .as_deref()
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(normalize_endpoint)
            .collect::<Vec<_>>();

        nodes.push(this_node.clone());
        dedupe_preserve_order(&mut nodes);

        Some(Self { nodes, this_node })
    }
}

pub fn derive_node_id(endpoint: &str) -> String {
    let normalized = normalize_endpoint(endpoint);
    let parse_target = if normalized.contains("://") {
        normalized.clone()
    } else {
        format!("http://{normalized}")
    };

    if let Ok(url) = url::Url::parse(&parse_target)
        && let Some(host) = url.host_str()
    {
        if let Some(port) = url.port_or_known_default() {
            return format!("{host}:{port}");
        }
        return host.to_string();
    }

    normalized
}

pub fn normalize_endpoint(endpoint: &str) -> String {
    endpoint.trim().trim_end_matches('/').to_string()
}

fn dedupe_preserve_order(values: &mut Vec<String>) {
    let mut seen = HashSet::new();
    values.retain(|item| seen.insert(item.clone()));
}
