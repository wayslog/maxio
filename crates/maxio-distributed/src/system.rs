use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crate::{
    discovery::NodeDiscovery,
    types::{ClusterConfig, ClusterStatus, derive_node_id, normalize_endpoint},
};

#[derive(Clone)]
pub struct DistributedSys {
    discovery: NodeDiscovery,
    this_node: String,
}

impl DistributedSys {
    pub async fn new(config: ClusterConfig) -> Self {
        let discovery = NodeDiscovery::new(config.clone()).await;
        discovery.start_health_checks().await;
        Self {
            discovery,
            this_node: config.this_node,
        }
    }

    pub fn is_distributed(&self) -> bool {
        self.discovery.is_distributed()
    }

    pub fn get_cluster_status(&self) -> ClusterStatus {
        let nodes = self.discovery.get_nodes();
        let online_nodes = nodes.iter().filter(|node| node.status.is_online()).count();

        ClusterStatus {
            this_node: self.this_node.clone(),
            total_nodes: nodes.len(),
            online_nodes,
            nodes,
        }
    }

    pub fn should_handle_request(&self, bucket: &str) -> bool {
        let mut candidates = self.discovery.get_online_nodes();
        if candidates.is_empty() {
            candidates = self.discovery.get_nodes();
        }

        if candidates.is_empty() {
            return true;
        }

        candidates.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        let selected = self.select_node(bucket, &candidates);
        let this_id = derive_node_id(&normalize_endpoint(&self.this_node));
        selected.id == this_id || selected.endpoint == self.this_node
    }

    fn select_node<'a>(
        &self,
        bucket: &str,
        nodes: &'a [crate::types::NodeInfo],
    ) -> &'a crate::types::NodeInfo {
        let mut hasher = DefaultHasher::new();
        bucket.hash(&mut hasher);
        let hash_value = hasher.finish() as usize;
        let index = hash_value % nodes.len();
        &nodes[index]
    }
}

trait NodeStatusExt {
    fn is_online(&self) -> bool;
}

impl NodeStatusExt for crate::types::NodeStatus {
    fn is_online(&self) -> bool {
        matches!(self, crate::types::NodeStatus::Online)
    }
}
