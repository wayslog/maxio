use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use maxio_common::error::{MaxioError, Result};
use tokio::sync::RwLock;

use crate::pool::decommission;
use crate::pool::expansion;
use crate::pool::rebalance;
use crate::pool::types::{DecommissionStatus, PoolInfo, PoolStatus, RebalanceStatus};

#[derive(Debug, Default)]
pub(crate) struct PoolState {
    pub(crate) pools: BTreeMap<String, PoolInfo>,
    pub(crate) decommission_status: HashMap<String, DecommissionStatus>,
    pub(crate) last_rebalance: Option<RebalanceStatus>,
    pub(crate) cluster_initialized: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PoolManager {
    pub(crate) state: Arc<RwLock<PoolState>>,
}

impl PoolManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn list_pools(&self) -> Vec<PoolInfo> {
        let state = self.state.read().await;
        state.pools.values().cloned().collect()
    }

    pub async fn add_pool(
        &self,
        id: impl Into<String>,
        endpoints: Vec<String>,
        capacity: u64,
    ) -> Result<PoolInfo> {
        expansion::add_pool(self, id.into(), endpoints, capacity).await
    }

    pub async fn remove_pool(&self, pool_id: &str) -> Result<()> {
        let mut state = self.state.write().await;
        let pool = state
            .pools
            .get(pool_id)
            .ok_or_else(|| MaxioError::InvalidArgument(format!("pool not found: {pool_id}")))?;

        if pool.status != PoolStatus::Decommissioned {
            return Err(MaxioError::InvalidArgument(format!(
                "pool {pool_id} must be decommissioned before removal"
            )));
        }

        if pool.used_space != 0 {
            return Err(MaxioError::InvalidArgument(format!(
                "pool {pool_id} still has used space: {} bytes",
                pool.used_space
            )));
        }

        state.pools.remove(pool_id);
        state.decommission_status.remove(pool_id);
        Ok(())
    }

    pub async fn get_pool_info(&self, pool_id: &str) -> Result<PoolInfo> {
        let state = self.state.read().await;
        state
            .pools
            .get(pool_id)
            .cloned()
            .ok_or_else(|| MaxioError::InvalidArgument(format!("pool not found: {pool_id}")))
    }

    pub async fn start_decommission(&self, pool_id: &str) -> Result<DecommissionStatus> {
        decommission::start_decommission(self, pool_id).await
    }

    pub async fn start_rebalance(&self) -> Result<RebalanceStatus> {
        rebalance::start_rebalance(self).await
    }

    pub async fn get_decommission_status(&self, pool_id: &str) -> Option<DecommissionStatus> {
        let state = self.state.read().await;
        state.decommission_status.get(pool_id).cloned()
    }

    pub async fn get_last_rebalance_status(&self) -> Option<RebalanceStatus> {
        let state = self.state.read().await;
        state.last_rebalance.clone()
    }
}
