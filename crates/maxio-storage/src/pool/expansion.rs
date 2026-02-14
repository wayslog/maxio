use std::collections::HashSet;

use maxio_common::error::{MaxioError, Result};

use crate::pool::manager::PoolManager;
use crate::pool::types::{PoolInfo, PoolStatus};

pub async fn add_pool(
    manager: &PoolManager,
    pool_id: String,
    endpoints: Vec<String>,
    capacity: u64,
) -> Result<PoolInfo> {
    validate_pool_id(&pool_id)?;
    validate_capacity(capacity)?;
    validate_endpoints(&endpoints)?;

    {
        let state = manager.state.read().await;
        if state.pools.contains_key(&pool_id) {
            return Err(MaxioError::InvalidArgument(format!(
                "pool already exists: {pool_id}"
            )));
        }
        register_new_pool_endpoints(&state.pools.values().collect::<Vec<_>>(), &endpoints)?;
    }

    initialize_pool_format(&pool_id, &endpoints).await?;
    join_cluster().await;

    let mut state = manager.state.write().await;
    if state.pools.contains_key(&pool_id) {
        return Err(MaxioError::InvalidArgument(format!(
            "pool already exists: {pool_id}"
        )));
    }
    register_new_pool_endpoints(&state.pools.values().collect::<Vec<_>>(), &endpoints)?;

    state.cluster_initialized = true;
    let info = PoolInfo {
        id: pool_id.clone(),
        endpoints,
        status: PoolStatus::Active,
        capacity,
        used_space: 0,
    };
    state.pools.insert(pool_id, info.clone());

    Ok(info)
}

fn validate_pool_id(pool_id: &str) -> Result<()> {
    if pool_id.trim().is_empty() {
        return Err(MaxioError::InvalidArgument(
            "pool id cannot be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_capacity(capacity: u64) -> Result<()> {
    if capacity == 0 {
        return Err(MaxioError::InvalidArgument(
            "pool capacity must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_endpoints(endpoints: &[String]) -> Result<()> {
    if endpoints.is_empty() {
        return Err(MaxioError::InvalidArgument(
            "pool must include at least one endpoint".to_string(),
        ));
    }

    let mut seen = HashSet::new();
    for endpoint in endpoints {
        let normalized = endpoint.trim();
        if normalized.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "pool endpoints cannot be empty".to_string(),
            ));
        }
        if !seen.insert(normalized.to_string()) {
            return Err(MaxioError::InvalidArgument(format!(
                "duplicate endpoint in pool definition: {normalized}"
            )));
        }
    }

    Ok(())
}

fn register_new_pool_endpoints(existing_pools: &[&PoolInfo], endpoints: &[String]) -> Result<()> {
    let mut existing = HashSet::new();
    for pool in existing_pools {
        for endpoint in &pool.endpoints {
            existing.insert(endpoint.as_str());
        }
    }

    for endpoint in endpoints {
        if existing.contains(endpoint.as_str()) {
            return Err(MaxioError::InvalidArgument(format!(
                "endpoint is already registered in another pool: {endpoint}"
            )));
        }
    }

    Ok(())
}

async fn initialize_pool_format(pool_id: &str, endpoints: &[String]) -> Result<()> {
    for endpoint in endpoints {
        if endpoint.trim().is_empty() {
            return Err(MaxioError::InvalidArgument(format!(
                "invalid endpoint for pool {pool_id}"
            )));
        }
    }

    tokio::task::yield_now().await;
    Ok(())
}

async fn join_cluster() {
    tokio::task::yield_now().await;
}
