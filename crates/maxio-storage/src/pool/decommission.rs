use std::cmp::Reverse;

use chrono::Utc;
use maxio_common::error::{MaxioError, Result};

use crate::pool::manager::PoolManager;
use crate::pool::types::{DecommissionStatus, PoolStatus};

const MIGRATION_OBJECT_CHUNK_BYTES: u64 = 64 * 1024 * 1024;

pub async fn start_decommission(manager: &PoolManager, pool_id: &str) -> Result<DecommissionStatus> {
    let mut state = manager.state.write().await;

    let source = state
        .pools
        .get(pool_id)
        .cloned()
        .ok_or_else(|| MaxioError::InvalidArgument(format!("pool not found: {pool_id}")))?;

    if source.status == PoolStatus::Decommissioned {
        return Err(MaxioError::InvalidArgument(format!(
            "pool is already decommissioned: {pool_id}"
        )));
    }

    if let Some(status) = state.decommission_status.get(pool_id) {
        if status.progress < 100 {
            return Err(MaxioError::InvalidArgument(format!(
                "decommission already in progress for pool: {pool_id}"
            )));
        }
    }

    let started_at = Utc::now();

    let mut target_pool_ids = state
        .pools
        .iter()
        .filter(|(id, info)| {
            id.as_str() != pool_id
                && info.status == PoolStatus::Active
                && info.capacity.saturating_sub(info.used_space) > 0
        })
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();

    target_pool_ids.sort_by_key(|id| {
        let free = state
            .pools
            .get(id)
            .map(|info| info.capacity.saturating_sub(info.used_space))
            .unwrap_or(0);
        Reverse(free)
    });

    let total_bytes = source.used_space;

    if total_bytes > 0 && target_pool_ids.is_empty() {
        return Err(MaxioError::InvalidArgument(format!(
            "cannot decommission pool {pool_id}: no active target pools"
        )));
    }

    let total_free_capacity = target_pool_ids.iter().try_fold(0_u64, |acc, id| {
        let free = state
            .pools
            .get(id)
            .ok_or_else(|| MaxioError::InternalError(format!("target pool disappeared: {id}")))?
            .capacity
            .saturating_sub(
                state
                    .pools
                    .get(id)
                    .ok_or_else(|| {
                        MaxioError::InternalError(format!("target pool disappeared: {id}"))
                    })?
                    .used_space,
            );
        Ok::<u64, MaxioError>(acc.saturating_add(free))
    })?;

    if total_free_capacity < total_bytes {
        return Err(MaxioError::InvalidArgument(format!(
            "insufficient cluster capacity to decommission {pool_id}: need {total_bytes} bytes, free {total_free_capacity} bytes"
        )));
    }

    if let Some(pool) = state.pools.get_mut(pool_id) {
        pool.status = PoolStatus::Decommissioning;
    }

    let mut bytes_moved = 0_u64;
    let mut objects_moved = 0_u64;
    let mut remaining = total_bytes;

    for target_id in target_pool_ids {
        if remaining == 0 {
            break;
        }

        let Some(target) = state.pools.get_mut(&target_id) else {
            return Err(MaxioError::InternalError(format!(
                "target pool disappeared during migration: {target_id}"
            )));
        };

        let free = target.capacity.saturating_sub(target.used_space);
        if free == 0 {
            continue;
        }

        let moved = remaining.min(free);
        target.used_space = target.used_space.saturating_add(moved);
        remaining = remaining.saturating_sub(moved);
        bytes_moved = bytes_moved.saturating_add(moved);

        let moved_objects = moved.div_ceil(MIGRATION_OBJECT_CHUNK_BYTES);
        objects_moved = objects_moved.saturating_add(moved_objects);

        let progress = progress_percent(bytes_moved, total_bytes);
        let status = DecommissionStatus {
            pool_id: pool_id.to_string(),
            progress,
            objects_moved,
            bytes_moved,
            started_at,
        };
        state
            .decommission_status
            .insert(pool_id.to_string(), status.clone());
    }

    if remaining != 0 {
        return Err(MaxioError::InternalError(format!(
            "decommission left unmigrated bytes for pool {pool_id}: {remaining}"
        )));
    }

    if let Some(source_mut) = state.pools.get_mut(pool_id) {
        source_mut.used_space = 0;
        source_mut.status = PoolStatus::Decommissioned;
    }

    let completed = DecommissionStatus {
        pool_id: pool_id.to_string(),
        progress: 100,
        objects_moved,
        bytes_moved,
        started_at,
    };
    state
        .decommission_status
        .insert(pool_id.to_string(), completed.clone());

    Ok(completed)
}

fn progress_percent(done: u64, total: u64) -> u8 {
    if total == 0 {
        return 100;
    }

    let raw = done.saturating_mul(100) / total;
    let bounded = raw.min(100);
    bounded as u8
}
