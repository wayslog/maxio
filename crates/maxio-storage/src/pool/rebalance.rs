use std::collections::HashSet;

use chrono::Utc;
use maxio_common::error::{MaxioError, Result};

use crate::pool::manager::PoolManager;
use crate::pool::types::{PoolStatus, RebalanceStatus};

pub async fn start_rebalance(manager: &PoolManager) -> Result<RebalanceStatus> {
    let mut state = manager.state.write().await;

    let active_pool_ids = state
        .pools
        .iter()
        .filter(|(_, info)| info.status == PoolStatus::Active)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();

    if active_pool_ids.len() < 2 {
        return Err(MaxioError::InvalidArgument(
            "rebalance requires at least two active pools".to_string(),
        ));
    }

    let total_capacity = active_pool_ids.iter().try_fold(0_u64, |acc, id| {
        let cap = state
            .pools
            .get(id)
            .ok_or_else(|| MaxioError::InternalError(format!("missing active pool: {id}")))?
            .capacity;
        Ok::<u64, MaxioError>(acc.saturating_add(cap))
    })?;

    if total_capacity == 0 {
        return Err(MaxioError::InvalidArgument(
            "rebalance cannot run on zero-capacity pools".to_string(),
        ));
    }

    let total_used = active_pool_ids.iter().try_fold(0_u64, |acc, id| {
        let used = state
            .pools
            .get(id)
            .ok_or_else(|| MaxioError::InternalError(format!("missing active pool: {id}")))?
            .used_space;
        Ok::<u64, MaxioError>(acc.saturating_add(used))
    })?;

    let started_at = Utc::now();
    let mut targets = Vec::with_capacity(active_pool_ids.len());
    let mut assigned = 0_u64;

    for (idx, id) in active_pool_ids.iter().enumerate() {
        let capacity = state
            .pools
            .get(id)
            .ok_or_else(|| MaxioError::InternalError(format!("missing active pool: {id}")))?
            .capacity;

        let desired = if idx + 1 == active_pool_ids.len() {
            total_used.saturating_sub(assigned)
        } else {
            let numerator = u128::from(total_used).saturating_mul(u128::from(capacity));
            let value = numerator / u128::from(total_capacity);
            let value_u64 = u64::try_from(value).map_err(|_| {
                MaxioError::InternalError("rebalance target conversion overflow".to_string())
            })?;
            assigned = assigned.saturating_add(value_u64);
            value_u64
        };

        targets.push((id.clone(), desired));
    }

    let mut surplus = Vec::new();
    let mut deficit = Vec::new();

    for (id, target_used) in &targets {
        let current = state
            .pools
            .get(id)
            .ok_or_else(|| MaxioError::InternalError(format!("missing active pool: {id}")))?
            .used_space;

        if current > *target_used {
            surplus.push((id.clone(), current - *target_used));
        } else if current < *target_used {
            deficit.push((id.clone(), *target_used - current));
        }
    }

    let total_to_move: u64 = surplus.iter().map(|(_, bytes)| *bytes).sum();
    let mut bytes_moved = 0_u64;
    let mut pools_touched = HashSet::new();
    let mut deficit_index = 0_usize;

    for (source_id, mut available) in surplus {
        while available > 0 {
            if deficit_index >= deficit.len() {
                break;
            }

            let (target_id, needed) = &mut deficit[deficit_index];
            if *needed == 0 {
                deficit_index = deficit_index.saturating_add(1);
                continue;
            }

            let moved = available.min(*needed);

            {
                let source = state.pools.get_mut(&source_id).ok_or_else(|| {
                    MaxioError::InternalError(format!("missing source pool during rebalance: {source_id}"))
                })?;
                source.used_space = source.used_space.saturating_sub(moved);
            }

            {
                let target = state.pools.get_mut(target_id).ok_or_else(|| {
                    MaxioError::InternalError(format!("missing target pool during rebalance: {target_id}"))
                })?;
                target.used_space = target.used_space.saturating_add(moved);
            }

            available = available.saturating_sub(moved);
            *needed = needed.saturating_sub(moved);
            bytes_moved = bytes_moved.saturating_add(moved);

            pools_touched.insert(source_id.clone());
            pools_touched.insert(target_id.clone());

            if *needed == 0 {
                deficit_index = deficit_index.saturating_add(1);
            }
        }
    }

    let progress = progress_percent(bytes_moved, total_to_move);
    let status = RebalanceStatus {
        progress,
        bytes_moved,
        pools_touched: pools_touched.len(),
        started_at,
    };
    state.last_rebalance = Some(status.clone());

    Ok(status)
}

fn progress_percent(done: u64, total: u64) -> u8 {
    if total == 0 {
        return 100;
    }

    let raw = done.saturating_mul(100) / total;
    let bounded = raw.min(100);
    bounded as u8
}
