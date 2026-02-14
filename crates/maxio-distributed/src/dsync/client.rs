use std::{sync::Arc, time::Duration};

use futures::{StreamExt, stream::FuturesUnordered};
use tokio::time::{sleep, timeout};

use super::{
    lock_args::LockArgs,
    locker::{LockResult, NetLocker},
};

pub const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);
pub const REFRESH_CALL_TIMEOUT: Duration = Duration::from_secs(5);
pub const UNLOCK_CALL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct AcquireOutcome {
    pub granted: Vec<bool>,
    pub locks_acquired: usize,
    pub failures: usize,
    pub quorum: usize,
    pub tolerance: usize,
    pub succeeded: bool,
}

#[derive(Debug, Clone)]
pub struct RefreshOutcome {
    pub refreshed: usize,
    pub failures: usize,
    pub lock_not_found: usize,
    pub quorum_lost: bool,
}

#[derive(Clone)]
pub struct DsyncClient {
    lockers: Vec<Arc<dyn NetLocker>>,
}

impl DsyncClient {
    pub fn new(lockers: Vec<Arc<dyn NetLocker>>) -> Self {
        Self { lockers }
    }

    pub fn total_nodes(&self) -> usize {
        self.lockers.len()
    }

    pub fn tolerance(&self) -> usize {
        self.lockers.len() / 2
    }

    pub fn quorum(&self, write_lock: bool) -> usize {
        let total = self.lockers.len();
        if total == 0 {
            return 0;
        }

        let tolerance = total / 2;
        let mut quorum = total.saturating_sub(tolerance);

        if write_lock && quorum == tolerance {
            quorum = quorum.saturating_add(1);
        }

        quorum.min(total)
    }

    pub async fn lock(&self, args: &LockArgs) -> AcquireOutcome {
        self.acquire(args, false).await
    }

    pub async fn rlock(&self, args: &LockArgs) -> AcquireOutcome {
        self.acquire(args, true).await
    }

    async fn acquire(&self, args: &LockArgs, read_lock: bool) -> AcquireOutcome {
        let total = self.lockers.len();
        let tolerance = self.tolerance();
        let quorum = args.quorum.clamp(1, total.max(1));
        let mut granted = vec![false; total];

        if total == 0 {
            return AcquireOutcome {
                granted,
                locks_acquired: 0,
                failures: 1,
                quorum,
                tolerance,
                succeeded: false,
            };
        }

        let mut pending = FuturesUnordered::new();
        for (index, locker) in self.lockers.iter().enumerate() {
            let locker = Arc::clone(locker);
            let call_args = args.clone();
            pending.push(async move {
                let call = if read_lock {
                    locker.rlock(&call_args)
                } else {
                    locker.lock(&call_args)
                };

                let outcome = match timeout(ACQUIRE_TIMEOUT, call).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => LockResult::Failed,
                    Err(_) => LockResult::Failed,
                };
                (index, outcome)
            });
        }

        let mut locks_acquired = 0usize;
        let mut failures = 0usize;
        let mut remaining = total;

        while let Some((index, outcome)) = pending.next().await {
            remaining = remaining.saturating_sub(1);

            match outcome {
                LockResult::Success => {
                    granted[index] = true;
                    locks_acquired = locks_acquired.saturating_add(1);
                }
                LockResult::NotAcquired | LockResult::LockNotFound | LockResult::Failed => {
                    failures = failures.saturating_add(1);
                }
            }

            if locks_acquired >= quorum && failures <= tolerance {
                break;
            }

            if locks_acquired.saturating_add(remaining) < quorum || failures > tolerance {
                break;
            }
        }

        let succeeded = locks_acquired >= quorum && failures <= tolerance;

        AcquireOutcome {
            granted,
            locks_acquired,
            failures,
            quorum,
            tolerance,
            succeeded,
        }
    }

    pub async fn refresh(&self, args: &LockArgs, granted: &[bool]) -> RefreshOutcome {
        let total = self.lockers.len();
        let quorum = args.quorum.clamp(1, total.max(1));
        let mut pending = FuturesUnordered::new();

        for (index, locker) in self.lockers.iter().enumerate() {
            if !granted.get(index).copied().unwrap_or(false) {
                continue;
            }

            let locker = Arc::clone(locker);
            let call_args = args.clone();
            pending.push(async move {
                let outcome = match timeout(REFRESH_CALL_TIMEOUT, locker.refresh(&call_args)).await
                {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => LockResult::Failed,
                    Err(_) => LockResult::Failed,
                };
                outcome
            });
        }

        let mut refreshed = 0usize;
        let mut failures = 0usize;
        let mut lock_not_found = 0usize;

        while let Some(outcome) = pending.next().await {
            match outcome {
                LockResult::Success => refreshed = refreshed.saturating_add(1),
                LockResult::LockNotFound => {
                    failures = failures.saturating_add(1);
                    lock_not_found = lock_not_found.saturating_add(1);
                }
                LockResult::NotAcquired | LockResult::Failed => {
                    failures = failures.saturating_add(1);
                }
            }
        }

        let quorum_lost = lock_not_found > total.saturating_sub(quorum);

        RefreshOutcome {
            refreshed,
            failures,
            lock_not_found,
            quorum_lost,
        }
    }

    pub async fn force_unlock(&self, args: &LockArgs) {
        let mut pending = FuturesUnordered::new();

        for locker in &self.lockers {
            let locker = Arc::clone(locker);
            let call_args = args.clone();
            pending.push(async move {
                let _ = timeout(UNLOCK_CALL_TIMEOUT, locker.force_unlock(&call_args)).await;
            });
        }

        while pending.next().await.is_some() {}
    }

    pub async fn unlock_with_retry(&self, args: &LockArgs, granted: Vec<bool>, read_lock: bool) {
        let mut pending = FuturesUnordered::new();

        for (index, locker) in self.lockers.iter().enumerate() {
            if !granted.get(index).copied().unwrap_or(false) {
                continue;
            }

            let locker = Arc::clone(locker);
            let call_args = args.clone();
            pending.push(async move {
                loop {
                    let call = if read_lock {
                        locker.runlock(&call_args)
                    } else {
                        locker.unlock(&call_args)
                    };

                    let done = match timeout(UNLOCK_CALL_TIMEOUT, call).await {
                        Ok(Ok(LockResult::Success | LockResult::LockNotFound)) => true,
                        Ok(Ok(LockResult::NotAcquired | LockResult::Failed)) => false,
                        Ok(Err(_)) => false,
                        Err(_) => false,
                    };

                    if done {
                        break;
                    }

                    sleep(Duration::from_millis(500)).await;
                }
            });
        }

        while pending.next().await.is_some() {}
    }
}
