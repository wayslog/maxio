use async_trait::async_trait;
use maxio_common::error::Result;

use super::lock_args::LockArgs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockResult {
    Success,
    NotAcquired,
    LockNotFound,
    Failed,
}

#[async_trait]
pub trait NetLocker: Send + Sync {
    async fn lock(&self, args: &LockArgs) -> Result<LockResult>;
    async fn rlock(&self, args: &LockArgs) -> Result<LockResult>;
    async fn unlock(&self, args: &LockArgs) -> Result<LockResult>;
    async fn runlock(&self, args: &LockArgs) -> Result<LockResult>;
    async fn refresh(&self, args: &LockArgs) -> Result<LockResult>;
    async fn force_unlock(&self, args: &LockArgs) -> Result<LockResult>;
}
