use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::errors::Result;

use super::stream::Stream;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HandlerID {
    Storage,
    Healing,
    Replication,
    Admin,
    Custom(u8),
}

impl HandlerID {
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Storage => 1,
            Self::Healing => 2,
            Self::Replication => 3,
            Self::Admin => 4,
            Self::Custom(value) => value,
        }
    }

    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Storage,
            2 => Self::Healing,
            3 => Self::Replication,
            4 => Self::Admin,
            _ => Self::Custom(value),
        }
    }
}

#[async_trait]
pub trait SingleHandler: Send + Sync {
    async fn handle(&self, payload: Vec<u8>) -> Result<Vec<u8>>;
}

#[async_trait]
pub trait StreamHandler: Send + Sync {
    async fn open(&self, stream: Stream, initial_payload: Vec<u8>) -> Result<()>;
}

#[derive(Clone)]
pub enum HandlerKind {
    Single(Arc<dyn SingleHandler>),
    Stream(Arc<dyn StreamHandler>),
}

#[derive(Clone, Default)]
pub struct HandlerRegistry {
    inner: Arc<RwLock<HashMap<(u8, Option<String>), HandlerKind>>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_single(
        &self,
        handler_id: HandlerID,
        subroute: Option<String>,
        handler: Arc<dyn SingleHandler>,
    ) {
        self.inner
            .write()
            .await
            .insert((handler_id.as_u8(), subroute), HandlerKind::Single(handler));
    }

    pub async fn register_stream(
        &self,
        handler_id: HandlerID,
        subroute: Option<String>,
        handler: Arc<dyn StreamHandler>,
    ) {
        self.inner
            .write()
            .await
            .insert((handler_id.as_u8(), subroute), HandlerKind::Stream(handler));
    }

    pub async fn get(&self, handler: u8, subroute: Option<&str>) -> Option<HandlerKind> {
        let guard = self.inner.read().await;
        if let Some(value) = guard.get(&(handler, subroute.map(ToOwned::to_owned))) {
            return Some(value.clone());
        }
        guard.get(&(handler, None)).cloned()
    }
}
