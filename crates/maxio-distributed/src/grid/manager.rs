use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::errors::{GridError, Result};

use super::{
    connection::{Connection, ConnectionState},
    handler::HandlerRegistry,
    message::{Flags, Message, MuxId},
};

#[derive(Clone)]
pub struct Manager {
    handlers: HandlerRegistry,
    connections: Arc<RwLock<HashMap<String, Arc<Connection>>>>,
}

impl Manager {
    pub fn new(handlers: HandlerRegistry) -> Self {
        Self {
            handlers,
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn ensure_connection(&self, node_addr: &str) -> Result<Arc<Connection>> {
        if let Some(existing) = self.connections.read().await.get(node_addr).cloned() {
            return Ok(existing);
        }

        let connection = Arc::new(Connection::new(
            node_addr.to_string(),
            self.handlers.clone(),
        ));
        connection.start().await?;

        self.connections
            .write()
            .await
            .insert(node_addr.to_string(), connection.clone());
        Ok(connection)
    }

    pub async fn remove_connection(&self, node_addr: &str) {
        self.connections.write().await.remove(node_addr);
    }

    pub async fn get_connection(&self, node_addr: &str) -> Option<Arc<Connection>> {
        self.connections.read().await.get(node_addr).cloned()
    }

    pub async fn list_states(&self) -> HashMap<String, ConnectionState> {
        let snapshot = self.connections.read().await.clone();
        let mut states = HashMap::with_capacity(snapshot.len());

        for (addr, connection) in snapshot {
            states.insert(addr, connection.state().await);
        }

        states
    }

    pub async fn request(
        &self,
        node_addr: &str,
        mux_id: MuxId,
        handler: u8,
        payload: Vec<u8>,
        flags: Flags,
    ) -> Result<Message> {
        let connection = self
            .get_connection(node_addr)
            .await
            .ok_or_else(|| GridError::NodeNotConnected(node_addr.to_string()))?;
        connection.request(mux_id, handler, payload, flags).await
    }
}
