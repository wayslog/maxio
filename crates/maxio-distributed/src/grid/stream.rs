use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::{Mutex, mpsc};

use crate::errors::{GridError, Result};

use super::message::MuxId;

#[derive(Debug)]
pub struct Stream {
    mux_id: MuxId,
    tx: mpsc::Sender<Vec<u8>>,
    rx: Arc<Mutex<mpsc::Receiver<Vec<u8>>>>,
    closed: Arc<AtomicBool>,
}

impl Clone for Stream {
    fn clone(&self) -> Self {
        Self {
            mux_id: self.mux_id,
            tx: self.tx.clone(),
            rx: Arc::clone(&self.rx),
            closed: Arc::clone(&self.closed),
        }
    }
}

impl Stream {
    pub fn new(mux_id: MuxId, tx: mpsc::Sender<Vec<u8>>, rx: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            mux_id,
            tx,
            rx: Arc::new(Mutex::new(rx)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn mux_id(&self) -> MuxId {
        self.mux_id
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub async fn send(&self, payload: Vec<u8>) -> Result<()> {
        if self.is_closed() {
            return Err(GridError::StreamClosed(self.mux_id));
        }

        self.tx
            .send(payload)
            .await
            .map_err(|_| GridError::StreamClosed(self.mux_id))
    }

    pub async fn recv(&self) -> Option<Vec<u8>> {
        let mut guard = self.rx.lock().await;
        guard.recv().await
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}
