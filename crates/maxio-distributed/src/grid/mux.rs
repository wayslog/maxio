use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::{
    sync::{RwLock, mpsc, oneshot},
    time,
};

use crate::errors::{GridError, Result};

use super::{
    handler::{HandlerKind, HandlerRegistry},
    message::{Flags, Message, MuxId, Op, Seq},
    stream::Stream,
};

#[derive(Clone)]
pub struct MuxClient {
    tx: mpsc::Sender<Message>,
    next_seq: Arc<std::sync::atomic::AtomicU32>,
    pending: Arc<RwLock<HashMap<Seq, oneshot::Sender<Message>>>>,
    timeout: Duration,
}

impl MuxClient {
    pub fn new(tx: mpsc::Sender<Message>, timeout: Duration) -> Self {
        Self {
            tx,
            next_seq: Arc::new(std::sync::atomic::AtomicU32::new(1)),
            pending: Arc::new(RwLock::new(HashMap::new())),
            timeout,
        }
    }

    pub async fn request(&self, mux_id: MuxId, handler: u8, payload: Vec<u8>, flags: Flags) -> Result<Message> {
        let seq = self
            .next_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let message = Message::new(mux_id, seq, handler, Op::Request, flags, payload);
        let (tx, rx) = oneshot::channel();
        self.pending.write().await.insert(seq, tx);

        if let Err(_send_err) = self.tx.send(message).await {
            self.pending.write().await.remove(&seq);
            return Err(GridError::ConnectionClosed);
        }

        match time::timeout(self.timeout, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => {
                self.pending.write().await.remove(&seq);
                Err(GridError::ConnectionClosed)
            }
            Err(_) => {
                self.pending.write().await.remove(&seq);
                Err(GridError::RequestTimeout { seq })
            }
        }
    }

    pub async fn handle_response(&self, message: Message) -> Result<()> {
        let tx = self.pending.write().await.remove(&message.seq);
        match tx {
            Some(waiter) => waiter.send(message).map_err(|_| GridError::ConnectionClosed),
            None => Err(GridError::UnexpectedResponse { seq: message.seq }),
        }
    }

    pub async fn fail_all(&self, err: &GridError) {
        let mut pending = self.pending.write().await;
        pending.clear();
        tracing::debug!(?err, "mux pending requests cleared");
    }
}

#[derive(Clone)]
pub struct MuxServer {
    handlers: HandlerRegistry,
    tx: mpsc::Sender<Message>,
    stream_incoming: Arc<RwLock<HashMap<MuxId, mpsc::Sender<Vec<u8>>>>>,
}

impl MuxServer {
    pub fn new(handlers: HandlerRegistry, tx: mpsc::Sender<Message>) -> Self {
        Self {
            handlers,
            tx,
            stream_incoming: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn handle_request(&self, message: Message) -> Result<()> {
        let (subroute, payload) = message.extract_subroute()?;
        let handler = self
            .handlers
            .get(message.handler, subroute.as_deref())
            .await
            .ok_or(GridError::HandlerNotFound {
                handler: message.handler,
                subroute,
            })?;

        match handler {
            HandlerKind::Single(single_handler) => {
                let response_payload = single_handler.handle(payload.to_vec()).await?;
                let response = Message::new(
                    message.mux_id,
                    message.seq,
                    message.handler,
                    Op::Response,
                    Flags::EOF,
                    response_payload,
                );
                self.tx
                    .send(response)
                    .await
                    .map_err(|_| GridError::ConnectionClosed)
            }
            HandlerKind::Stream(stream_handler) => {
                let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(64);
                let (in_tx, in_rx) = mpsc::channel::<Vec<u8>>(64);
                self.stream_incoming.write().await.insert(message.mux_id, in_tx);

                let stream = Stream::new(message.mux_id, out_tx, in_rx);
                let writer = self.tx.clone();
                let handler_id = message.handler;
                let seq = message.seq;
                let mux_id = message.mux_id;

                tokio::spawn(async move {
                    while let Some(chunk) = out_rx.recv().await {
                        let frame = Message::new(mux_id, seq, handler_id, Op::Response, Flags::NONE, chunk);
                        if writer.send(frame).await.is_err() {
                            break;
                        }
                    }

                    let _ = writer
                        .send(Message::new(mux_id, seq, handler_id, Op::Response, Flags::EOF, Vec::new()))
                        .await;
                });

                stream_handler.open(stream, payload.to_vec()).await
            }
        }
    }

    pub async fn handle_stream_chunk(&self, message: Message) -> Result<()> {
        let sender = self.stream_incoming.read().await.get(&message.mux_id).cloned();
        match sender {
            Some(tx) => tx
                .send(message.payload)
                .await
                .map_err(|_| GridError::StreamClosed(message.mux_id)),
            None => Err(GridError::UnknownMux { mux_id: message.mux_id }),
        }
    }

    pub async fn close_stream(&self, mux_id: MuxId) {
        self.stream_incoming.write().await.remove(&mux_id);
    }
}
