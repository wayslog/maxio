use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{RwLock, mpsc},
    time,
};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::errors::{GridError, Result};

use super::{
    handler::HandlerRegistry,
    message::{Flags, Message, MuxId, Op},
    mux::{MuxClient, MuxServer},
};

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub enum ConnectionState {
    Unconnected,
    Connecting,
    Connected,
    Error(String),
}

#[derive(Clone)]
pub struct Connection {
    remote_addr: String,
    state: Arc<RwLock<ConnectionState>>,
    outgoing_tx: mpsc::Sender<Message>,
    outgoing_rx: Arc<RwLock<Option<mpsc::Receiver<Message>>>>,
    inbound_tx: mpsc::Sender<Message>,
    mux_client: MuxClient,
    mux_server: MuxServer,
}

impl Connection {
    pub fn new(remote_addr: String, handlers: HandlerRegistry) -> Self {
        let (outgoing_tx, outgoing_rx) = mpsc::channel(512);
        let (inbound_tx, inbound_rx) = mpsc::channel(512);

        let mux_client = MuxClient::new(outgoing_tx.clone(), Duration::from_secs(20));
        let mux_server = MuxServer::new(handlers, outgoing_tx.clone());

        let connection = Self {
            remote_addr,
            state: Arc::new(RwLock::new(ConnectionState::Unconnected)),
            outgoing_tx,
            outgoing_rx: Arc::new(RwLock::new(Some(outgoing_rx))),
            inbound_tx,
            mux_client,
            mux_server,
        };

        connection.spawn_dispatcher(inbound_rx);
        connection
    }

    pub fn remote_addr(&self) -> &str {
        &self.remote_addr
    }

    pub async fn state(&self) -> ConnectionState {
        self.state.read().await.clone()
    }

    pub async fn start(&self) -> Result<()> {
        let mut outgoing = self
            .outgoing_rx
            .write()
            .await
            .take()
            .ok_or(GridError::ConnectionAlreadyStarted)?;

        let this = self.clone();
        tokio::spawn(async move {
            this.run(&mut outgoing).await;
        });
        Ok(())
    }

    pub async fn request(
        &self,
        mux_id: MuxId,
        handler: u8,
        payload: Vec<u8>,
        flags: Flags,
    ) -> Result<Message> {
        self.mux_client
            .request(mux_id, handler, payload, flags)
            .await
    }

    pub async fn send(&self, message: Message) -> Result<()> {
        self.outgoing_tx
            .send(message)
            .await
            .map_err(|_| GridError::ConnectionClosed)
    }

    async fn set_state(&self, state: ConnectionState) {
        *self.state.write().await = state;
    }

    fn spawn_dispatcher(&self, mut inbound_rx: mpsc::Receiver<Message>) {
        let mux_client = self.mux_client.clone();
        let mux_server = self.mux_server.clone();
        let outgoing_tx = self.outgoing_tx.clone();

        tokio::spawn(async move {
            while let Some(message) = inbound_rx.recv().await {
                match message.op {
                    Op::Response => {
                        if message.flags.contains(Flags::EOF) {
                            mux_server.close_stream(message.mux_id).await;
                        }
                        if let Err(err) = mux_client.handle_response(message.clone()).await {
                            let _ = mux_server.handle_stream_chunk(message).await.map_err(
                                |stream_err| {
                                    tracing::debug!(?err, ?stream_err, "response dispatch failed");
                                },
                            );
                        }
                    }
                    Op::Request => {
                        if let Err(err) = mux_server.handle_request(message).await {
                            tracing::warn!(?err, "request dispatch failed");
                        }
                    }
                    Op::Ping => {
                        let pong = Message::new(
                            message.mux_id,
                            message.seq,
                            message.handler,
                            Op::Pong,
                            Flags::NONE,
                            Vec::new(),
                        );
                        let _ = outgoing_tx
                            .send(pong)
                            .await
                            .map_err(|err| tracing::debug!(?err, "pong shortcut failed"));
                    }
                    Op::Pong | Op::Connect | Op::Merged => {}
                }
            }
        });
    }

    async fn run(&self, outgoing: &mut mpsc::Receiver<Message>) {
        let mut backoff = Duration::from_secs(1);

        loop {
            self.set_state(ConnectionState::Connecting).await;

            match tokio_tungstenite::connect_async(&self.remote_addr).await {
                Ok((stream, _)) => {
                    self.set_state(ConnectionState::Connected).await;
                    backoff = Duration::from_secs(1);
                    let result = self.session(stream, outgoing).await;
                    if let Err(err) = result {
                        self.set_state(ConnectionState::Error(err.to_string()))
                            .await;
                        self.mux_client.fail_all(&err).await;
                    }
                    self.set_state(ConnectionState::Unconnected).await;
                }
                Err(err) => {
                    self.set_state(ConnectionState::Error(err.to_string()))
                        .await;
                    self.mux_client.fail_all(&GridError::WebSocket(err)).await;
                }
            }

            time::sleep(backoff).await;
            backoff = std::cmp::min(backoff.saturating_mul(2), RECONNECT_MAX_BACKOFF);
        }
    }

    async fn session(
        &self,
        stream: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
        outgoing: &mut mpsc::Receiver<Message>,
    ) -> Result<()> {
        let (mut ws_tx, mut ws_rx) = stream.split();
        let mut keepalive = time::interval(KEEPALIVE_INTERVAL);
        let mut last_pong = Instant::now();

        let connect_msg = Message::new(0, 0, 0, Op::Connect, Flags::STATELESS, Vec::new());
        ws_tx
            .send(WsMessage::Binary(connect_msg.encode()?.into()))
            .await
            .map_err(GridError::WebSocket)?;

        loop {
            tokio::select! {
                maybe_out = outgoing.recv() => {
                    let Some(msg) = maybe_out else {
                        return Err(GridError::ConnectionClosed);
                    };
                    ws_tx
                        .send(WsMessage::Binary(msg.encode()?.into()))
                        .await
                        .map_err(GridError::WebSocket)?;
                }
                incoming = ws_rx.next() => {
                    match incoming {
                        Some(Ok(WsMessage::Binary(bytes))) => {
                            let msg = Message::decode(&bytes)?;
                            if matches!(msg.op, Op::Pong) {
                                last_pong = Instant::now();
                            }
                            self.inbound_tx
                                .send(msg)
                                .await
                                .map_err(|_| GridError::ConnectionClosed)?;
                        }
                        Some(Ok(WsMessage::Ping(payload))) => {
                            ws_tx
                                .send(WsMessage::Pong(payload))
                                .await
                                .map_err(GridError::WebSocket)?;
                        }
                        Some(Ok(WsMessage::Pong(_))) => {
                            last_pong = Instant::now();
                        }
                        Some(Ok(WsMessage::Close(_))) => {
                            return Err(GridError::ConnectionClosed);
                        }
                        Some(Ok(_)) => {}
                        Some(Err(err)) => return Err(GridError::WebSocket(err)),
                        None => return Err(GridError::ConnectionClosed),
                    }
                }
                _ = keepalive.tick() => {
                    if last_pong.elapsed() > KEEPALIVE_TIMEOUT {
                        return Err(GridError::KeepaliveTimeout);
                    }

                    let ping = Message::new(0, 0, 0, Op::Ping, Flags::STATELESS, Vec::new());
                    ws_tx
                        .send(WsMessage::Binary(ping.encode()?.into()))
                        .await
                        .map_err(GridError::WebSocket)?;
                }
            }
        }
    }
}
