use thiserror::Error;

pub type Result<T> = std::result::Result<T, GridError>;

#[derive(Debug, Error)]
pub enum GridError {
    #[error("message encode error: {0}")]
    Encode(#[source] rmp_serde::encode::Error),
    #[error("message decode error: {0}")]
    Decode(#[source] rmp_serde::decode::Error),
    #[error("websocket error: {0}")]
    WebSocket(#[source] tokio_tungstenite::tungstenite::Error),
    #[error("connection already started")]
    ConnectionAlreadyStarted,
    #[error("connection closed")]
    ConnectionClosed,
    #[error("keepalive timeout")]
    KeepaliveTimeout,
    #[error("request timed out for sequence {seq}")]
    RequestTimeout { seq: u32 },
    #[error("unexpected response for sequence {seq}")]
    UnexpectedResponse { seq: u32 },
    #[error("handler not found: handler={handler}, subroute={subroute:?}")]
    HandlerNotFound {
        handler: u8,
        subroute: Option<String>,
    },
    #[error("stream closed for mux {0}")]
    StreamClosed(u32),
    #[error("unknown mux id {mux_id}")]
    UnknownMux { mux_id: u32 },
    #[error("invalid subroute payload")]
    InvalidSubroutePayload,
    #[error("subroute too long: {len}")]
    SubrouteTooLong { len: usize },
    #[error("invalid utf8 in subroute: {0}")]
    Utf8(#[source] std::str::Utf8Error),
    #[error("node not connected: {0}")]
    NodeNotConnected(String),
}
