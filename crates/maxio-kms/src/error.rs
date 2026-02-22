use thiserror::Error;

#[derive(Debug, Error)]
pub enum KmsError {
    #[error("KMS not configured")]
    NotConfigured,
    #[error("key not found: {0}")]
    KeyNotFound(String),
    #[error("encryption failed: {0}")]
    EncryptionFailed(String),
    #[error("decryption failed: {0}")]
    DecryptionFailed(String),
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    #[error("authentication failed")]
    AuthenticationFailed,
    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

pub type Result<T> = std::result::Result<T, KmsError>;
