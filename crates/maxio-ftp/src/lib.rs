//! FTP/SFTP Protocol Support for maxio
//!
//! Provides FTP and SFTP server implementations that map to S3 operations.
//! - Buckets are exposed as top-level directories
//! - Objects are exposed as files within bucket directories
//! - Authentication uses IAM credentials

mod config;
mod ftp;
mod sftp;
mod vfs;

pub use config::FtpConfig;
pub use ftp::FtpServer;
pub use sftp::SftpServer;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum FtpError {
    #[error("authentication failed")]
    AuthFailed,

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, FtpError>;
