//! SFTP server implementation
//!
//! Implements a basic SFTP server that maps to S3 operations.
//! This is a simplified implementation - a production version would use
//! a full SSH/SFTP library like russh.

use std::sync::Arc;

use maxio_iam::IAMSys;
use maxio_storage::traits::ObjectLayer;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::config::FtpConfig;
use crate::vfs::Vfs;
use crate::{FtpError, Result};

/// SFTP server
pub struct SftpServer {
    config: FtpConfig,
    vfs: Arc<Vfs>,
}

impl SftpServer {
    /// Create a new SFTP server
    pub fn new(config: FtpConfig, storage: Arc<dyn ObjectLayer>, iam: Arc<IAMSys>) -> Self {
        let vfs = Arc::new(Vfs::new(storage, iam));
        Self { config, vfs }
    }

    /// Start the SFTP server
    pub async fn run(&self) -> Result<()> {
        if !self.config.sftp_enabled {
            info!("SFTP server is disabled");
            return Ok(());
        }

        let listener = TcpListener::bind(&self.config.sftp_address)
            .await
            .map_err(|e| FtpError::Io(e))?;

        info!("SFTP server listening on {}", self.config.sftp_address);

        // Note: A real SFTP implementation would:
        // 1. Perform SSH key exchange
        // 2. Handle SSH authentication (password or public key)
        // 3. Implement the SFTP subsystem protocol
        //
        // This would require a library like russh or thrussh.
        // For now, we just accept connections and log them.

        loop {
            match listener.accept().await {
                Ok((_stream, addr)) => {
                    info!("SFTP connection from {} (SSH handshake not implemented)", addr);
                    // In a real implementation:
                    // - Perform SSH handshake
                    // - Authenticate user
                    // - Handle SFTP subsystem requests
                    warn!("SFTP protocol not fully implemented - connection rejected");
                }
                Err(e) => {
                    warn!("SFTP accept error: {}", e);
                }
            }
        }
    }

    /// Check if SFTP is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.sftp_enabled
    }
}

// SFTP protocol constants (for future implementation)
#[allow(dead_code)]
mod sftp_protocol {
    // SFTP packet types
    pub const SSH_FXP_INIT: u8 = 1;
    pub const SSH_FXP_VERSION: u8 = 2;
    pub const SSH_FXP_OPEN: u8 = 3;
    pub const SSH_FXP_CLOSE: u8 = 4;
    pub const SSH_FXP_READ: u8 = 5;
    pub const SSH_FXP_WRITE: u8 = 6;
    pub const SSH_FXP_LSTAT: u8 = 7;
    pub const SSH_FXP_FSTAT: u8 = 8;
    pub const SSH_FXP_SETSTAT: u8 = 9;
    pub const SSH_FXP_FSETSTAT: u8 = 10;
    pub const SSH_FXP_OPENDIR: u8 = 11;
    pub const SSH_FXP_READDIR: u8 = 12;
    pub const SSH_FXP_REMOVE: u8 = 13;
    pub const SSH_FXP_MKDIR: u8 = 14;
    pub const SSH_FXP_RMDIR: u8 = 15;
    pub const SSH_FXP_REALPATH: u8 = 16;
    pub const SSH_FXP_STAT: u8 = 17;
    pub const SSH_FXP_RENAME: u8 = 18;
    pub const SSH_FXP_READLINK: u8 = 19;
    pub const SSH_FXP_SYMLINK: u8 = 20;
    pub const SSH_FXP_STATUS: u8 = 101;
    pub const SSH_FXP_HANDLE: u8 = 102;
    pub const SSH_FXP_DATA: u8 = 103;
    pub const SSH_FXP_NAME: u8 = 104;
    pub const SSH_FXP_ATTRS: u8 = 105;

    // SFTP status codes
    pub const SSH_FX_OK: u32 = 0;
    pub const SSH_FX_EOF: u32 = 1;
    pub const SSH_FX_NO_SUCH_FILE: u32 = 2;
    pub const SSH_FX_PERMISSION_DENIED: u32 = 3;
    pub const SSH_FX_FAILURE: u32 = 4;
    pub const SSH_FX_BAD_MESSAGE: u32 = 5;
    pub const SSH_FX_NO_CONNECTION: u32 = 6;
    pub const SSH_FX_CONNECTION_LOST: u32 = 7;
    pub const SSH_FX_OP_UNSUPPORTED: u32 = 8;

    // File open flags
    pub const SSH_FXF_READ: u32 = 0x00000001;
    pub const SSH_FXF_WRITE: u32 = 0x00000002;
    pub const SSH_FXF_APPEND: u32 = 0x00000004;
    pub const SSH_FXF_CREAT: u32 = 0x00000008;
    pub const SSH_FXF_TRUNC: u32 = 0x00000010;
    pub const SSH_FXF_EXCL: u32 = 0x00000020;
}
