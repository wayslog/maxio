//! FTP/SFTP server configuration

use serde::{Deserialize, Serialize};

/// FTP server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtpConfig {
    /// Enable FTP server
    pub ftp_enabled: bool,
    /// FTP listen address (e.g., "0.0.0.0:21")
    pub ftp_address: String,
    /// FTP passive port range start
    pub ftp_passive_port_start: u16,
    /// FTP passive port range end
    pub ftp_passive_port_end: u16,

    /// Enable SFTP server
    pub sftp_enabled: bool,
    /// SFTP listen address (e.g., "0.0.0.0:22")
    pub sftp_address: String,
    /// Path to SSH host key file
    pub sftp_host_key_path: Option<String>,
}

impl Default for FtpConfig {
    fn default() -> Self {
        Self {
            ftp_enabled: false,
            ftp_address: "0.0.0.0:8021".to_string(),
            ftp_passive_port_start: 30000,
            ftp_passive_port_end: 30100,
            sftp_enabled: false,
            sftp_address: "0.0.0.0:8022".to_string(),
            sftp_host_key_path: None,
        }
    }
}

impl FtpConfig {
    /// Check if any protocol is enabled
    pub fn is_enabled(&self) -> bool {
        self.ftp_enabled || self.sftp_enabled
    }

    /// Get passive port range
    pub fn passive_port_range(&self) -> std::ops::RangeInclusive<u16> {
        self.ftp_passive_port_start..=self.ftp_passive_port_end
    }
}
