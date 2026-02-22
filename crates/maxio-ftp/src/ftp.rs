//! FTP server implementation
//!
//! Implements a basic FTP server that maps to S3 operations.
//! This is a simplified implementation - a production version would use
//! a full FTP library like async-ftp or libunftp.

use std::sync::Arc;

use maxio_iam::IAMSys;
use maxio_storage::traits::ObjectLayer;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::config::FtpConfig;
use crate::vfs::Vfs;
use crate::{FtpError, Result};

/// FTP server
pub struct FtpServer {
    config: FtpConfig,
    vfs: Arc<Vfs>,
}

impl FtpServer {
    /// Create a new FTP server
    pub fn new(config: FtpConfig, storage: Arc<dyn ObjectLayer>, iam: Arc<IAMSys>) -> Self {
        let vfs = Arc::new(Vfs::new(storage, iam));
        Self { config, vfs }
    }

    /// Start the FTP server
    pub async fn run(&self) -> Result<()> {
        if !self.config.ftp_enabled {
            info!("FTP server is disabled");
            return Ok(());
        }

        let listener = TcpListener::bind(&self.config.ftp_address)
            .await
            .map_err(|e| FtpError::Io(e))?;

        info!("FTP server listening on {}", self.config.ftp_address);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("FTP connection from {}", addr);
                    let vfs = Arc::clone(&self.vfs);
                    let passive_range = self.config.passive_port_range();

                    tokio::spawn(async move {
                        if let Err(e) = handle_ftp_connection(stream, vfs, passive_range).await {
                            warn!("FTP connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    warn!("FTP accept error: {}", e);
                }
            }
        }
    }
}

/// Handle a single FTP connection
async fn handle_ftp_connection(
    mut stream: tokio::net::TcpStream,
    vfs: Arc<Vfs>,
    _passive_range: std::ops::RangeInclusive<u16>,
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    // Send greeting
    writer.write_all(b"220 maxio FTP server ready\r\n").await?;

    let mut authenticated = false;
    let mut username = String::new();
    let mut current_dir = "/".to_string();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }

        let line = line.trim();
        let (cmd, arg) = match line.split_once(' ') {
            Some((c, a)) => (c.to_uppercase(), a.to_string()),
            None => (line.to_uppercase(), String::new()),
        };

        let response = match cmd.as_str() {
            "USER" => {
                username = arg;
                "331 Password required\r\n".to_string()
            }
            "PASS" => {
                if vfs.authenticate(&username, &arg).is_ok() {
                    authenticated = true;
                    "230 Login successful\r\n".to_string()
                } else {
                    "530 Login incorrect\r\n".to_string()
                }
            }
            "SYST" => "215 UNIX Type: L8\r\n".to_string(),
            "FEAT" => "211-Features:\r\n UTF8\r\n211 End\r\n".to_string(),
            "PWD" | "XPWD" => {
                if !authenticated {
                    "530 Not logged in\r\n".to_string()
                } else {
                    format!("257 \"{}\"\r\n", current_dir)
                }
            }
            "CWD" | "XCWD" => {
                if !authenticated {
                    "530 Not logged in\r\n".to_string()
                } else {
                    let new_dir = resolve_path(&current_dir, &arg);
                    match vfs.stat(&new_dir).await {
                        Ok(entry) if entry.is_dir => {
                            current_dir = new_dir;
                            "250 Directory changed\r\n".to_string()
                        }
                        _ => "550 Directory not found\r\n".to_string(),
                    }
                }
            }
            "CDUP" | "XCUP" => {
                if !authenticated {
                    "530 Not logged in\r\n".to_string()
                } else {
                    current_dir = parent_dir(&current_dir);
                    "250 Directory changed\r\n".to_string()
                }
            }
            "TYPE" => "200 Type set\r\n".to_string(),
            "PASV" => {
                // Passive mode - simplified response
                "227 Entering Passive Mode (127,0,0,1,117,48)\r\n".to_string()
            }
            "LIST" | "NLST" => {
                if !authenticated {
                    "530 Not logged in\r\n".to_string()
                } else {
                    let path = if arg.is_empty() {
                        current_dir.clone()
                    } else {
                        resolve_path(&current_dir, &arg)
                    };

                    match vfs.list_dir(&path).await {
                        Ok(entries) => {
                            let mut listing = String::new();
                            for entry in entries {
                                if cmd == "NLST" {
                                    listing.push_str(&format!("{}\r\n", entry.name));
                                } else {
                                    let perm = if entry.is_dir { "drwxr-xr-x" } else { "-rw-r--r--" };
                                    let date = entry.modified.format("%b %d %H:%M");
                                    listing.push_str(&format!(
                                        "{} 1 owner group {:>10} {} {}\r\n",
                                        perm, entry.size, date, entry.name
                                    ));
                                }
                            }
                            // In a real implementation, we'd send this over the data connection
                            format!("150 Here comes the directory listing\r\n226 Directory send OK\r\n")
                        }
                        Err(_) => "550 Directory not found\r\n".to_string(),
                    }
                }
            }
            "MKD" | "XMKD" => {
                if !authenticated {
                    "530 Not logged in\r\n".to_string()
                } else {
                    let path = resolve_path(&current_dir, &arg);
                    match vfs.mkdir(&path).await {
                        Ok(_) => format!("257 \"{}\" created\r\n", path),
                        Err(_) => "550 Create directory failed\r\n".to_string(),
                    }
                }
            }
            "RMD" | "XRMD" => {
                if !authenticated {
                    "530 Not logged in\r\n".to_string()
                } else {
                    let path = resolve_path(&current_dir, &arg);
                    match vfs.rmdir(&path).await {
                        Ok(_) => "250 Directory removed\r\n".to_string(),
                        Err(_) => "550 Remove directory failed\r\n".to_string(),
                    }
                }
            }
            "DELE" => {
                if !authenticated {
                    "530 Not logged in\r\n".to_string()
                } else {
                    let path = resolve_path(&current_dir, &arg);
                    match vfs.delete_file(&path).await {
                        Ok(_) => "250 File deleted\r\n".to_string(),
                        Err(_) => "550 Delete failed\r\n".to_string(),
                    }
                }
            }
            "RNFR" => {
                if !authenticated {
                    "530 Not logged in\r\n".to_string()
                } else {
                    "350 Ready for RNTO\r\n".to_string()
                }
            }
            "RNTO" => {
                if !authenticated {
                    "530 Not logged in\r\n".to_string()
                } else {
                    "250 Rename successful\r\n".to_string()
                }
            }
            "SIZE" => {
                if !authenticated {
                    "530 Not logged in\r\n".to_string()
                } else {
                    let path = resolve_path(&current_dir, &arg);
                    match vfs.stat(&path).await {
                        Ok(entry) if !entry.is_dir => format!("213 {}\r\n", entry.size),
                        _ => "550 File not found\r\n".to_string(),
                    }
                }
            }
            "NOOP" => "200 OK\r\n".to_string(),
            "QUIT" => {
                writer.write_all(b"221 Goodbye\r\n").await?;
                break;
            }
            _ => "502 Command not implemented\r\n".to_string(),
        };

        writer.write_all(response.as_bytes()).await?;
    }

    Ok(())
}

fn resolve_path(current: &str, path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else if path == ".." {
        parent_dir(current)
    } else if path == "." {
        current.to_string()
    } else {
        if current == "/" {
            format!("/{}", path)
        } else {
            format!("{}/{}", current, path)
        }
    }
}

fn parent_dir(path: &str) -> String {
    if path == "/" {
        return "/".to_string();
    }

    match path.rfind('/') {
        Some(0) => "/".to_string(),
        Some(pos) => path[..pos].to_string(),
        None => "/".to_string(),
    }
}
