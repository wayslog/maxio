use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use maxio_auth::credentials::{CredentialProvider, StaticCredentialProvider};
use maxio_iam::IAMSys;
use maxio_storage::{
    erasure::{ErasureConfig, objects::ErasureObjectLayer},
    single::SingleDiskObjectLayer,
    traits::ObjectLayer,
};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "maxio", about = "S3-compatible object storage server")]
struct Cli {
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    #[arg(long, default_value = "9000")]
    port: u16,

    #[arg(long, default_value = "./data")]
    data_dir: String,

    #[arg(long, default_value_t = false)]
    erasure: bool,

    #[arg(long)]
    disks: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::from_default_env().add_directive("maxio=info".parse()?);
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .init();

    let cli = Cli::parse();
    let addr = format!("{}:{}", cli.host, cli.port);
    let object_layer: Arc<dyn ObjectLayer> = if cli.erasure {
        let disks = cli
            .disks
            .as_deref()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "--disks is required when --erasure is enabled"))?;
        let disk_paths: Vec<PathBuf> = disks
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .collect();

        if disk_paths.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--disks must include at least one disk path",
            )
            .into());
        }

        Arc::new(ErasureObjectLayer::new(disk_paths, ErasureConfig::default()).await?)
    } else {
        let data_dir = PathBuf::from(&cli.data_dir);
        tokio::fs::create_dir_all(&data_dir).await?;
        Arc::new(SingleDiskObjectLayer::new(data_dir).await?)
    };
    let access_key = std::env::var("MAXIO_ROOT_USER").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key =
        std::env::var("MAXIO_ROOT_PASSWORD").unwrap_or_else(|_| "minioadmin".to_string());
    let iam_data_dir = PathBuf::from(&cli.data_dir);
    tokio::fs::create_dir_all(&iam_data_dir).await?;
    let iam = Arc::new(IAMSys::new(&iam_data_dir).await?);
    let credential_provider: Arc<dyn CredentialProvider> =
        Arc::new(StaticCredentialProvider::with_iam(access_key, secret_key, Arc::clone(&iam)));

    let app = maxio_s3_api::router::s3_router(object_layer, credential_provider, iam);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("maxio server listening on {addr}");
    axum::serve(listener, app).await?;

    Ok(())
}
