use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use maxio_storage::{single::SingleDiskObjectLayer, traits::ObjectLayer};
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::from_default_env().add_directive("maxio=info".parse()?);
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .init();

    let cli = Cli::parse();
    let addr = format!("{}:{}", cli.host, cli.port);
    let data_dir = PathBuf::from(&cli.data_dir);

    tokio::fs::create_dir_all(&data_dir).await?;

    let object_layer: Arc<dyn ObjectLayer> = Arc::new(SingleDiskObjectLayer::new(data_dir).await?);

    let app = maxio_s3_api::router::s3_router(object_layer);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("maxio server listening on {addr}");
    axum::serve(listener, app).await?;

    Ok(())
}
