use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use maxio_auth::credentials::{CredentialProvider, StaticCredentialProvider};
use maxio_distributed::{ClusterConfig, DistributedSys};
use maxio_iam::IAMSys;
use maxio_lifecycle::{LifecycleStore, LifecycleSys};
use maxio_notification::{NotificationStore, NotificationSys, WebhookTarget};
use maxio_storage::{
    erasure::{ErasureConfig, objects::ErasureObjectLayer},
    single::SingleDiskObjectLayer,
    traits::ObjectLayer,
};
use tracing::{info, warn};
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
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let cli = Cli::parse();
    let addr = format!("{}:{}", cli.host, cli.port);
    let (object_layer, notification_root): (Arc<dyn ObjectLayer>, PathBuf) = if cli.erasure {
        let disks = cli.disks.as_deref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--disks is required when --erasure is enabled",
            )
        })?;
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

        let notification_root = disk_paths[0].clone();
        (
            Arc::new(ErasureObjectLayer::new(disk_paths, ErasureConfig::default()).await?),
            notification_root,
        )
    } else {
        let data_dir = PathBuf::from(&cli.data_dir);
        tokio::fs::create_dir_all(&data_dir).await?;
        (
            Arc::new(SingleDiskObjectLayer::new(data_dir.clone()).await?),
            data_dir,
        )
    };
    let access_key = std::env::var("MAXIO_ROOT_USER").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key =
        std::env::var("MAXIO_ROOT_PASSWORD").unwrap_or_else(|_| "minioadmin".to_string());
    let iam_data_dir = PathBuf::from(&cli.data_dir);
    tokio::fs::create_dir_all(&iam_data_dir).await?;
    let iam = Arc::new(IAMSys::new(&iam_data_dir).await?);
    let credential_provider: Arc<dyn CredentialProvider> = Arc::new(
        StaticCredentialProvider::with_iam(access_key, secret_key, Arc::clone(&iam)),
    );

    let mut notification_sys = NotificationSys::new(NotificationStore::new(notification_root.clone()));
    if let Ok(endpoint) = std::env::var("MAXIO_NOTIFY_WEBHOOK_ENDPOINT") {
        let endpoint = endpoint.trim();
        if !endpoint.is_empty() {
            notification_sys.register_target(
                "webhook".to_string(),
                Box::new(WebhookTarget::new(endpoint.to_string())),
            );
            info!("webhook notification target enabled");
        }
    }
    let notification_sys = Arc::new(notification_sys);
    let lifecycle_store_root = notification_root.clone();
    let lifecycle_sys = Arc::new(LifecycleSys::new(
        LifecycleStore::new(lifecycle_store_root),
        notification_root,
    ));

    let lifecycle_runner = Arc::clone(&lifecycle_sys);
    let lifecycle_objects = Arc::clone(&object_layer);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60 * 60));
        loop {
            interval.tick().await;
            if let Err(err) = lifecycle_runner
                .run_lifecycle_scan(Arc::clone(&lifecycle_objects))
                .await
            {
                warn!(error = %err, "lifecycle background scan failed");
            }
        }
    });
    info!("lifecycle background scanner enabled");

    let default_node_endpoint = format!("http://127.0.0.1:{}", cli.port);
    let cluster_config = ClusterConfig::from_env()
        .unwrap_or_else(|| ClusterConfig::single(default_node_endpoint));
    let distributed_sys = Arc::new(DistributedSys::new(cluster_config).await);

    let app = maxio_s3_api::router::s3_router(
        object_layer,
        credential_provider,
        iam,
        notification_sys,
        lifecycle_sys,
        distributed_sys,
    );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("maxio server listening on {addr}");
    axum::serve(listener, app).await?;

    Ok(())
}
