use std::sync::Arc;

use axum::{Json, extract::State};

use crate::{
    AdminSys,
    handlers::AdminApiError,
    types::{AdminInfo, ServerProperties, ServiceStatus, StorageInfo},
};

pub async fn server_info(State(admin): State<Arc<AdminSys>>) -> Result<Json<AdminInfo>, AdminApiError> {
    let storage = collect_storage_info(admin.object_layer()).await?;
    let services = ServiceStatus {
        iam: "online".to_string(),
        storage: "online".to_string(),
        distributed: if admin.distributed().is_distributed() {
            "online".to_string()
        } else {
            "standalone".to_string()
        },
    };

    Ok(Json(AdminInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: admin.uptime_seconds(),
        boot_time: admin.boot_time(),
        server: ServerProperties {
            endpoint: admin.endpoint().to_string(),
            region: admin.region().to_string(),
        },
        storage,
        services,
    }))
}

async fn collect_storage_info(
    object_layer: Arc<dyn maxio_storage::traits::ObjectLayer>,
) -> Result<StorageInfo, AdminApiError> {
    let buckets = object_layer.list_buckets().await.map_err(AdminApiError::from)?;

    let mut used_bytes: u64 = 0;
    for bucket in &buckets {
        let mut marker = String::new();
        loop {
            let page = object_layer
                .list_objects(&bucket.name, "", &marker, "", 1000)
                .await
                .map_err(AdminApiError::from)?;

            for object in &page.objects {
                if let Ok(size) = u64::try_from(object.size) {
                    used_bytes = used_bytes.saturating_add(size);
                }
            }

            if !page.is_truncated {
                break;
            }

            let Some(next_marker) = page.next_marker else {
                break;
            };
            marker = next_marker;
        }
    }

    Ok(StorageInfo {
        used_bytes,
        available_bytes: 0,
    })
}
