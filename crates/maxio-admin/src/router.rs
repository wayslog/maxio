use std::sync::Arc;

use axum::{middleware, routing::get, Router};
use maxio_common::error::Result;
use maxio_distributed::DistributedSys;
use maxio_storage::traits::ObjectLayer;

use crate::{
    handlers,
    metrics::{ApiMetrics, MetricsRegistry, StorageMetrics, SystemMetrics},
    middleware::admin_auth,
    AdminSys,
};

pub struct AdminState {
    pub object_layer: Arc<dyn ObjectLayer>,
    pub distributed: Arc<DistributedSys>,
    pub registry: Arc<MetricsRegistry>,
    pub api_metrics: Arc<ApiMetrics>,
    pub storage_metrics: Arc<StorageMetrics>,
    pub system_metrics: Arc<SystemMetrics>,
}

impl AdminState {
    pub fn new(
        object_layer: Arc<dyn ObjectLayer>,
        distributed: Arc<DistributedSys>,
    ) -> Result<Self> {
        let registry = Arc::new(MetricsRegistry::new());
        let api_metrics = Arc::new(ApiMetrics::register(registry.as_ref())?);
        let storage_metrics = Arc::new(StorageMetrics::register(registry.as_ref())?);
        let system_metrics = Arc::new(SystemMetrics::register(registry.as_ref())?);

        Ok(Self {
            object_layer,
            distributed,
            registry,
            api_metrics,
            storage_metrics,
            system_metrics,
        })
    }
}

pub fn admin_router(state: Arc<AdminState>) -> Router {
    Router::new()
        .route("/minio/health/live", get(handlers::health::health_live))
        .route("/minio/health/ready", get(handlers::health::health_ready))
        .route(
            "/minio/health/cluster",
            get(handlers::health::health_cluster),
        )
        .route(
            "/minio/prometheus/metrics",
            get(handlers::metrics::prometheus_metrics),
        )
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            handlers::metrics::track_api_metrics,
        ))
        .with_state(state)
}

pub fn admin_api_router(admin: Arc<AdminSys>) -> Router {
    Router::new()
        .route("/minio/admin/v3/info", get(handlers::info::server_info))
        .route(
            "/minio/admin/v3/config",
            get(handlers::config::get_config).put(handlers::config::set_config),
        )
        .route(
            "/minio/admin/v3/config-kv",
            get(handlers::config::get_config_kv)
                .put(handlers::config::set_config_kv)
                .delete(handlers::config::delete_config_kv),
        )
        .route(
            "/minio/admin/v3/add-user",
            axum::routing::put(handlers::user::add_user),
        )
        .route(
            "/minio/admin/v3/remove-user",
            axum::routing::delete(handlers::user::remove_user),
        )
        .route(
            "/minio/admin/v3/list-users",
            get(handlers::user::list_users),
        )
        .route(
            "/minio/admin/v3/user-info",
            get(handlers::user::get_user_info),
        )
        .route(
            "/minio/admin/v3/add-policy",
            axum::routing::put(handlers::policy::add_policy),
        )
        .route(
            "/minio/admin/v3/remove-policy",
            axum::routing::delete(handlers::policy::remove_policy),
        )
        .route(
            "/minio/admin/v3/list-policies",
            get(handlers::policy::list_policies),
        )
        .route(
            "/minio/admin/v3/batch/jobs",
            get(handlers::batch::list_batch_jobs).post(handlers::batch::submit_batch_job),
        )
        .route(
            "/minio/admin/v3/batch/jobs/{job_id}",
            get(handlers::batch::get_batch_job).delete(handlers::batch::cancel_batch_job),
        )
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&admin),
            admin_auth,
        ))
        .with_state(admin)
}
