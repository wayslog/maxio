use std::sync::Arc;

use axum::{middleware, routing::delete, routing::get, routing::post, routing::put, Router};
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
        // Server info
        .route("/minio/admin/v3/info", get(handlers::info::server_info))
        // Config operations
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
        // User operations
        .route("/minio/admin/v3/add-user", put(handlers::user::add_user))
        .route("/minio/admin/v3/remove-user", delete(handlers::user::remove_user))
        .route("/minio/admin/v3/list-users", get(handlers::user::list_users))
        .route("/minio/admin/v3/user-info", get(handlers::user::get_user_info))
        .route("/minio/admin/v3/set-user-status", put(handlers::user::set_user_status))
        .route("/minio/admin/v3/accountinfo", get(handlers::user::account_info))
        // Group operations
        .route("/minio/admin/v3/update-group-members", put(handlers::user::update_group_members))
        .route("/minio/admin/v3/group", get(handlers::user::get_group))
        .route("/minio/admin/v3/groups", get(handlers::user::list_groups))
        .route("/minio/admin/v3/set-group-status", put(handlers::user::set_group_status))
        // Builtin IAM policy operations
        .route("/minio/admin/v3/idp/builtin/policy-entities", get(handlers::user::list_policy_mapping_entities))
        .route("/minio/admin/v3/idp/builtin/policy/{operation}", post(handlers::user::attach_detach_policy_builtin))
        // IAM export/import
        .route("/minio/admin/v3/export-iam", get(handlers::user::export_iam))
        .route("/minio/admin/v3/import-iam", put(handlers::user::import_iam))
        // Service account operations
        .route(
            "/minio/admin/v3/add-service-account",
            put(handlers::service_account::add_service_account),
        )
        .route(
            "/minio/admin/v3/update-service-account",
            post(handlers::service_account::update_service_account),
        )
        .route(
            "/minio/admin/v3/info-service-account",
            get(handlers::service_account::info_service_account),
        )
        .route(
            "/minio/admin/v3/list-service-accounts",
            get(handlers::service_account::list_service_accounts),
        )
        .route(
            "/minio/admin/v3/delete-service-account",
            delete(handlers::service_account::delete_service_account),
        )
        // Policy operations
        .route(
            "/minio/admin/v3/add-canned-policy",
            put(handlers::policy::add_policy),
        )
        .route(
            "/minio/admin/v3/remove-canned-policy",
            delete(handlers::policy::remove_policy),
        )
        .route(
            "/minio/admin/v3/list-canned-policies",
            get(handlers::policy::list_policies),
        )
        // Batch job operations
        .route(
            "/minio/admin/v3/start-job",
            post(handlers::batch::submit_batch_job),
        )
        .route(
            "/minio/admin/v3/list-jobs",
            get(handlers::batch::list_batch_jobs),
        )
        .route(
            "/minio/admin/v3/describe-job",
            get(handlers::batch::get_batch_job),
        )
        .route(
            "/minio/admin/v3/cancel-job",
            delete(handlers::batch::cancel_batch_job),
        )
        // Profiling operations
        .route(
            "/minio/admin/v3/profile",
            post(handlers::profiling::start_profiling),
        )
        .route("/minio/admin/v3/trace", get(handlers::trace::get_trace))
        // Speed test operations
        .route(
            "/minio/admin/v3/speedtest",
            get(handlers::speedtest::run_speedtest),
        )
        .route(
            "/minio/admin/v3/drivespeed",
            get(handlers::speedtest::run_drivespeed),
        )
        .route(
            "/minio/admin/v3/netspeed",
            get(handlers::speedtest::run_netspeed),
        )
        // Pool operations
        .route(
            "/minio/admin/v3/pools/list",
            get(handlers::pools::list_pools),
        )
        .route(
            "/minio/admin/v3/pools/status",
            get(handlers::pools::status_pool),
        )
        .route(
            "/minio/admin/v3/pools/decommission",
            post(handlers::pools::start_decommission),
        )
        .route(
            "/minio/admin/v3/pools/cancel",
            post(handlers::pools::cancel_decommission),
        )
        // Rebalance operations
        .route(
            "/minio/admin/v3/rebalance/start",
            post(handlers::pools::rebalance_start),
        )
        .route(
            "/minio/admin/v3/rebalance/status",
            get(handlers::pools::rebalance_status),
        )
        .route(
            "/minio/admin/v3/rebalance/stop",
            post(handlers::pools::rebalance_stop),
        )
        // Healing operations
        .route("/minio/admin/v3/heal/", post(handlers::healing::heal_root))
        .route(
            "/minio/admin/v3/heal/{bucket}",
            post(handlers::healing::heal_handler),
        )
        .route(
            "/minio/admin/v3/heal/{bucket}/{*prefix}",
            post(handlers::healing::heal_handler),
        )
        .route(
            "/minio/admin/v3/background-heal/status",
            post(handlers::healing::background_heal_status),
        )
        // LDAP operations
        .route(
            "/minio/admin/v3/idp/ldap/policy-entities",
            get(handlers::ldap::list_ldap_policy_mapping_entities),
        )
        .route(
            "/minio/admin/v3/idp/ldap/policy/{operation}",
            post(handlers::ldap::attach_detach_policy_ldap),
        )
        .route(
            "/minio/admin/v3/idp/ldap/add-service-account",
            put(handlers::ldap::add_service_account_ldap),
        )
        .route(
            "/minio/admin/v3/idp/ldap/list-access-keys",
            get(handlers::ldap::list_access_keys_ldap),
        )
        .route(
            "/minio/admin/v3/idp/ldap/list-access-keys-bulk",
            get(handlers::ldap::list_access_keys_ldap_bulk),
        )
        // OpenID operations
        .route(
            "/minio/admin/v3/idp/openid/list-access-keys-bulk",
            get(handlers::openid::list_access_keys_openid_bulk),
        )
        // Tier operations
        .route(
            "/minio/admin/v3/tier",
            put(handlers::tiers::add_tier).get(handlers::tiers::list_tiers),
        )
        .route(
            "/minio/admin/v3/tier/{tier}",
            post(handlers::tiers::edit_tier)
                .delete(handlers::tiers::remove_tier)
                .get(handlers::tiers::verify_tier),
        )
        .route(
            "/minio/admin/v3/tier-stats",
            get(handlers::tiers::tier_stats),
        )
        // Bucket quota operations
        .route(
            "/minio/admin/v3/get-bucket-quota",
            get(handlers::bucket_admin::get_bucket_quota),
        )
        .route(
            "/minio/admin/v3/set-bucket-quota",
            put(handlers::bucket_admin::put_bucket_quota),
        )
        // Remote target operations
        .route(
            "/minio/admin/v3/list-remote-targets",
            get(handlers::bucket_admin::list_remote_targets),
        )
        .route(
            "/minio/admin/v3/set-remote-target",
            put(handlers::bucket_admin::set_remote_target),
        )
        .route(
            "/minio/admin/v3/remove-remote-target",
            delete(handlers::bucket_admin::remove_remote_target),
        )
        // Bucket metadata operations
        .route(
            "/minio/admin/v3/export-bucket-metadata",
            get(handlers::bucket_admin::export_bucket_metadata),
        )
        .route(
            "/minio/admin/v3/import-bucket-metadata",
            put(handlers::bucket_admin::import_bucket_metadata),
        )
        // Replication operations
        .route(
            "/minio/admin/v3/replication/diff",
            post(handlers::bucket_admin::replication_diff),
        )
        .route(
            "/minio/admin/v3/replication/mrf",
            get(handlers::bucket_admin::replication_mrf),
        )
        // Site replication operations
        .route(
            "/minio/admin/v3/site-replication/add",
            put(handlers::site_replication::site_replication_add),
        )
        .route(
            "/minio/admin/v3/site-replication/remove",
            put(handlers::site_replication::site_replication_remove),
        )
        .route(
            "/minio/admin/v3/site-replication/info",
            get(handlers::site_replication::site_replication_info),
        )
        .route(
            "/minio/admin/v3/site-replication/status",
            get(handlers::site_replication::site_replication_status),
        )
        .route(
            "/minio/admin/v3/site-replication/edit",
            put(handlers::site_replication::site_replication_edit),
        )
        // IDP config operations
        .route(
            "/minio/admin/v3/idp-config/{type}",
            get(handlers::idp_config::list_identity_provider_cfg),
        )
        .route(
            "/minio/admin/v3/idp-config/{type}/{name}",
            put(handlers::idp_config::add_identity_provider_cfg)
                .post(handlers::idp_config::update_identity_provider_cfg)
                .get(handlers::idp_config::get_identity_provider_cfg)
                .delete(handlers::idp_config::delete_identity_provider_cfg),
        )
        // Auth middleware
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&admin),
            admin_auth,
        ))
        .with_state(admin)
}
