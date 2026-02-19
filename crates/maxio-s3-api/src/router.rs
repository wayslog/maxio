use std::{collections::HashMap, sync::Arc};

use axum::{
    Extension, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    response::Response,
    routing::{delete, get, post, put},
};
use maxio_auth::{credentials::CredentialProvider, middleware::AuthLayer};
use maxio_common::error::MaxioError;
use maxio_distributed::DistributedSys;
use maxio_iam::IAMSys;
use maxio_lifecycle::LifecycleSys;
use maxio_notification::NotificationSys;
use maxio_storage::traits::ObjectLayer;

use crate::handlers;

use crate::error::S3Error;
use crate::middleware::response_headers::ResponseHeadersLayer;

const MAX_BODY_SIZE: usize = 5 * 1024 * 1024 * 1024; // 5GB

async fn get_bucket_dispatch(
    State(store): State<Arc<dyn ObjectLayer>>,
    Extension(notifications): Extension<Arc<NotificationSys>>,
    Extension(lifecycle): Extension<Arc<LifecycleSys>>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, S3Error> {
    if query.contains_key("acl") {
        handlers::acl::get_bucket_acl(State(store), Path(bucket)).await
    } else if query.contains_key("location") {
        handlers::bucket::get_bucket_location(State(store), Path(bucket)).await
    } else if query.contains_key("versioning") {
        handlers::versioning::get_bucket_versioning(State(store), Path(bucket)).await
    } else if query.contains_key("versions") {
        handlers::versioning::list_object_versions(State(store), Path(bucket), Query(query)).await
    } else if query.contains_key("uploads") {
        handlers::multipart::list_multipart_uploads(State(store), Path(bucket), Query(query)).await
    } else if query.contains_key("notification") {
        handlers::bucket::get_bucket_notification_configuration(
            State(store),
            Extension(notifications),
            Path(bucket),
        )
        .await
    } else if query.contains_key("lifecycle") {
        handlers::lifecycle::get_bucket_lifecycle_configuration(
            State(store),
            Extension(lifecycle),
            Path(bucket),
        )
        .await
    } else if query.contains_key("logging") {
        handlers::logging::get_bucket_logging(State(store), Path(bucket)).await
    } else if query.contains_key("replication") {
        handlers::replication::get_bucket_replication(State(store), Path(bucket)).await
    } else if query.contains_key("cors") {
        handlers::cors::get_bucket_cors(State(store), Path(bucket)).await
    } else if query.contains_key("website") {
        handlers::website::get_bucket_website(State(store), Path(bucket)).await
    } else if query.contains_key("policy") {
        handlers::bucket_policy::get_bucket_policy(State(store), Path(bucket)).await
    } else if query.contains_key("policyStatus") {
        handlers::bucket_policy::get_bucket_policy_status(State(store), Path(bucket)).await
    } else if query.contains_key("encryption") {
        handlers::bucket_encryption::get_bucket_encryption(State(store), Path(bucket)).await
    } else if query.contains_key("object-lock") {
        handlers::object_lock::get_object_lock_configuration(State(store), Path(bucket)).await
    } else if query.contains_key("publicAccessBlock") {
        handlers::public_access::get_public_access_block(State(store), Path(bucket)).await
    } else if query.contains_key("requestPayment") {
        handlers::request_payment::get_bucket_request_payment(State(store), Path(bucket)).await
    } else if query.contains_key("ownershipControls") {
        handlers::ownership::get_bucket_ownership_controls(State(store), Path(bucket)).await
    } else if query.contains_key("tagging") {
        handlers::bucket_tagging::get_bucket_tagging(State(store), Path(bucket)).await
    } else if query.contains_key("accelerate") {
        handlers::bucket_tagging::get_bucket_accelerate(State(store), Path(bucket)).await
    } else if query.get("list-type").is_some_and(|v| v == "2") {
        handlers::object::list_objects_v2(State(store), Path(bucket), Query(query)).await
    } else {
        handlers::object::list_objects_v1(State(store), Path(bucket), Query(query)).await
    }
}

async fn put_bucket_dispatch(
    State(store): State<Arc<dyn ObjectLayer>>,
    Extension(notifications): Extension<Arc<NotificationSys>>,
    Extension(lifecycle): Extension<Arc<LifecycleSys>>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    if query.contains_key("acl") {
        handlers::acl::put_bucket_acl(State(store), Path(bucket), headers, body).await
    } else if query.contains_key("versioning") {
        handlers::versioning::put_bucket_versioning(State(store), Path(bucket), body).await
    } else if query.contains_key("notification") {
        handlers::bucket::put_bucket_notification_configuration(
            State(store),
            Extension(notifications),
            Path(bucket),
            body,
        )
        .await
    } else if query.contains_key("lifecycle") {
        handlers::lifecycle::put_bucket_lifecycle_configuration(
            State(store),
            Extension(lifecycle),
            Path(bucket),
            body,
        )
        .await
    } else if query.contains_key("logging") {
        handlers::logging::put_bucket_logging(State(store), Path(bucket), body).await
    } else if query.contains_key("replication") {
        handlers::replication::put_bucket_replication(State(store), Path(bucket), body).await
    } else if query.contains_key("cors") {
        handlers::cors::put_bucket_cors(State(store), Path(bucket), body).await
    } else if query.contains_key("website") {
        handlers::website::put_bucket_website(State(store), Path(bucket), body).await
    } else if query.contains_key("policy") {
        handlers::bucket_policy::put_bucket_policy(State(store), Path(bucket), body).await
    } else if query.contains_key("encryption") {
        handlers::bucket_encryption::put_bucket_encryption(State(store), Path(bucket), body).await
    } else if query.contains_key("object-lock") {
        handlers::object_lock::put_object_lock_configuration(State(store), Path(bucket), body).await
    } else if query.contains_key("publicAccessBlock") {
        handlers::public_access::put_public_access_block(State(store), Path(bucket), body).await
    } else if query.contains_key("requestPayment") {
        handlers::request_payment::put_bucket_request_payment(State(store), Path(bucket), body)
            .await
    } else if query.contains_key("ownershipControls") {
        handlers::ownership::put_bucket_ownership_controls(State(store), Path(bucket), body).await
    } else if query.contains_key("tagging") {
        handlers::bucket_tagging::put_bucket_tagging(State(store), Path(bucket), body).await
    } else {
        handlers::bucket::make_bucket(State(store), Path(bucket)).await
    }
}

async fn delete_bucket_dispatch(
    State(store): State<Arc<dyn ObjectLayer>>,
    Extension(lifecycle): Extension<Arc<LifecycleSys>>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, S3Error> {
    if query.contains_key("lifecycle") {
        handlers::lifecycle::delete_bucket_lifecycle_configuration(
            State(store),
            Extension(lifecycle),
            Path(bucket),
        )
        .await
    } else if query.contains_key("replication") {
        handlers::replication::delete_bucket_replication(State(store), Path(bucket)).await
    } else if query.contains_key("cors") {
        handlers::cors::delete_bucket_cors(State(store), Path(bucket)).await
    } else if query.contains_key("website") {
        handlers::website::delete_bucket_website(State(store), Path(bucket)).await
    } else if query.contains_key("policy") {
        handlers::bucket_policy::delete_bucket_policy(State(store), Path(bucket)).await
    } else if query.contains_key("encryption") {
        handlers::bucket_encryption::delete_bucket_encryption(State(store), Path(bucket)).await
    } else if query.contains_key("publicAccessBlock") {
        handlers::public_access::delete_public_access_block(State(store), Path(bucket)).await
    } else if query.contains_key("ownershipControls") {
        handlers::ownership::delete_bucket_ownership_controls(State(store), Path(bucket)).await
    } else if query.contains_key("tagging") {
        handlers::bucket_tagging::delete_bucket_tagging(State(store), Path(bucket)).await
    } else {
        handlers::bucket::delete_bucket(State(store), Path(bucket)).await
    }
}

async fn put_object_dispatch(
    State(store): State<Arc<dyn ObjectLayer>>,
    Extension(notifications): Extension<Arc<NotificationSys>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    if query.contains_key("acl") {
        handlers::acl::put_object_acl(State(store), Path((bucket, key)), headers, body).await
    } else if query.contains_key("legal-hold") {
        handlers::object_lock::put_object_legal_hold(State(store), Path((bucket, key)), body).await
    } else if query.contains_key("retention") {
        handlers::object_lock::put_object_retention(State(store), Path((bucket, key)), body).await
    } else if query.contains_key("tagging") {
        handlers::tagging::put_object_tagging(State(store), Path((bucket, key)), body).await
    } else if query.contains_key("uploadId")
        && query.contains_key("partNumber")
        && headers.contains_key("x-amz-copy-source")
    {
        handlers::multipart::upload_part_copy(
            State(store),
            Path((bucket, key)),
            Query(query),
            headers,
        )
        .await
    } else if query.contains_key("uploadId") && query.contains_key("partNumber") {
        handlers::multipart::upload_part(State(store), Path((bucket, key)), Query(query), body)
            .await
    } else if headers.contains_key("x-amz-copy-source") {
        handlers::object::copy_object(
            State(store),
            Extension(notifications),
            Path((bucket, key)),
            headers,
        )
        .await
    } else {
        handlers::object::put_object(
            State(store),
            Extension(notifications),
            Path((bucket, key)),
            headers,
            body,
        )
        .await
    }
}

async fn post_bucket_dispatch(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    if query.contains_key("delete") {
        handlers::delete::delete_objects(State(store), Path(bucket), body).await
    } else {
        Err(S3Error::from(MaxioError::NotImplemented(
            "unsupported POST operation for bucket route".to_string(),
        )))
    }
}

async fn post_object_dispatch(
    State(store): State<Arc<dyn ObjectLayer>>,
    Extension(notifications): Extension<Arc<NotificationSys>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    if query.contains_key("uploads") {
        handlers::multipart::create_multipart_upload(State(store), Path((bucket, key)), headers)
            .await
    } else if query.contains_key("uploadId") {
        handlers::multipart::complete_multipart_upload(
            State(store),
            Extension(notifications),
            Path((bucket, key)),
            Query(query),
            headers,
            body,
        )
        .await
    } else {
        Err(S3Error::from(MaxioError::NotImplemented(
            "unsupported POST operation for object route".to_string(),
        )))
    }
}

async fn get_object_dispatch(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, S3Error> {
    if query.contains_key("acl") {
        handlers::acl::get_object_acl(State(store), Path((bucket, key))).await
    } else if query.contains_key("legal-hold") {
        handlers::object_lock::get_object_legal_hold(State(store), Path((bucket, key))).await
    } else if query.contains_key("retention") {
        handlers::object_lock::get_object_retention(State(store), Path((bucket, key))).await
    } else if query.contains_key("tagging") {
        handlers::tagging::get_object_tagging(State(store), Path((bucket, key))).await
    } else if query.contains_key("attributes") {
        handlers::object::get_object_attributes(
            State(store),
            Path((bucket, key)),
            Query(query),
            headers,
        )
        .await
    } else if query.contains_key("uploadId") {
        handlers::multipart::list_parts(State(store), Path((bucket, key)), Query(query)).await
    } else {
        handlers::object::get_object(State(store), Path((bucket, key)), Query(query), headers).await
    }
}

async fn delete_object_dispatch(
    State(store): State<Arc<dyn ObjectLayer>>,
    Extension(notifications): Extension<Arc<NotificationSys>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, S3Error> {
    if query.contains_key("tagging") {
        handlers::tagging::delete_object_tagging(State(store), Path((bucket, key))).await
    } else if query.contains_key("uploadId") {
        handlers::multipart::abort_multipart_upload(State(store), Path((bucket, key)), Query(query))
            .await
    } else {
        handlers::object::delete_object(
            State(store),
            Extension(notifications),
            Path((bucket, key)),
            Query(query),
        )
        .await
    }
}

pub fn s3_router(
    object_layer: Arc<dyn ObjectLayer>,
    credential_provider: Arc<dyn CredentialProvider>,
    iam: Arc<IAMSys>,
    notifications: Arc<NotificationSys>,
    lifecycle: Arc<LifecycleSys>,
    distributed: Arc<DistributedSys>,
) -> Router {
    let app: Router<Arc<dyn ObjectLayer>> = Router::<Arc<dyn ObjectLayer>>::new()
        .route("/minio/admin/v3/info", get(handlers::admin::server_info))
        .route("/minio/admin/v3/add-user", post(handlers::admin::add_user))
        .route(
            "/minio/admin/v3/remove-user",
            delete(handlers::admin::remove_user),
        )
        .route(
            "/minio/admin/v3/list-users",
            get(handlers::admin::list_users),
        )
        .route(
            "/minio/admin/v3/add-canned-policy",
            post(handlers::admin::add_canned_policy),
        )
        .route(
            "/minio/admin/v3/set-user-or-group-policy",
            put(handlers::admin::set_user_or_group_policy),
        )
        .route("/minio/health/live", get(handlers::health::health_live).head(handlers::health::health_live))
        .route(
            "/minio/health/cluster",
            get(handlers::health::health_cluster).head(handlers::health::health_cluster),
        )
        .route(
            "/minio/health/ready",
            get(handlers::health::health_ready).head(handlers::health::health_ready),
        )
        .route(
            "/minio/health/cluster/read",
            get(handlers::health::health_cluster_read).head(handlers::health::health_cluster_read),
        )
        .route("/", get(handlers::bucket::list_buckets))
        .route(
            "/{bucket}",
            put(put_bucket_dispatch)
                .post(post_bucket_dispatch)
                .head(handlers::bucket::head_bucket)
                .delete(delete_bucket_dispatch)
                .get(get_bucket_dispatch),
        )
        .route(
            "/{bucket}/{*key}",
            put(put_object_dispatch)
                .post(post_object_dispatch)
                .get(get_object_dispatch)
                .head(handlers::object::head_object)
                .delete(delete_object_dispatch),
        );

    app.layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .layer(ResponseHeadersLayer::new())
        .layer(AuthLayer::new(credential_provider))
        .layer(Extension(iam))
        .layer(Extension(notifications))
        .layer(Extension(lifecycle))
        .layer(Extension(distributed))
        .with_state(object_layer)
}
