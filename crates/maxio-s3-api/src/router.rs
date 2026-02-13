use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    response::Response,
    routing::{get, put},
    Router,
};
use maxio_auth::{credentials::CredentialProvider, middleware::AuthLayer};
use maxio_storage::traits::ObjectLayer;

use crate::handlers;

use crate::error::S3Error;

async fn get_bucket_dispatch(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, S3Error> {
    if query.contains_key("location") {
        handlers::bucket::get_bucket_location(State(store), Path(bucket)).await
    } else if query.get("list-type").is_some_and(|v| v == "2") {
        handlers::object::list_objects_v2(State(store), Path(bucket), Query(query)).await
    } else {
        handlers::object::list_objects_v1(State(store), Path(bucket), Query(query)).await
    }
}

pub fn s3_router(
    object_layer: Arc<dyn ObjectLayer>,
    credential_provider: Arc<dyn CredentialProvider>,
) -> Router {
    let app: Router<Arc<dyn ObjectLayer>> = Router::<Arc<dyn ObjectLayer>>::new()
        .route("/", get(handlers::bucket::list_buckets))
        .route(
            "/{bucket}",
            put(handlers::bucket::make_bucket)
                .head(handlers::bucket::head_bucket)
                .delete(handlers::bucket::delete_bucket)
                .get(get_bucket_dispatch),
        )
        .route(
            "/{bucket}/{*key}",
            put(handlers::object::put_object)
                .get(handlers::object::get_object)
                .head(handlers::object::head_object)
                .delete(handlers::object::delete_object),
        );

    app.layer(AuthLayer::new(credential_provider))
        .with_state(object_layer)
}
