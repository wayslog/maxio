use std::sync::Arc;

use axum::{
    Extension,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use maxio_common::error::MaxioError;
use maxio_lifecycle::types::LifecycleConfiguration;
use maxio_lifecycle::LifecycleSys;
use maxio_storage::traits::ObjectLayer;
use quick_xml::{de::from_str as xml_from_str, se::to_string as xml_to_string};
use serde::Serialize;

use crate::error::S3Error;

type S3Result = Result<Response, S3Error>;

fn xml_response<T: Serialize>(status: StatusCode, payload: &T) -> S3Result {
    let xml = xml_to_string(payload).map_err(|err| {
        S3Error::from(MaxioError::InternalError(format!(
            "failed to serialize xml response: {err}"
        )))
    })?;
    let body = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{xml}");
    Ok((status, [("Content-Type", "application/xml")], body).into_response())
}

pub async fn get_bucket_lifecycle_configuration(
    State(store): State<Arc<dyn ObjectLayer>>,
    Extension(lifecycle): Extension<Arc<LifecycleSys>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;
    let config = lifecycle
        .get_config(&bucket)
        .await?
        .unwrap_or_default();
    xml_response(StatusCode::OK, &config)
}

pub async fn put_bucket_lifecycle_configuration(
    State(store): State<Arc<dyn ObjectLayer>>,
    Extension(lifecycle): Extension<Arc<LifecycleSys>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;
    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let config: LifecycleConfiguration = xml_from_str(body_str)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid lifecycle xml body: {err}")))?;
    lifecycle.set_config(&bucket, config).await?;
    Ok(StatusCode::OK.into_response())
}

pub async fn delete_bucket_lifecycle_configuration(
    State(store): State<Arc<dyn ObjectLayer>>,
    Extension(lifecycle): Extension<Arc<LifecycleSys>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;
    lifecycle.delete_config(&bucket).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
