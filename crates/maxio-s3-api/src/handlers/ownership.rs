use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use maxio_common::error::MaxioError;
use maxio_storage::traits::ObjectLayer;
use quick_xml::{de::from_str as xml_from_str, se::to_string as xml_to_string};
use serde::{Deserialize, Serialize};

use crate::error::S3Error;

type S3Result = Result<Response, S3Error>;

const INTERNAL_CONFIG_BUCKET: &str = ".minio.sys";

fn ownership_controls_key(bucket: &str) -> String {
    format!("buckets/{bucket}/ownership-controls.xml")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "OwnershipControls")]
pub struct OwnershipControls {
    #[serde(rename = "Rule")]
    pub rule: OwnershipRule,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnershipRule {
    #[serde(rename = "ObjectOwnership")]
    pub object_ownership: String,
}

fn xml_response<T: Serialize>(status: StatusCode, payload: &T) -> S3Result {
    let xml = xml_to_string(payload).map_err(|err| {
        S3Error::from(MaxioError::InternalError(format!(
            "failed to serialize xml response: {err}"
        )))
    })?;
    let body = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{xml}");
    Ok((status, [("Content-Type", "application/xml")], body).into_response())
}

fn validate_object_ownership(value: &str) -> Result<(), MaxioError> {
    match value {
        "BucketOwnerPreferred" | "ObjectWriter" | "BucketOwnerEnforced" => Ok(()),
        other => Err(MaxioError::InvalidArgument(format!(
            "invalid ObjectOwnership value: {other}"
        ))),
    }
}

async fn ensure_internal_bucket(store: &Arc<dyn ObjectLayer>) -> Result<(), MaxioError> {
    match store.make_bucket(INTERNAL_CONFIG_BUCKET).await {
        Ok(()) | Err(MaxioError::BucketAlreadyExists(_)) => Ok(()),
        Err(err) => Err(err),
    }
}

pub async fn get_bucket_ownership_controls(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = ownership_controls_key(&bucket);
    let (_, body) = store
        .get_object(INTERNAL_CONFIG_BUCKET, &key, None)
        .await
        .map_err(|err| match err {
            MaxioError::ObjectNotFound { .. } | MaxioError::BucketNotFound(_) => {
                MaxioError::OwnershipControlsNotFound(bucket.clone())
            }
            other => other,
        })?;

    let body_str = std::str::from_utf8(&body).map_err(|err| {
        MaxioError::InternalError(format!(
            "stored ownership controls config is not valid UTF-8: {err}"
        ))
    })?;
    let config: OwnershipControls = xml_from_str(body_str).map_err(|err| {
        MaxioError::InternalError(format!(
            "stored ownership controls config is invalid XML: {err}"
        ))
    })?;
    validate_object_ownership(config.rule.object_ownership.as_str())?;

    xml_response(StatusCode::OK, &config)
}

pub async fn put_bucket_ownership_controls(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let config: OwnershipControls = xml_from_str(body_str).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid ownership controls xml body: {err}"))
    })?;
    validate_object_ownership(config.rule.object_ownership.as_str())?;

    let xml = xml_to_string(&config).map_err(|err| {
        MaxioError::InternalError(format!(
            "failed to serialize ownership controls xml for storage: {err}"
        ))
    })?;

    ensure_internal_bucket(&store).await?;
    let key = ownership_controls_key(&bucket);
    store
        .put_object(
            INTERNAL_CONFIG_BUCKET,
            &key,
            Bytes::from(xml),
            Some("application/xml"),
            HashMap::new(),
            None,
        )
        .await?;

    Ok(StatusCode::OK.into_response())
}

pub async fn delete_bucket_ownership_controls(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = ownership_controls_key(&bucket);
    match store.delete_object(INTERNAL_CONFIG_BUCKET, &key).await {
        Ok(()) | Err(MaxioError::ObjectNotFound { .. }) | Err(MaxioError::BucketNotFound(_)) => {
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Err(err) => Err(S3Error::from(err)),
    }
}
