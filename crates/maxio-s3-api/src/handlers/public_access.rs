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

fn public_access_block_config_key(bucket: &str) -> String {
    format!("buckets/{bucket}/public-access-block.xml")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "PublicAccessBlockConfiguration")]
pub struct PublicAccessBlockConfiguration {
    #[serde(rename = "BlockPublicAcls")]
    pub block_public_acls: bool,
    #[serde(rename = "IgnorePublicAcls")]
    pub ignore_public_acls: bool,
    #[serde(rename = "BlockPublicPolicy")]
    pub block_public_policy: bool,
    #[serde(rename = "RestrictPublicBuckets")]
    pub restrict_public_buckets: bool,
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

async fn ensure_internal_bucket(store: &Arc<dyn ObjectLayer>) -> Result<(), MaxioError> {
    match store.make_bucket(INTERNAL_CONFIG_BUCKET).await {
        Ok(()) | Err(MaxioError::BucketAlreadyExists(_)) => Ok(()),
        Err(err) => Err(err),
    }
}

pub async fn get_public_access_block(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = public_access_block_config_key(&bucket);
    let (_, body) = store
        .get_object(INTERNAL_CONFIG_BUCKET, &key, None)
        .await
        .map_err(|err| match err {
            MaxioError::ObjectNotFound { .. } | MaxioError::BucketNotFound(_) => {
                MaxioError::NoSuchPublicAccessBlockConfiguration(bucket.clone())
            }
            other => other,
        })?;

    let body_str = std::str::from_utf8(&body).map_err(|err| {
        MaxioError::InternalError(format!(
            "stored public access block config is not valid UTF-8: {err}"
        ))
    })?;
    let config: PublicAccessBlockConfiguration = xml_from_str(body_str).map_err(|err| {
        MaxioError::InternalError(format!(
            "stored public access block config is invalid XML: {err}"
        ))
    })?;

    xml_response(StatusCode::OK, &config)
}

pub async fn put_public_access_block(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let config: PublicAccessBlockConfiguration = xml_from_str(body_str).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid public access block xml body: {err}"))
    })?;
    let xml = xml_to_string(&config).map_err(|err| {
        MaxioError::InternalError(format!(
            "failed to serialize public access block xml for storage: {err}"
        ))
    })?;

    ensure_internal_bucket(&store).await?;
    let key = public_access_block_config_key(&bucket);
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

pub async fn delete_public_access_block(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = public_access_block_config_key(&bucket);
    match store.delete_object(INTERNAL_CONFIG_BUCKET, &key).await {
        Ok(()) | Err(MaxioError::ObjectNotFound { .. }) | Err(MaxioError::BucketNotFound(_)) => {
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Err(err) => Err(S3Error::from(err)),
    }
}
