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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename = "ServerSideEncryptionConfiguration")]
struct ServerSideEncryptionConfigurationXml {
    #[serde(rename = "Rule", default)]
    rules: Vec<ServerSideEncryptionRuleXml>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ServerSideEncryptionRuleXml {
    #[serde(rename = "ApplyServerSideEncryptionByDefault")]
    apply_server_side_encryption_by_default: ApplyServerSideEncryptionByDefaultXml,
    #[serde(rename = "BucketKeyEnabled", skip_serializing_if = "Option::is_none")]
    bucket_key_enabled: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ApplyServerSideEncryptionByDefaultXml {
    #[serde(rename = "SSEAlgorithm")]
    sse_algorithm: String,
    #[serde(rename = "KMSMasterKeyID", skip_serializing_if = "Option::is_none")]
    kms_master_key_id: Option<String>,
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

fn encryption_config_key(bucket: &str) -> String {
    format!("buckets/{bucket}/encryption.xml")
}

fn validate_encryption_config(
    config: &ServerSideEncryptionConfigurationXml,
) -> Result<(), MaxioError> {
    if config.rules.is_empty() {
        return Err(MaxioError::InvalidArgument(
            "encryption configuration must include at least one Rule".to_string(),
        ));
    }

    for (index, rule) in config.rules.iter().enumerate() {
        match rule
            .apply_server_side_encryption_by_default
            .sse_algorithm
            .as_str()
        {
            "AES256" | "aws:kms" => {}
            other => {
                return Err(MaxioError::InvalidArgument(format!(
                    "encryption rule {} has unsupported SSEAlgorithm: {other}",
                    index + 1
                )));
            }
        }
    }

    Ok(())
}

async fn ensure_internal_bucket(store: &Arc<dyn ObjectLayer>) -> Result<(), MaxioError> {
    match store.make_bucket(INTERNAL_CONFIG_BUCKET).await {
        Ok(()) | Err(MaxioError::BucketAlreadyExists(_)) => Ok(()),
        Err(err) => Err(err),
    }
}

pub async fn get_bucket_encryption(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = encryption_config_key(&bucket);
    let (_, body) = store
        .get_object(INTERNAL_CONFIG_BUCKET, &key, None)
        .await
        .map_err(|err| match err {
            MaxioError::ObjectNotFound { .. } => {
                MaxioError::ServerSideEncryptionConfigurationNotFound(bucket.clone())
            }
            other => other,
        })?;

    let body_str = std::str::from_utf8(&body).map_err(|err| {
        MaxioError::InternalError(format!(
            "stored encryption config is not valid UTF-8: {err}"
        ))
    })?;
    let config: ServerSideEncryptionConfigurationXml = xml_from_str(body_str).map_err(|err| {
        MaxioError::InternalError(format!("stored encryption config is invalid XML: {err}"))
    })?;

    xml_response(StatusCode::OK, &config)
}

pub async fn put_bucket_encryption(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let config: ServerSideEncryptionConfigurationXml = xml_from_str(body_str).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid encryption xml body: {err}"))
    })?;
    validate_encryption_config(&config)?;

    let xml = xml_to_string(&config).map_err(|err| {
        MaxioError::InternalError(format!("failed to serialize encryption xml body: {err}"))
    })?;

    let key = encryption_config_key(&bucket);
    ensure_internal_bucket(&store).await?;
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

pub async fn delete_bucket_encryption(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = encryption_config_key(&bucket);
    match store.delete_object(INTERNAL_CONFIG_BUCKET, &key).await {
        Ok(()) | Err(MaxioError::ObjectNotFound { .. }) => {
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Err(err) => Err(S3Error::from(err)),
    }
}
