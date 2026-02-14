use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use maxio_common::error::MaxioError;
use maxio_distributed::ReplicationConfig;
use maxio_storage::traits::ObjectLayer;

use crate::error::S3Error;

type S3Result = Result<Response, S3Error>;

const INTERNAL_CONFIG_BUCKET: &str = ".minio.sys";

fn replication_config_key(bucket: &str) -> String {
    format!("buckets/{bucket}/replication/config.xml")
}

fn xml_response(status: StatusCode, xml: String) -> S3Result {
    let body = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{xml}");
    Ok((status, [("Content-Type", "application/xml")], body).into_response())
}

fn validate_replication_config(config: &ReplicationConfig) -> Result<(), MaxioError> {
    if config.role.as_deref().is_none_or(|role| role.trim().is_empty()) {
        return Err(MaxioError::InvalidArgument(
            "replication Role is required".to_string(),
        ));
    }
    if config.rules.is_empty() {
        return Err(MaxioError::InvalidArgument(
            "replication configuration must include at least one Rule".to_string(),
        ));
    }

    let mut priorities = std::collections::HashSet::new();
    for (index, rule) in config.rules.iter().enumerate() {
        if rule.destination.bucket.trim().is_empty() {
            return Err(MaxioError::InvalidArgument(format!(
                "replication rule {} has empty destination bucket",
                index + 1
            )));
        }

        if let Some(priority) = rule.priority {
            if !priorities.insert(priority) {
                return Err(MaxioError::InvalidArgument(format!(
                    "duplicate replication rule priority: {priority}"
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

pub async fn put_bucket_replication(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let config = ReplicationConfig::from_xml(body_str)?;
    validate_replication_config(&config)?;

    let xml = config.to_xml()?;
    let key = replication_config_key(&bucket);
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

pub async fn get_bucket_replication(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = replication_config_key(&bucket);
    let (_, body) = store
        .get_object(INTERNAL_CONFIG_BUCKET, &key, None)
        .await
        .map_err(|err| match err {
            MaxioError::ObjectNotFound { .. } => MaxioError::InvalidArgument(
                "replication configuration not found for bucket".to_string(),
            ),
            other => other,
        })?;

    let config_body = std::str::from_utf8(&body).map_err(|err| {
        MaxioError::InternalError(format!("stored replication config is not valid UTF-8: {err}"))
    })?;
    let config = ReplicationConfig::from_xml(config_body)?;
    let xml = config.to_xml()?;
    xml_response(StatusCode::OK, xml)
}

pub async fn delete_bucket_replication(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = replication_config_key(&bucket);
    match store.delete_object(INTERNAL_CONFIG_BUCKET, &key).await {
        Ok(()) | Err(MaxioError::ObjectNotFound { .. }) => {
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Err(err) => Err(S3Error::from(err)),
    }
}
