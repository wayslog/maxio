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

fn logging_config_key(bucket: &str) -> String {
    format!("buckets/{bucket}/logging.xml")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "BucketLoggingStatus")]
struct BucketLoggingStatusXml {
    #[serde(rename = "LoggingEnabled", skip_serializing_if = "Option::is_none")]
    logging_enabled: Option<LoggingEnabledXml>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LoggingEnabledXml {
    #[serde(rename = "TargetBucket", skip_serializing_if = "Option::is_none")]
    target_bucket: Option<String>,
    #[serde(rename = "TargetPrefix", skip_serializing_if = "Option::is_none")]
    target_prefix: Option<String>,
    #[serde(rename = "TargetGrants", skip_serializing_if = "Option::is_none")]
    target_grants: Option<TargetGrantsXml>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TargetGrantsXml {
    #[serde(rename = "Grant", default)]
    grants: Vec<GrantXml>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GrantXml {
    #[serde(rename = "Grantee")]
    grantee: GranteeXml,
    #[serde(rename = "Permission")]
    permission: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GranteeXml {
    #[serde(rename = "@xsi:type", alias = "Type")]
    type_name: String,
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(rename = "DisplayName", skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(rename = "URI", skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
    #[serde(rename = "EmailAddress", skip_serializing_if = "Option::is_none")]
    email_address: Option<String>,
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

fn validate_bucket_logging(config: &BucketLoggingStatusXml) -> Result<(), MaxioError> {
    if let Some(logging) = &config.logging_enabled {
        let has_target_bucket = logging
            .target_bucket
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());
        if !has_target_bucket {
            return Err(MaxioError::InvalidArgument(
                "LoggingEnabled requires a non-empty TargetBucket".to_string(),
            ));
        }

        if let Some(target_grants) = &logging.target_grants {
            for (index, grant) in target_grants.grants.iter().enumerate() {
                if grant.permission.trim().is_empty() {
                    return Err(MaxioError::InvalidArgument(format!(
                        "TargetGrants Grant {} has an empty Permission",
                        index + 1
                    )));
                }
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

pub async fn get_bucket_logging(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = logging_config_key(&bucket);
    let config = match store.get_object(INTERNAL_CONFIG_BUCKET, &key, None).await {
        Ok((_, body)) => {
            let body_str = std::str::from_utf8(&body).map_err(|err| {
                MaxioError::InternalError(format!(
                    "stored bucket logging config is not valid UTF-8: {err}"
                ))
            })?;
            xml_from_str(body_str).map_err(|err| {
                MaxioError::InternalError(format!(
                    "stored bucket logging config is invalid XML: {err}"
                ))
            })?
        }
        Err(MaxioError::ObjectNotFound { .. }) | Err(MaxioError::BucketNotFound(_)) => {
            BucketLoggingStatusXml {
                logging_enabled: Some(LoggingEnabledXml::default()),
            }
        }
        Err(err) => return Err(S3Error::from(err)),
    };

    xml_response(StatusCode::OK, &config)
}

pub async fn put_bucket_logging(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let config: BucketLoggingStatusXml = xml_from_str(body_str)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid logging xml body: {err}")))?;
    validate_bucket_logging(&config)?;

    let xml = xml_to_string(&config).map_err(|err| {
        MaxioError::InternalError(format!(
            "failed to serialize bucket logging xml body: {err}"
        ))
    })?;

    ensure_internal_bucket(&store).await?;
    let key = logging_config_key(&bucket);
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
