use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use maxio_common::error::MaxioError;
use maxio_storage::traits::ObjectLayer;
use quick_xml::{de::from_str as xml_from_str, se::to_string as xml_to_string};
use serde::{Deserialize, Serialize};

use crate::error::S3Error;

type S3Result = Result<Response, S3Error>;

const INTERNAL_CONFIG_BUCKET: &str = ".minio.sys";
const LEGAL_HOLD_METADATA_KEY: &str = "x-amz-object-lock-legal-hold";
const RETENTION_MODE_METADATA_KEY: &str = "x-amz-object-lock-mode";
const RETENTION_DATE_METADATA_KEY: &str = "x-amz-object-lock-retain-until-date";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "ObjectLockConfiguration")]
struct ObjectLockConfigurationXml {
    #[serde(rename = "ObjectLockEnabled", skip_serializing_if = "Option::is_none")]
    object_lock_enabled: Option<String>,
    #[serde(rename = "Rule", skip_serializing_if = "Option::is_none")]
    rule: Option<ObjectLockRuleXml>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectLockRuleXml {
    #[serde(rename = "DefaultRetention", skip_serializing_if = "Option::is_none")]
    default_retention: Option<DefaultRetentionXml>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DefaultRetentionXml {
    #[serde(rename = "Mode")]
    mode: String,
    #[serde(rename = "Days", skip_serializing_if = "Option::is_none")]
    days: Option<i64>,
    #[serde(rename = "Years", skip_serializing_if = "Option::is_none")]
    years: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "LegalHold")]
struct LegalHoldXml {
    #[serde(rename = "Status")]
    status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "Retention")]
struct RetentionXml {
    #[serde(rename = "Mode")]
    mode: String,
    #[serde(rename = "RetainUntilDate")]
    retain_until_date: String,
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

fn object_lock_config_key(bucket: &str) -> String {
    format!("buckets/{bucket}/object-lock.xml")
}

fn parse_retention_mode(mode: &str) -> Result<&'static str, MaxioError> {
    match mode {
        "GOVERNANCE" => Ok("GOVERNANCE"),
        "COMPLIANCE" => Ok("COMPLIANCE"),
        other => Err(MaxioError::InvalidArgument(format!(
            "invalid retention mode: {other}"
        ))),
    }
}

fn parse_legal_hold_status(status: &str) -> Result<&'static str, MaxioError> {
    match status {
        "ON" => Ok("ON"),
        "OFF" => Ok("OFF"),
        other => Err(MaxioError::InvalidArgument(format!(
            "invalid legal hold status: {other}"
        ))),
    }
}

fn parse_retain_until_date(value: &str) -> Result<String, MaxioError> {
    DateTime::parse_from_rfc3339(value)
        .map(|parsed| parsed.with_timezone(&Utc).to_rfc3339())
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid RetainUntilDate: {err}")))
}

fn validate_object_lock_configuration(
    config: &ObjectLockConfigurationXml,
) -> Result<(), MaxioError> {
    match config.object_lock_enabled.as_deref() {
        Some("Enabled") => {}
        Some(other) => {
            return Err(MaxioError::InvalidArgument(format!(
                "invalid ObjectLockEnabled value: {other}"
            )));
        }
        None => {
            return Err(MaxioError::InvalidArgument(
                "missing ObjectLockEnabled in object lock configuration".to_string(),
            ));
        }
    }

    if let Some(default_retention) = config
        .rule
        .as_ref()
        .and_then(|rule| rule.default_retention.as_ref())
    {
        parse_retention_mode(default_retention.mode.as_str())?;
        match (default_retention.days, default_retention.years) {
            (Some(days), None) if days > 0 => {}
            (None, Some(years)) if years > 0 => {}
            (Some(_), Some(_)) => {
                return Err(MaxioError::InvalidArgument(
                    "DefaultRetention must include either Days or Years, not both".to_string(),
                ));
            }
            (None, None) => {
                return Err(MaxioError::InvalidArgument(
                    "DefaultRetention must include Days or Years".to_string(),
                ));
            }
            (Some(_), None) => {
                return Err(MaxioError::InvalidArgument(
                    "DefaultRetention Days must be greater than zero".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(MaxioError::InvalidArgument(
                    "DefaultRetention Years must be greater than zero".to_string(),
                ));
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

pub async fn get_object_lock_configuration(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = object_lock_config_key(&bucket);
    let (_, body) = store
        .get_object(INTERNAL_CONFIG_BUCKET, &key, None)
        .await
        .map_err(|err| match err {
            MaxioError::ObjectNotFound { .. } | MaxioError::BucketNotFound(_) => {
                MaxioError::ObjectLockConfigurationNotFound(bucket.clone())
            }
            other => other,
        })?;

    let body_str = std::str::from_utf8(&body).map_err(|err| {
        MaxioError::InternalError(format!(
            "stored object lock configuration is not valid UTF-8: {err}"
        ))
    })?;
    let config: ObjectLockConfigurationXml = xml_from_str(body_str).map_err(|err| {
        MaxioError::InternalError(format!(
            "stored object lock configuration is invalid XML: {err}"
        ))
    })?;

    xml_response(StatusCode::OK, &config)
}

pub async fn put_object_lock_configuration(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let config: ObjectLockConfigurationXml = xml_from_str(body_str).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid object lock xml body: {err}"))
    })?;
    validate_object_lock_configuration(&config)?;

    let xml = xml_to_string(&config).map_err(|err| {
        MaxioError::InternalError(format!(
            "failed to serialize object lock xml body for storage: {err}"
        ))
    })?;

    let key = object_lock_config_key(&bucket);
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

pub async fn get_object_legal_hold(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
) -> S3Result {
    let info = store.get_object_info(&bucket, &key, None).await?;
    let status = parse_legal_hold_status(
        info.metadata
            .get(LEGAL_HOLD_METADATA_KEY)
            .map(String::as_str)
            .unwrap_or("OFF"),
    )
    .map_err(|err| {
        MaxioError::InternalError(format!(
            "stored object legal hold metadata is invalid: {err}"
        ))
    })?;

    let payload = LegalHoldXml {
        status: status.to_string(),
    };
    xml_response(StatusCode::OK, &payload)
}

pub async fn put_object_legal_hold(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> S3Result {
    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let payload: LegalHoldXml = xml_from_str(body_str).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid legal hold xml body: {err}"))
    })?;
    let status = parse_legal_hold_status(payload.status.as_str())?;

    let (info, data) = store.get_object(&bucket, &key, None).await?;
    let mut metadata = info.metadata;
    metadata.insert(LEGAL_HOLD_METADATA_KEY.to_string(), status.to_string());
    store
        .put_object(
            &bucket,
            &key,
            data,
            Some(&info.content_type),
            metadata,
            None,
        )
        .await?;

    Ok(StatusCode::OK.into_response())
}

pub async fn get_object_retention(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
) -> S3Result {
    let info = store.get_object_info(&bucket, &key, None).await?;
    let mode = info
        .metadata
        .get(RETENTION_MODE_METADATA_KEY)
        .ok_or_else(|| {
            MaxioError::InvalidArgument("object retention mode is not set".to_string())
        })?;
    let retain_until_date = info
        .metadata
        .get(RETENTION_DATE_METADATA_KEY)
        .ok_or_else(|| {
            MaxioError::InvalidArgument("object retention retain-until-date is not set".to_string())
        })?;

    let mode = parse_retention_mode(mode.as_str())?.to_string();
    let retain_until_date = parse_retain_until_date(retain_until_date.as_str())?;
    let payload = RetentionXml {
        mode,
        retain_until_date,
    };

    xml_response(StatusCode::OK, &payload)
}

pub async fn put_object_retention(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> S3Result {
    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let payload: RetentionXml = xml_from_str(body_str)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid retention xml body: {err}")))?;

    let mode = parse_retention_mode(payload.mode.as_str())?;
    let retain_until_date = parse_retain_until_date(payload.retain_until_date.as_str())?;

    let (info, data) = store.get_object(&bucket, &key, None).await?;
    let mut metadata = info.metadata;
    metadata.insert(RETENTION_MODE_METADATA_KEY.to_string(), mode.to_string());
    metadata.insert(
        RETENTION_DATE_METADATA_KEY.to_string(),
        retain_until_date.to_string(),
    );
    store
        .put_object(
            &bucket,
            &key,
            data,
            Some(&info.content_type),
            metadata,
            None,
        )
        .await?;

    Ok(StatusCode::OK.into_response())
}
