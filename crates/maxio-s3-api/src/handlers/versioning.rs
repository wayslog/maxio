use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use maxio_common::error::MaxioError;
use maxio_storage::traits::{ObjectLayer, ObjectVersion, VersioningState};
use quick_xml::{de::from_str as xml_from_str, se::to_string as xml_to_string};
use serde::{Deserialize, Serialize};

use crate::error::S3Error;

type S3Result = Result<Response, S3Error>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename = "VersioningConfiguration")]
struct VersioningConfigurationXml {
    #[serde(rename = "Status", skip_serializing_if = "Option::is_none")]
    status: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename = "ListVersionsResult")]
struct ListVersionsResultXml {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "MaxKeys")]
    max_keys: i32,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "Version", default)]
    versions: Vec<VersionXml>,
    #[serde(rename = "DeleteMarker", default)]
    delete_markers: Vec<DeleteMarkerXml>,
}

#[derive(Debug, Serialize)]
struct VersionXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: String,
    #[serde(rename = "IsLatest")]
    is_latest: bool,
    #[serde(rename = "LastModified")]
    last_modified: String,
    #[serde(rename = "ETag")]
    etag: String,
    #[serde(rename = "Size")]
    size: i64,
    #[serde(rename = "StorageClass")]
    storage_class: String,
}

#[derive(Debug, Serialize)]
struct DeleteMarkerXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: String,
    #[serde(rename = "IsLatest")]
    is_latest: bool,
    #[serde(rename = "LastModified")]
    last_modified: String,
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

fn parse_status(status: Option<&str>) -> Result<VersioningState, MaxioError> {
    match status {
        Some("Enabled") => Ok(VersioningState::Enabled),
        Some("Suspended") => Ok(VersioningState::Suspended),
        Some(other) => Err(MaxioError::InvalidArgument(format!(
            "invalid versioning status: {other}"
        ))),
        None => Err(MaxioError::InvalidArgument(
            "missing versioning status".to_string(),
        )),
    }
}

fn format_status(state: VersioningState) -> Option<String> {
    match state {
        VersioningState::Enabled => Some("Enabled".to_string()),
        VersioningState::Suspended => Some("Suspended".to_string()),
        VersioningState::Unversioned => None,
    }
}

fn parse_max_keys(query: &HashMap<String, String>) -> i32 {
    query
        .get("max-keys")
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1000)
}

fn split_versions(items: Vec<ObjectVersion>) -> (Vec<VersionXml>, Vec<DeleteMarkerXml>) {
    let mut versions = Vec::new();
    let mut delete_markers = Vec::new();

    for item in items {
        if item.is_delete_marker {
            delete_markers.push(DeleteMarkerXml {
                key: item.key,
                version_id: item.version_id,
                is_latest: item.is_latest,
                last_modified: item.last_modified.to_rfc3339(),
            });
        } else {
            versions.push(VersionXml {
                key: item.key,
                version_id: item.version_id,
                is_latest: item.is_latest,
                last_modified: item.last_modified.to_rfc3339(),
                etag: item.etag.unwrap_or_default(),
                size: item.size,
                storage_class: "STANDARD".to_string(),
            });
        }
    }

    (versions, delete_markers)
}

pub async fn get_bucket_versioning(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    let state = store.get_bucket_versioning(&bucket).await?;
    let payload = VersioningConfigurationXml {
        status: format_status(state),
    };
    xml_response(StatusCode::OK, &payload)
}

pub async fn put_bucket_versioning(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let payload: VersioningConfigurationXml = xml_from_str(body_str)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid versioning xml body: {err}")))?;
    let state = parse_status(payload.status.as_deref())?;
    store.set_bucket_versioning(&bucket, state).await?;
    Ok(StatusCode::OK.into_response())
}

pub async fn list_object_versions(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> S3Result {
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let max_keys = parse_max_keys(&query);
    let items = store.list_object_versions(&bucket, &prefix, max_keys).await?;
    let (versions, delete_markers) = split_versions(items);
    let payload = ListVersionsResultXml {
        name: bucket,
        prefix,
        max_keys,
        is_truncated: false,
        versions,
        delete_markers,
    };
    xml_response(StatusCode::OK, &payload)
}
