use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{
        header::{CONTENT_TYPE, ETAG, HOST},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use maxio_common::error::MaxioError;
use maxio_storage::traits::{CompletePart, MultipartUploadInfo, ObjectLayer, PartInfo};
use quick_xml::{de::from_str as xml_from_str, se::to_string as xml_to_string};
use serde::{Deserialize, Serialize};

use crate::error::S3Error;

type S3Result = Result<Response, S3Error>;

#[derive(Debug, Serialize)]
#[serde(rename = "InitiateMultipartUploadResult")]
struct InitiateMultipartUploadResultXml {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "UploadId")]
    upload_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "CompleteMultipartUpload")]
struct CompleteMultipartUploadXml {
    #[serde(rename = "Part", default)]
    parts: Vec<CompletePartXml>,
}

#[derive(Debug, Deserialize)]
struct CompletePartXml {
    #[serde(rename = "PartNumber")]
    part_number: i32,
    #[serde(rename = "ETag")]
    etag: String,
}

#[derive(Debug, Serialize)]
#[serde(rename = "CompleteMultipartUploadResult")]
struct CompleteMultipartUploadResultXml {
    #[serde(rename = "Location")]
    location: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "ETag")]
    etag: String,
}

#[derive(Debug, Serialize)]
#[serde(rename = "ListPartsResult")]
struct ListPartsResultXml {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "UploadId")]
    upload_id: String,
    #[serde(rename = "Part", default)]
    parts: Vec<PartXml>,
}

#[derive(Debug, Serialize)]
struct PartXml {
    #[serde(rename = "PartNumber")]
    part_number: i32,
    #[serde(rename = "LastModified")]
    last_modified: String,
    #[serde(rename = "ETag")]
    etag: String,
    #[serde(rename = "Size")]
    size: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename = "ListMultipartUploadsResult")]
struct ListMultipartUploadsResultXml {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "Upload", default)]
    uploads: Vec<MultipartUploadXml>,
}

#[derive(Debug, Serialize)]
struct MultipartUploadXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "UploadId")]
    upload_id: String,
    #[serde(rename = "Initiated")]
    initiated: String,
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

fn quoted_etag(etag: &str) -> String {
    if etag.starts_with('"') && etag.ends_with('"') {
        etag.to_string()
    } else {
        format!("\"{etag}\"")
    }
}

fn extract_put_metadata(headers: &HeaderMap) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    for (name, value) in headers {
        let name = name.as_str();
        if let Some(meta_key) = name.strip_prefix("x-amz-meta-") {
            if let Ok(meta_value) = value.to_str() {
                metadata.insert(meta_key.to_string(), meta_value.to_string());
            }
        }
    }
    metadata
}

fn parse_upload_id(query: &HashMap<String, String>) -> Result<&str, MaxioError> {
    query
        .get("uploadId")
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| MaxioError::InvalidArgument("missing uploadId".to_string()))
}

fn parse_part_number(query: &HashMap<String, String>) -> Result<i32, MaxioError> {
    query
        .get("partNumber")
        .ok_or_else(|| MaxioError::InvalidArgument("missing partNumber".to_string()))?
        .parse::<i32>()
        .map_err(|_| MaxioError::InvalidArgument("invalid partNumber".to_string()))
}

fn parse_complete_parts(payload: CompleteMultipartUploadXml) -> Vec<CompletePart> {
    payload
        .parts
        .into_iter()
        .map(|part| CompletePart {
            part_number: part.part_number,
            etag: part.etag,
        })
        .collect()
}

fn map_parts(parts: Vec<PartInfo>) -> Vec<PartXml> {
    parts
        .into_iter()
        .map(|part| PartXml {
            part_number: part.part_number,
            last_modified: part.last_modified.to_rfc3339(),
            etag: quoted_etag(&part.etag),
            size: part.size,
        })
        .collect()
}

fn map_uploads(uploads: Vec<MultipartUploadInfo>) -> Vec<MultipartUploadXml> {
    uploads
        .into_iter()
        .map(|upload| MultipartUploadXml {
            key: upload.key,
            upload_id: upload.upload_id,
            initiated: upload.initiated.to_rfc3339(),
        })
        .collect()
}

pub async fn create_multipart_upload(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> S3Result {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    let metadata = extract_put_metadata(&headers);
    let upload_id = store
        .create_multipart_upload(&bucket, &key, content_type, metadata)
        .await?;

    let payload = InitiateMultipartUploadResultXml {
        bucket,
        key,
        upload_id,
    };
    xml_response(StatusCode::OK, &payload)
}

pub async fn upload_part(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> S3Result {
    let upload_id = parse_upload_id(&query)?;
    let part_number = parse_part_number(&query)?;
    let etag = store
        .upload_part(&bucket, &key, upload_id, part_number, body)
        .await?;

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        ETAG,
        HeaderValue::from_str(&quoted_etag(&etag)).map_err(|err| {
            MaxioError::InvalidArgument(format!("invalid etag header value: {err}"))
        })?,
    );
    Ok((StatusCode::OK, response_headers).into_response())
}

pub async fn complete_multipart_upload(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> S3Result {
    let upload_id = parse_upload_id(&query)?;
    let body_str = std::str::from_utf8(&body).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid complete multipart xml body encoding: {err}"))
    })?;
    let payload: CompleteMultipartUploadXml = xml_from_str(body_str).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid complete multipart xml body: {err}"))
    })?;
    let parts = parse_complete_parts(payload);

    let info = store
        .complete_multipart_upload(&bucket, &key, upload_id, parts)
        .await?;

    let host = headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost");
    let payload = CompleteMultipartUploadResultXml {
        location: format!("http://{host}/{bucket}/{key}"),
        bucket,
        key,
        etag: quoted_etag(&info.etag),
    };
    xml_response(StatusCode::OK, &payload)
}

pub async fn abort_multipart_upload(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> S3Result {
    let upload_id = parse_upload_id(&query)?;
    store.abort_multipart_upload(&bucket, &key, upload_id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

pub async fn list_parts(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> S3Result {
    let upload_id = parse_upload_id(&query)?;
    let parts = store.list_parts(&bucket, &key, upload_id).await?;
    let payload = ListPartsResultXml {
        bucket,
        key,
        upload_id: upload_id.to_string(),
        parts: map_parts(parts),
    };
    xml_response(StatusCode::OK, &payload)
}

pub async fn list_multipart_uploads(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> S3Result {
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let uploads = store.list_multipart_uploads(&bucket, &prefix).await?;
    let payload = ListMultipartUploadsResultXml {
        bucket,
        prefix,
        is_truncated: false,
        uploads: map_uploads(uploads),
    };
    xml_response(StatusCode::OK, &payload)
}
