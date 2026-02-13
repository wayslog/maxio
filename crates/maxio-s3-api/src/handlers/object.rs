use std::{collections::HashMap, sync::Arc};

use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{
        header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE},
        HeaderMap, HeaderName, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use md5::{Digest, Md5};
use maxio_common::{
    error::MaxioError,
    types::{ObjectEncryption, ObjectInfo},
};
use maxio_storage::traits::{
    GetEncryptionOptions, ListObjectsResult, ObjectLayer, PutEncryptionOptions, VersioningState,
};
use quick_xml::se::to_string as xml_to_string;
use serde::Serialize;

use crate::error::S3Error;

type S3Result = std::result::Result<Response, S3Error>;

const SSE_HEADER: &str = "x-amz-server-side-encryption";
const SSE_C_ALGORITHM_HEADER: &str = "x-amz-server-side-encryption-customer-algorithm";
const SSE_C_KEY_HEADER: &str = "x-amz-server-side-encryption-customer-key";
const SSE_C_KEY_MD5_HEADER: &str = "x-amz-server-side-encryption-customer-key-md5";

#[derive(Debug, Serialize)]
#[serde(rename = "ListBucketResult")]
struct ListBucketResultXml {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "Marker")]
    marker: String,
    #[serde(rename = "MaxKeys")]
    max_keys: i32,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "Contents", default)]
    contents: Vec<ObjectContentXml>,
    #[serde(rename = "CommonPrefixes", default)]
    common_prefixes: Vec<CommonPrefixXml>,
}

#[derive(Debug, Serialize)]
#[serde(rename = "ListBucketV2Result")]
struct ListBucketV2ResultXml {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "KeyCount")]
    key_count: i32,
    #[serde(rename = "MaxKeys")]
    max_keys: i32,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "Contents", default)]
    contents: Vec<ObjectContentXml>,
    #[serde(rename = "ContinuationToken", skip_serializing_if = "Option::is_none")]
    continuation_token: Option<String>,
    #[serde(
        rename = "NextContinuationToken",
        skip_serializing_if = "Option::is_none"
    )]
    next_continuation_token: Option<String>,
    #[serde(rename = "CommonPrefixes", default)]
    common_prefixes: Vec<CommonPrefixXml>,
}

#[derive(Debug, Serialize)]
struct ObjectContentXml {
    #[serde(rename = "Key")]
    key: String,
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
struct CommonPrefixXml {
    #[serde(rename = "Prefix")]
    prefix: String,
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

fn header_value(value: &str) -> std::result::Result<HeaderValue, MaxioError> {
    HeaderValue::from_str(value)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid header value: {err}")))
}

fn write_object_headers(
    headers: &mut HeaderMap,
    info: &ObjectInfo,
    content_len: usize,
) -> std::result::Result<(), MaxioError> {
    headers.insert(CONTENT_TYPE, header_value(&info.content_type)?);
    headers.insert(CONTENT_LENGTH, header_value(&content_len.to_string())?);
    headers.insert(ETAG, header_value(&quoted_etag(&info.etag))?);
    headers.insert(LAST_MODIFIED, header_value(&info.last_modified.to_rfc2822())?);

    for (key, value) in &info.metadata {
        let header_name = HeaderName::from_bytes(format!("x-amz-meta-{key}").as_bytes())
            .map_err(|err| MaxioError::InvalidArgument(format!("invalid metadata key: {err}")))?;
        headers.insert(header_name, header_value(value)?);
    }

    if let Some(encryption) = info.encryption.as_ref() {
        write_encryption_response_headers(headers, encryption)?;
    }

    Ok(())
}

fn write_encryption_response_headers(
    headers: &mut HeaderMap,
    encryption: &ObjectEncryption,
) -> std::result::Result<(), MaxioError> {
    headers.insert(SSE_HEADER, header_value(&encryption.algorithm)?);
    if encryption.sse_type == "SSE-C" {
        headers.insert(
            SSE_C_ALGORITHM_HEADER,
            header_value(&encryption.algorithm)?,
        );
        if let Some(key_md5) = encryption.key_md5.as_deref() {
            headers.insert(SSE_C_KEY_MD5_HEADER, header_value(key_md5)?);
        }
    }
    Ok(())
}

fn map_objects(objects: Vec<ObjectInfo>) -> Vec<ObjectContentXml> {
    objects
        .into_iter()
        .map(|item| ObjectContentXml {
            key: item.key,
            last_modified: item.last_modified.to_rfc3339(),
            etag: quoted_etag(&item.etag),
            size: item.size,
            storage_class: "STANDARD".to_string(),
        })
        .collect()
}

fn map_prefixes(prefixes: Vec<String>) -> Vec<CommonPrefixXml> {
    prefixes
        .into_iter()
        .map(|prefix| CommonPrefixXml { prefix })
        .collect()
}

fn parse_max_keys(query: &HashMap<String, String>) -> i32 {
    query
        .get("max-keys")
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1000)
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

fn parse_sse_c_headers(
    headers: &HeaderMap,
    require_complete_if_present: bool,
) -> std::result::Result<Option<GetEncryptionOptions>, MaxioError> {
    let algorithm = headers
        .get(SSE_C_ALGORITHM_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim);
    let key_b64 = headers
        .get(SSE_C_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim);
    let key_md5 = headers
        .get(SSE_C_KEY_MD5_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim);

    let any_present = algorithm.is_some() || key_b64.is_some() || key_md5.is_some();
    if !any_present {
        return Ok(None);
    }

    if require_complete_if_present
        && (algorithm.is_none() || key_b64.is_none() || key_md5.is_none())
    {
        return Err(MaxioError::InvalidArgument(
            "incomplete SSE-C headers".to_string(),
        ));
    }

    let algorithm = algorithm.ok_or_else(|| {
        MaxioError::InvalidArgument("missing SSE-C algorithm header".to_string())
    })?;
    if algorithm != "AES256" {
        return Err(MaxioError::InvalidArgument(
            "unsupported SSE-C algorithm".to_string(),
        ));
    }

    let key_b64 = key_b64
        .ok_or_else(|| MaxioError::InvalidArgument("missing SSE-C customer key".to_string()))?;
    let key_md5 = key_md5.ok_or_else(|| {
        MaxioError::InvalidArgument("missing SSE-C customer key MD5".to_string())
    })?;

    let key_bytes = BASE64_STANDARD.decode(key_b64).map_err(|_| {
        MaxioError::InvalidArgument("invalid base64 SSE-C customer key".to_string())
    })?;
    if key_bytes.len() != 32 {
        return Err(MaxioError::InvalidArgument(
            "SSE-C customer key must be 256-bit".to_string(),
        ));
    }

    let computed_md5 = BASE64_STANDARD.encode(Md5::digest(&key_bytes));
    if computed_md5 != key_md5 {
        return Err(MaxioError::InvalidArgument(
            "SSE-C customer key MD5 mismatch".to_string(),
        ));
    }

    let mut customer_key = [0_u8; 32];
    customer_key.copy_from_slice(&key_bytes);
    Ok(Some(GetEncryptionOptions {
        sse_c_key: Some(customer_key),
        sse_c_key_md5: Some(key_md5.to_string()),
    }))
}

fn parse_put_encryption(headers: &HeaderMap) -> std::result::Result<Option<PutEncryptionOptions>, MaxioError> {
    let sse_s3 = headers
        .get(SSE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .map(|value| value == "AES256")
        .unwrap_or(false);

    if headers
        .get(SSE_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim() != "AES256")
    {
        return Err(MaxioError::InvalidArgument(
            "unsupported x-amz-server-side-encryption algorithm".to_string(),
        ));
    }

    let sse_c = parse_sse_c_headers(headers, true)?;
    if sse_s3 && sse_c.is_some() {
        return Err(MaxioError::InvalidArgument(
            "SSE-S3 and SSE-C cannot be used together".to_string(),
        ));
    }

    if let Some(sse_c) = sse_c {
        return Ok(Some(PutEncryptionOptions {
            sse_s3: false,
            sse_c_key: sse_c.sse_c_key,
            sse_c_key_md5: sse_c.sse_c_key_md5,
        }));
    }

    if sse_s3 {
        return Ok(Some(PutEncryptionOptions {
            sse_s3: true,
            sse_c_key: None,
            sse_c_key_md5: None,
        }));
    }

    Ok(None)
}

pub async fn put_object(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> S3Result {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    let metadata = extract_put_metadata(&headers);
    let encryption = parse_put_encryption(&headers)?;
    let info = store
        .put_object(&bucket, &key, body, content_type, metadata, encryption)
        .await?;

    let mut response_headers = HeaderMap::new();
    response_headers.insert(ETAG, header_value(&quoted_etag(&info.etag))?);
    if let Some(encryption) = info.encryption.as_ref() {
        write_encryption_response_headers(&mut response_headers, encryption)?;
    }
    Ok((StatusCode::OK, response_headers).into_response())
}

pub async fn get_object(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> S3Result {
    let version_id = query.get("versionId").cloned().filter(|item| !item.is_empty());
    let encryption = parse_sse_c_headers(&headers, false)?;
    let (info, data) = match version_id.as_deref() {
        Some(version_id) => {
            store
                .get_object_version(&bucket, &key, version_id, encryption.clone())
                .await?
        }
        None => store.get_object(&bucket, &key, encryption).await?,
    };
    let total_len = data.len();

    let range_header = headers
        .get(RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| parse_range_header(s, total_len));

    let (status, response_data, content_range) = match range_header {
        Some((start, end)) => {
            let slice = data.slice(start..=end);
            let content_range = format!("bytes {}-{}/{}", start, end, total_len);
            (StatusCode::PARTIAL_CONTENT, slice, Some(content_range))
        }
        None => (StatusCode::OK, data, None),
    };

    let response_len = response_data.len();
    let mut response = Response::new(Body::from(response_data));
    *response.status_mut() = status;
    write_object_headers(response.headers_mut(), &info, response_len)?;
    if let Some(version_id) = info.version_id.as_deref() {
        response
            .headers_mut()
            .insert("x-amz-version-id", HeaderValue::from_str(version_id).map_err(|err| {
                MaxioError::InvalidArgument(format!("invalid version id header value: {err}"))
            })?);
    }

    if let Some(range_str) = content_range {
        response.headers_mut().insert(
            CONTENT_RANGE,
            HeaderValue::from_str(&range_str).unwrap_or_else(|_| HeaderValue::from_static("")),
        );
    }

    Ok(response)
}

fn parse_range_header(header: &str, total_len: usize) -> Option<(usize, usize)> {
    let header = header.strip_prefix("bytes=")?;
    let parts: Vec<&str> = header.split('-').collect();
    if parts.len() != 2 {
        return None;
    }

    let start = parts[0].parse::<usize>().ok();
    let end_str = parts[1];

    match (start, end_str.is_empty()) {
        (Some(s), true) => Some((s, total_len.saturating_sub(1))),
        (Some(s), false) => {
            let e = end_str.parse::<usize>().ok()?;
            Some((s, e.min(total_len.saturating_sub(1))))
        }
        (None, false) => {
            let suffix_len = end_str.parse::<usize>().ok()?;
            let start = total_len.saturating_sub(suffix_len);
            Some((start, total_len.saturating_sub(1)))
        }
        _ => None,
    }
}

pub async fn head_object(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> S3Result {
    let encryption = parse_sse_c_headers(&headers, false)?;
    let info = store.get_object_info(&bucket, &key, encryption).await?;
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    let content_len = if info.size >= 0 { info.size as usize } else { 0 };
    write_object_headers(response.headers_mut(), &info, content_len)?;
    Ok(response)
}

pub async fn delete_object(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> S3Result {
    if let Some(version_id) = query.get("versionId").filter(|item| !item.is_empty()) {
        store.delete_object_version(&bucket, &key, version_id).await?;
        return Ok(StatusCode::NO_CONTENT.into_response());
    }

    let versioning = store.get_bucket_versioning(&bucket).await?;
    store.delete_object(&bucket, &key).await?;

    if versioning == VersioningState::Enabled {
        return Ok((
            StatusCode::NO_CONTENT,
            [("x-amz-delete-marker", "true")],
        )
            .into_response());
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}

pub async fn list_objects_v1(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> S3Result {
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let marker = query.get("marker").cloned().unwrap_or_default();
    let delimiter = query.get("delimiter").cloned().unwrap_or_default();
    let max_keys = parse_max_keys(&query);

    let result = store
        .list_objects(&bucket, &prefix, &marker, &delimiter, max_keys)
        .await?;
    let payload = ListBucketResultXml {
        name: bucket,
        prefix,
        marker,
        max_keys,
        is_truncated: result.is_truncated,
        contents: map_objects(result.objects),
        common_prefixes: map_prefixes(result.prefixes),
    };

    xml_response(StatusCode::OK, &payload)
}

pub async fn list_objects_v2(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> S3Result {
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let continuation_token = query.get("continuation-token").cloned();
    let marker = continuation_token
        .clone()
        .or_else(|| query.get("start-after").cloned())
        .unwrap_or_default();
    let delimiter = query.get("delimiter").cloned().unwrap_or_default();
    let max_keys = parse_max_keys(&query);

    let ListObjectsResult {
        objects,
        prefixes,
        is_truncated,
        next_marker,
    } = store
        .list_objects(&bucket, &prefix, &marker, &delimiter, max_keys)
        .await?;

    let key_count = objects.len() as i32;
    let payload = ListBucketV2ResultXml {
        name: bucket,
        prefix,
        key_count,
        max_keys,
        is_truncated,
        contents: map_objects(objects),
        continuation_token,
        next_continuation_token: next_marker,
        common_prefixes: map_prefixes(prefixes),
    };

    xml_response(StatusCode::OK, &payload)
}
