use std::sync::Arc;

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

fn tagging_config_key(bucket: &str) -> String {
    format!("buckets/{bucket}/tagging.xml")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "Tagging")]
pub struct Tagging {
    #[serde(rename = "TagSet")]
    pub tag_set: TagSet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagSet {
    #[serde(rename = "Tag", default)]
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Value")]
    pub value: String,
}

fn validate_tagging(tagging: &Tagging) -> Result<(), MaxioError> {
    if tagging.tag_set.tags.len() > 50 {
        return Err(MaxioError::InvalidArgument(
            "Object tags cannot be greater than 50".to_string(),
        ));
    }
    for tag in &tagging.tag_set.tags {
        if tag.key.len() > 128 {
            return Err(MaxioError::InvalidArgument(
                "Tag key cannot be longer than 128 characters".to_string(),
            ));
        }
        if tag.value.len() > 256 {
            return Err(MaxioError::InvalidArgument(
                "Tag value cannot be longer than 256 characters".to_string(),
            ));
        }
        if tag.key.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "Tag key cannot be empty".to_string(),
            ));
        }
    }
    Ok(())
}

pub async fn get_bucket_tagging(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store
        .get_bucket_info(&bucket)
        .await
        .map_err(|_| S3Error::from(MaxioError::BucketNotFound(bucket.clone())))?;

    let key = tagging_config_key(&bucket);
    let (_info, data) = store
        .get_object(INTERNAL_CONFIG_BUCKET, &key, None)
        .await
        .map_err(|_| S3Error::from(MaxioError::NoSuchTagSet(bucket.clone())))?;

    let xml_str = String::from_utf8(data.to_vec()).map_err(|err| {
        S3Error::from(MaxioError::InternalError(format!(
            "invalid tagging config encoding: {err}"
        )))
    })?;

    Ok((
        StatusCode::OK,
        [("Content-Type", "application/xml")],
        xml_str,
    )
        .into_response())
}

pub async fn put_bucket_tagging(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store
        .get_bucket_info(&bucket)
        .await
        .map_err(|_| S3Error::from(MaxioError::BucketNotFound(bucket.clone())))?;

    let body_str = String::from_utf8(body.to_vec()).map_err(|err| {
        S3Error::from(MaxioError::MalformedXML(format!(
            "invalid UTF-8 in request body: {err}"
        )))
    })?;

    let tagging: Tagging = xml_from_str(&body_str).map_err(|err| {
        S3Error::from(MaxioError::MalformedXML(format!(
            "failed to parse Tagging XML: {err}"
        )))
    })?;

    validate_tagging(&tagging)?;

    let xml_output = xml_to_string(&tagging).map_err(|err| {
        S3Error::from(MaxioError::InternalError(format!(
            "failed to serialize tagging config: {err}"
        )))
    })?;
    let xml_data = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{xml_output}");

    match store.make_bucket(INTERNAL_CONFIG_BUCKET).await {
        Ok(()) | Err(MaxioError::BucketAlreadyExists(_)) => {}
        Err(err) => return Err(S3Error::from(err)),
    }

    let key = tagging_config_key(&bucket);
    store
        .put_object(
            INTERNAL_CONFIG_BUCKET,
            &key,
            xml_data.into_bytes().into(),
            Some("application/xml"),
            std::collections::HashMap::new(),
            None,
        )
        .await
        .map_err(S3Error::from)?;

    Ok(StatusCode::NO_CONTENT.into_response())
}

pub async fn delete_bucket_tagging(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store
        .get_bucket_info(&bucket)
        .await
        .map_err(|_| S3Error::from(MaxioError::BucketNotFound(bucket.clone())))?;

    let key = tagging_config_key(&bucket);
    match store.delete_object(INTERNAL_CONFIG_BUCKET, &key).await {
        Ok(()) | Err(MaxioError::ObjectNotFound { .. }) => {}
        Err(err) => return Err(S3Error::from(err)),
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}

pub async fn get_bucket_accelerate(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store
        .get_bucket_info(&bucket)
        .await
        .map_err(|_| S3Error::from(MaxioError::BucketNotFound(bucket.clone())))?;

    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<AccelerateConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#;

    Ok((
        StatusCode::OK,
        [("Content-Type", "application/xml")],
        body,
    )
        .into_response())
}
