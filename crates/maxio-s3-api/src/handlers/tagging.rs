use std::{collections::HashSet, sync::Arc};

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

const OBJECT_TAGS_METADATA_KEY: &str = "maxio-tags";
const MAX_TAGS_PER_OBJECT: usize = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "Tagging")]
struct TaggingXml {
    #[serde(rename = "TagSet")]
    tag_set: TagSetXml,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TagSetXml {
    #[serde(rename = "Tag", default)]
    tags: Vec<TagXml>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TagXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value")]
    value: String,
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

fn validate_tag_set(tag_set: &[TagXml]) -> Result<(), MaxioError> {
    if tag_set.len() > MAX_TAGS_PER_OBJECT {
        return Err(MaxioError::InvalidArgument(format!(
            "maximum {MAX_TAGS_PER_OBJECT} tags are allowed per object"
        )));
    }

    let mut keys = HashSet::with_capacity(tag_set.len());
    for tag in tag_set {
        if tag.key.is_empty() {
            return Err(MaxioError::InvalidArgument(
                "tag key must not be empty".to_string(),
            ));
        }
        if !keys.insert(tag.key.clone()) {
            return Err(MaxioError::InvalidArgument(format!(
                "duplicate tag key is not allowed: {}",
                tag.key
            )));
        }
    }

    Ok(())
}

fn parse_tags_metadata(raw: Option<&String>) -> Result<Vec<TagXml>, MaxioError> {
    match raw {
        Some(value) => serde_json::from_str::<Vec<TagXml>>(value).map_err(|err| {
            MaxioError::InternalError(format!("failed to parse stored object tags: {err}"))
        }),
        None => Ok(Vec::new()),
    }
}

pub async fn put_object_tagging(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> S3Result {
    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let payload: TaggingXml = xml_from_str(body_str)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid tagging xml body: {err}")))?;
    validate_tag_set(&payload.tag_set.tags)?;

    let (info, data) = store.get_object(&bucket, &key, None).await?;
    let mut metadata = info.metadata;
    let serialized_tags = serde_json::to_string(&payload.tag_set.tags).map_err(|err| {
        MaxioError::InternalError(format!("failed to serialize object tags for storage: {err}"))
    })?;
    metadata.insert(OBJECT_TAGS_METADATA_KEY.to_string(), serialized_tags);
    store
        .put_object(&bucket, &key, data, Some(&info.content_type), metadata, None)
        .await?;

    Ok(StatusCode::OK.into_response())
}

pub async fn get_object_tagging(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
) -> S3Result {
    let info = store.get_object_info(&bucket, &key, None).await?;
    let tags = parse_tags_metadata(info.metadata.get(OBJECT_TAGS_METADATA_KEY))?;
    let payload = TaggingXml {
        tag_set: TagSetXml { tags },
    };

    xml_response(StatusCode::OK, &payload)
}

pub async fn delete_object_tagging(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
) -> S3Result {
    let (info, data) = store.get_object(&bucket, &key, None).await?;
    let mut metadata = info.metadata;
    metadata.remove(OBJECT_TAGS_METADATA_KEY);
    store
        .put_object(&bucket, &key, data, Some(&info.content_type), metadata, None)
        .await?;

    Ok(StatusCode::NO_CONTENT.into_response())
}
