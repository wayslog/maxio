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

fn website_config_key(bucket: &str) -> String {
    format!("buckets/{bucket}/website.xml")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "WebsiteConfiguration")]
pub struct WebsiteConfiguration {
    #[serde(rename = "IndexDocument", skip_serializing_if = "Option::is_none")]
    pub index_document: Option<IndexDocument>,
    #[serde(rename = "ErrorDocument", skip_serializing_if = "Option::is_none")]
    pub error_document: Option<ErrorDocument>,
    #[serde(
        rename = "RedirectAllRequestsTo",
        skip_serializing_if = "Option::is_none"
    )]
    pub redirect_all_requests_to: Option<RedirectAllRequestsTo>,
    #[serde(rename = "RoutingRules", skip_serializing_if = "Option::is_none")]
    pub routing_rules: Option<RoutingRules>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDocument {
    #[serde(rename = "Suffix")]
    pub suffix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDocument {
    #[serde(rename = "Key")]
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedirectAllRequestsTo {
    #[serde(rename = "HostName")]
    pub host_name: String,
    #[serde(rename = "Protocol", skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRules {
    #[serde(rename = "RoutingRule", default)]
    pub rules: Vec<RoutingRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    #[serde(rename = "Condition", skip_serializing_if = "Option::is_none")]
    pub condition: Option<Condition>,
    #[serde(rename = "Redirect", skip_serializing_if = "Option::is_none")]
    pub redirect: Option<Redirect>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    #[serde(rename = "KeyPrefixEquals", skip_serializing_if = "Option::is_none")]
    pub key_prefix_equals: Option<String>,
    #[serde(
        rename = "HttpErrorCodeReturnedEquals",
        skip_serializing_if = "Option::is_none"
    )]
    pub http_error_code_returned_equals: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Redirect {
    #[serde(rename = "HostName", skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
    #[serde(rename = "Protocol", skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(
        rename = "ReplaceKeyPrefixWith",
        skip_serializing_if = "Option::is_none"
    )]
    pub replace_key_prefix_with: Option<String>,
    #[serde(rename = "ReplaceKeyWith", skip_serializing_if = "Option::is_none")]
    pub replace_key_with: Option<String>,
    #[serde(rename = "HttpRedirectCode", skip_serializing_if = "Option::is_none")]
    pub http_redirect_code: Option<String>,
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

pub async fn get_bucket_website(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = website_config_key(&bucket);
    let (_, body) = store
        .get_object(INTERNAL_CONFIG_BUCKET, &key, None)
        .await
        .map_err(|err| match err {
            MaxioError::ObjectNotFound { .. } | MaxioError::BucketNotFound(_) => {
                MaxioError::NoSuchWebsiteConfiguration(bucket.clone())
            }
            other => other,
        })?;

    let config_body = std::str::from_utf8(&body).map_err(|err| {
        MaxioError::InternalError(format!("stored website config is not valid UTF-8: {err}"))
    })?;
    let config: WebsiteConfiguration = xml_from_str(config_body).map_err(|err| {
        MaxioError::InternalError(format!("stored website config is invalid XML: {err}"))
    })?;

    xml_response(StatusCode::OK, &config)
}

pub async fn put_bucket_website(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let config: WebsiteConfiguration = xml_from_str(body_str)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid website xml body: {err}")))?;
    let xml = xml_to_string(&config).map_err(|err| {
        MaxioError::InternalError(format!(
            "failed to serialize website xml for storage: {err}"
        ))
    })?;

    ensure_internal_bucket(&store).await?;
    let key = website_config_key(&bucket);
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

pub async fn delete_bucket_website(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = website_config_key(&bucket);
    match store.delete_object(INTERNAL_CONFIG_BUCKET, &key).await {
        Ok(()) | Err(MaxioError::ObjectNotFound { .. }) | Err(MaxioError::BucketNotFound(_)) => {
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Err(err) => Err(S3Error::from(err)),
    }
}
