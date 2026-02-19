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

fn cors_config_key(bucket: &str) -> String {
    format!("buckets/{bucket}/cors.xml")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "CORSConfiguration")]
pub struct CorsConfiguration {
    #[serde(rename = "CORSRule", default)]
    pub rules: Vec<CorsRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsRule {
    #[serde(rename = "AllowedOrigin", default)]
    pub allowed_origins: Vec<String>,
    #[serde(rename = "AllowedMethod", default)]
    pub allowed_methods: Vec<String>,
    #[serde(rename = "AllowedHeader", default)]
    pub allowed_headers: Vec<String>,
    #[serde(rename = "ExposeHeader", default)]
    pub expose_headers: Vec<String>,
    #[serde(rename = "MaxAgeSeconds", skip_serializing_if = "Option::is_none")]
    pub max_age_seconds: Option<i64>,
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
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

fn validate_cors_configuration(config: &CorsConfiguration) -> Result<(), MaxioError> {
    if config.rules.is_empty() {
        return Err(MaxioError::InvalidArgument(
            "CORSConfiguration must include at least one CORSRule".to_string(),
        ));
    }

    for (index, rule) in config.rules.iter().enumerate() {
        if rule.allowed_origins.is_empty() {
            return Err(MaxioError::InvalidArgument(format!(
                "CORS rule {} must include at least one AllowedOrigin",
                index + 1
            )));
        }

        for origin in &rule.allowed_origins {
            if origin.trim().is_empty() {
                return Err(MaxioError::InvalidArgument(format!(
                    "CORS rule {} contains an empty AllowedOrigin",
                    index + 1
                )));
            }
        }

        if rule.allowed_methods.is_empty() {
            return Err(MaxioError::InvalidArgument(format!(
                "CORS rule {} must include at least one AllowedMethod",
                index + 1
            )));
        }

        for method in &rule.allowed_methods {
            match method.as_str() {
                "GET" | "PUT" | "POST" | "DELETE" | "HEAD" => {}
                _ => {
                    return Err(MaxioError::InvalidArgument(format!(
                        "CORS rule {} has unsupported AllowedMethod: {}",
                        index + 1,
                        method
                    )));
                }
            }
        }

        for header in &rule.allowed_headers {
            if header.trim().is_empty() {
                return Err(MaxioError::InvalidArgument(format!(
                    "CORS rule {} contains an empty AllowedHeader",
                    index + 1
                )));
            }
        }

        for header in &rule.expose_headers {
            if header.trim().is_empty() {
                return Err(MaxioError::InvalidArgument(format!(
                    "CORS rule {} contains an empty ExposeHeader",
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

pub async fn get_bucket_cors(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = cors_config_key(&bucket);
    let (_, body) = store
        .get_object(INTERNAL_CONFIG_BUCKET, &key, None)
        .await
        .map_err(|err| match err {
            MaxioError::ObjectNotFound { .. } | MaxioError::BucketNotFound(_) => {
                MaxioError::NoSuchCORSConfiguration(bucket.clone())
            }
            other => other,
        })?;

    let config_body = std::str::from_utf8(&body).map_err(|err| {
        MaxioError::InternalError(format!("stored CORS config is not valid UTF-8: {err}"))
    })?;
    let config: CorsConfiguration = xml_from_str(config_body).map_err(|err| {
        MaxioError::InternalError(format!("stored CORS config is invalid XML: {err}"))
    })?;

    xml_response(StatusCode::OK, &config)
}

pub async fn put_bucket_cors(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let config: CorsConfiguration = xml_from_str(body_str)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid CORS xml body: {err}")))?;
    validate_cors_configuration(&config)?;

    let xml = xml_to_string(&config).map_err(|err| {
        MaxioError::InternalError(format!("failed to serialize CORS xml for storage: {err}"))
    })?;

    ensure_internal_bucket(&store).await?;
    let key = cors_config_key(&bucket);
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

pub async fn delete_bucket_cors(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = cors_config_key(&bucket);
    match store.delete_object(INTERNAL_CONFIG_BUCKET, &key).await {
        Ok(()) | Err(MaxioError::ObjectNotFound { .. }) | Err(MaxioError::BucketNotFound(_)) => {
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Err(err) => Err(S3Error::from(err)),
    }
}
