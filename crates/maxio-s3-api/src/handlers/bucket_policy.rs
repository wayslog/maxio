use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use maxio_common::error::MaxioError;
use maxio_storage::traits::ObjectLayer;
use quick_xml::se::to_string as xml_to_string;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::S3Error;

type S3Result = Result<Response, S3Error>;

const INTERNAL_CONFIG_BUCKET: &str = ".minio.sys";
const POLICY_VERSION: &str = "2012-10-17";

#[derive(Debug, Deserialize)]
struct BucketPolicyDocument {
    #[serde(rename = "Version")]
    version: String,
    #[serde(rename = "Statement")]
    statements: Vec<BucketPolicyStatement>,
}

#[derive(Debug, Deserialize)]
struct BucketPolicyStatement {
    #[serde(rename = "Sid")]
    _sid: Option<String>,
    #[serde(rename = "Effect")]
    effect: String,
    #[serde(rename = "Principal")]
    principal: Value,
    #[serde(rename = "Action")]
    action: Value,
    #[serde(rename = "Resource")]
    resource: Value,
    #[serde(rename = "Condition")]
    _condition: Option<Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename = "PolicyStatus")]
struct PolicyStatusXml {
    #[serde(rename = "IsPublic")]
    is_public: bool,
}

fn policy_key(bucket: &str) -> String {
    format!("buckets/{bucket}/policy.json")
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

fn validate_string_or_string_array(value: &Value, field_name: &str) -> Result<(), MaxioError> {
    let is_valid = match value {
        Value::String(text) => !text.trim().is_empty(),
        Value::Array(values) => values
            .iter()
            .all(|item| matches!(item, Value::String(text) if !text.trim().is_empty())),
        _ => false,
    };

    if is_valid {
        Ok(())
    } else {
        Err(MaxioError::InvalidArgument(format!(
            "policy statement field {field_name} must be a non-empty string or array of non-empty strings"
        )))
    }
}

fn validate_policy_document(policy: &BucketPolicyDocument) -> Result<(), MaxioError> {
    if policy.version != POLICY_VERSION {
        return Err(MaxioError::InvalidArgument(format!(
            "unsupported policy version: {}",
            policy.version
        )));
    }

    for statement in &policy.statements {
        match statement.effect.as_str() {
            "Allow" | "Deny" => {}
            other => {
                return Err(MaxioError::InvalidArgument(format!(
                    "invalid policy effect: {other}"
                )));
            }
        }
        validate_string_or_string_array(&statement.action, "Action")?;
        validate_string_or_string_array(&statement.resource, "Resource")?;
    }

    Ok(())
}

fn value_contains_public_wildcard(value: &Value) -> bool {
    match value {
        Value::String(text) => text == "*",
        Value::Array(values) => values
            .iter()
            .any(|item| matches!(item, Value::String(text) if text == "*")),
        _ => false,
    }
}

fn is_public_principal(principal: &Value) -> bool {
    match principal {
        Value::String(text) => text == "*",
        Value::Object(map) => map.values().any(value_contains_public_wildcard),
        _ => false,
    }
}

fn is_policy_public(policy: &BucketPolicyDocument) -> bool {
    policy
        .statements
        .iter()
        .any(|statement| statement.effect == "Allow" && is_public_principal(&statement.principal))
}

async fn ensure_internal_bucket(store: &Arc<dyn ObjectLayer>) -> Result<(), MaxioError> {
    match store.make_bucket(INTERNAL_CONFIG_BUCKET).await {
        Ok(()) | Err(MaxioError::BucketAlreadyExists(_)) => Ok(()),
        Err(err) => Err(err),
    }
}

async fn get_stored_policy(
    store: &Arc<dyn ObjectLayer>,
    bucket: &str,
) -> Result<String, MaxioError> {
    let key = policy_key(bucket);
    let (_, body) = store
        .get_object(INTERNAL_CONFIG_BUCKET, &key, None)
        .await
        .map_err(|err| match err {
            MaxioError::ObjectNotFound { .. } | MaxioError::BucketNotFound(_) => {
                MaxioError::NoSuchBucketPolicy(bucket.to_string())
            }
            other => other,
        })?;
    String::from_utf8(body.to_vec()).map_err(|err| {
        MaxioError::InternalError(format!("stored policy is not valid UTF-8: {err}"))
    })
}

pub async fn get_bucket_policy(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;
    let policy = get_stored_policy(&store, &bucket).await?;
    Ok((
        StatusCode::OK,
        [("Content-Type", "application/json")],
        policy,
    )
        .into_response())
}

pub async fn put_bucket_policy(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let body_str = std::str::from_utf8(&body).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid policy json body encoding: {err}"))
    })?;
    let policy: BucketPolicyDocument = serde_json::from_str(body_str)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid policy json body: {err}")))?;
    validate_policy_document(&policy)?;

    ensure_internal_bucket(&store).await?;
    let key = policy_key(&bucket);
    store
        .put_object(
            INTERNAL_CONFIG_BUCKET,
            &key,
            body,
            Some("application/json"),
            HashMap::new(),
            None,
        )
        .await?;

    Ok(StatusCode::OK.into_response())
}

pub async fn delete_bucket_policy(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = policy_key(&bucket);
    match store.delete_object(INTERNAL_CONFIG_BUCKET, &key).await {
        Ok(()) | Err(MaxioError::ObjectNotFound { .. }) | Err(MaxioError::BucketNotFound(_)) => {
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Err(err) => Err(S3Error::from(err)),
    }
}

pub async fn get_bucket_policy_status(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let policy_body = get_stored_policy(&store, &bucket).await?;
    let policy: BucketPolicyDocument = serde_json::from_str(&policy_body).map_err(|err| {
        MaxioError::InternalError(format!("stored policy json is invalid: {err}"))
    })?;
    validate_policy_document(&policy)?;

    let payload = PolicyStatusXml {
        is_public: is_policy_public(&policy),
    };
    xml_response(StatusCode::OK, &payload)
}
