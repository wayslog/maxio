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

fn request_payment_config_key(bucket: &str) -> String {
    format!("buckets/{bucket}/request-payment.xml")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "RequestPaymentConfiguration")]
pub struct RequestPaymentConfiguration {
    #[serde(rename = "Payer")]
    pub payer: String,
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

fn validate_payer(payer: &str) -> Result<(), MaxioError> {
    match payer {
        "BucketOwner" | "Requester" => Ok(()),
        other => Err(MaxioError::InvalidArgument(format!(
            "invalid Payer value: {other}"
        ))),
    }
}

async fn ensure_internal_bucket(store: &Arc<dyn ObjectLayer>) -> Result<(), MaxioError> {
    match store.make_bucket(INTERNAL_CONFIG_BUCKET).await {
        Ok(()) | Err(MaxioError::BucketAlreadyExists(_)) => Ok(()),
        Err(err) => Err(err),
    }
}

pub async fn get_bucket_request_payment(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let key = request_payment_config_key(&bucket);
    let config = match store.get_object(INTERNAL_CONFIG_BUCKET, &key, None).await {
        Ok((_, body)) => {
            let body_str = std::str::from_utf8(&body).map_err(|err| {
                MaxioError::InternalError(format!(
                    "stored request payment config is not valid UTF-8: {err}"
                ))
            })?;
            let parsed: RequestPaymentConfiguration = xml_from_str(body_str).map_err(|err| {
                MaxioError::InternalError(format!(
                    "stored request payment config is invalid XML: {err}"
                ))
            })?;
            validate_payer(parsed.payer.as_str())?;
            parsed
        }
        Err(MaxioError::ObjectNotFound { .. }) | Err(MaxioError::BucketNotFound(_)) => {
            RequestPaymentConfiguration {
                payer: "BucketOwner".to_string(),
            }
        }
        Err(err) => return Err(S3Error::from(err)),
    };

    xml_response(StatusCode::OK, &config)
}

pub async fn put_bucket_request_payment(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;

    let body_str = std::str::from_utf8(&body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let config: RequestPaymentConfiguration = xml_from_str(body_str).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid request payment xml body: {err}"))
    })?;
    validate_payer(config.payer.as_str())?;
    let xml = xml_to_string(&config).map_err(|err| {
        MaxioError::InternalError(format!(
            "failed to serialize request payment xml for storage: {err}"
        ))
    })?;

    ensure_internal_bucket(&store).await?;
    let key = request_payment_config_key(&bucket);
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
