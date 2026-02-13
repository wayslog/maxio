use std::sync::Arc;

use axum::{
    Extension,
    body::Bytes,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use http::StatusCode;
use maxio_common::{error::MaxioError, types::BucketInfo};
use maxio_notification::{NotificationSys, types::NotificationConfiguration};
use maxio_storage::traits::ObjectLayer;
use quick_xml::{de::from_str as xml_from_str, se::to_string as xml_to_string};
use serde::Serialize;

use crate::error::S3Error;

type S3Result = Result<Response, S3Error>;

#[derive(Debug, Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
struct ListAllMyBucketsResult {
    #[serde(rename = "Owner")]
    owner: Owner,
    #[serde(rename = "Buckets")]
    buckets: Buckets,
}

#[derive(Debug, Serialize)]
struct Owner {
    #[serde(rename = "ID")]
    id: String,
    #[serde(rename = "DisplayName")]
    display_name: String,
}

#[derive(Debug, Serialize)]
struct Buckets {
    #[serde(rename = "Bucket", default)]
    bucket: Vec<BucketXml>,
}

#[derive(Debug, Serialize)]
struct BucketXml {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "CreationDate")]
    creation_date: String,
}

#[derive(Debug, Serialize)]
#[serde(rename = "LocationConstraint")]
struct LocationConstraint {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "$text")]
    value: String,
}

impl From<&BucketInfo> for BucketXml {
    fn from(info: &BucketInfo) -> Self {
        Self {
            name: info.name.clone(),
            creation_date: info.created.to_rfc3339(),
        }
    }
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

pub async fn list_buckets(State(store): State<Arc<dyn ObjectLayer>>) -> S3Result {
    let buckets = store.list_buckets().await?;
    let payload = ListAllMyBucketsResult {
        owner: Owner {
            id: "maxio".to_string(),
            display_name: "maxio".to_string(),
        },
        buckets: Buckets {
            bucket: buckets.iter().map(BucketXml::from).collect(),
        },
    };
    xml_response(StatusCode::OK, &payload)
}

pub async fn make_bucket(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.make_bucket(&bucket).await?;
    Ok(StatusCode::OK.into_response())
}

pub async fn head_bucket(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;
    Ok(StatusCode::OK.into_response())
}

pub async fn delete_bucket(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.delete_bucket(&bucket).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

pub async fn get_bucket_location(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;
    let payload = LocationConstraint {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
        value: String::new(),
    };
    xml_response(StatusCode::OK, &payload)
}

pub async fn get_bucket_notification_configuration(
    State(store): State<Arc<dyn ObjectLayer>>,
    Extension(notifications): Extension<Arc<NotificationSys>>,
    Path(bucket): Path<String>,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;
    let config = notifications.get_config(&bucket).await?;
    xml_response(StatusCode::OK, &config)
}

pub async fn put_bucket_notification_configuration(
    State(store): State<Arc<dyn ObjectLayer>>,
    Extension(notifications): Extension<Arc<NotificationSys>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;
    let body_str = std::str::from_utf8(&body).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid notification xml body encoding: {err}"))
    })?;
    let config: NotificationConfiguration = xml_from_str(body_str).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid notification xml body: {err}"))
    })?;
    notifications.set_config(&bucket, config).await?;
    Ok(StatusCode::OK.into_response())
}
