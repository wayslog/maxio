use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use maxio_common::error::{MaxioError, Result as MaxioResult};
use maxio_storage::traits::ObjectLayer;
use quick_xml::{de::from_str as xml_from_str, se::to_string as xml_to_string};
use serde::{Deserialize, Serialize};

use crate::error::S3Error;

type S3Result = Result<Response, S3Error>;

#[derive(Debug, Deserialize)]
#[serde(rename = "Delete")]
struct DeleteRequestXml {
    #[serde(rename = "Object", default)]
    objects: Vec<DeleteObjectXml>,
    #[serde(rename = "Quiet", default)]
    quiet: bool,
}

#[derive(Debug, Deserialize)]
struct DeleteObjectXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename = "DeleteResult")]
struct DeleteResultXml {
    #[serde(rename = "Deleted", default)]
    deleted: Vec<DeletedEntryXml>,
    #[serde(rename = "Error", default)]
    errors: Vec<DeleteErrorEntryXml>,
}

#[derive(Debug, Serialize)]
struct DeletedEntryXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId", skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct DeleteErrorEntryXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId", skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "Message")]
    message: String,
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

fn parse_delete_request(body: &Bytes) -> MaxioResult<DeleteRequestXml> {
    let body_str = std::str::from_utf8(body).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid delete xml body encoding: {err}"))
    })?;
    xml_from_str(body_str)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid delete xml body: {err}")))
}

pub async fn delete_objects(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> S3Result {
    store.get_bucket_info(&bucket).await?;
    let payload = parse_delete_request(&body)?;
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for object in payload.objects {
        let key = object.key.trim().to_string();
        let version_id = object.version_id.clone().filter(|value| !value.is_empty());

        if key.is_empty() {
            errors.push(DeleteErrorEntryXml {
                key: object.key,
                version_id,
                code: MaxioError::InvalidArgument("missing object key".to_string())
                    .s3_error_code()
                    .to_string(),
                message: "invalid argument: missing object key".to_string(),
            });
            continue;
        }

        let result = if let Some(version_id_ref) = version_id.as_deref() {
            store
                .delete_object_version(&bucket, &key, version_id_ref)
                .await
        } else {
            store.delete_object(&bucket, &key).await
        };

        match result {
            Ok(()) => {
                if !payload.quiet {
                    deleted.push(DeletedEntryXml {
                        key,
                        version_id: version_id.clone(),
                    });
                }
            }
            Err(MaxioError::ObjectNotFound { .. }) => {
                if !payload.quiet {
                    deleted.push(DeletedEntryXml { key, version_id });
                }
            }
            Err(err) => errors.push(DeleteErrorEntryXml {
                key,
                version_id,
                code: err.s3_error_code().to_string(),
                message: err.to_string(),
            }),
        }
    }

    let result = DeleteResultXml { deleted, errors };
    xml_response(StatusCode::OK, &result)
}
