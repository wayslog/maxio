use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use maxio_common::error::MaxioError;
use maxio_storage::traits::ObjectLayer;
use quick_xml::{de::from_str as xml_from_str, se::to_string as xml_to_string};
use serde::{Deserialize, Serialize};

use crate::error::S3Error;

type S3Result = Result<Response, S3Error>;

const CANNED_ACL_HEADER: &str = "x-amz-acl";
const ACL_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const GROUP_ALL_USERS_URI: &str = "http://acs.amazonaws.com/groups/global/AllUsers";
const GROUP_AUTHENTICATED_USERS_URI: &str =
    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "AccessControlPolicy")]
pub struct AccessControlPolicy {
    #[serde(rename = "@xmlns", default = "default_acl_xmlns")]
    pub xmlns: String,
    #[serde(rename = "Owner")]
    pub owner: Owner,
    #[serde(rename = "AccessControlList")]
    pub access_control_list: AccessControlList,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessControlList {
    #[serde(rename = "Grant", default)]
    pub grants: Vec<Grant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Owner {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grant {
    #[serde(rename = "Grantee")]
    pub grantee: Grantee,
    #[serde(rename = "Permission")]
    pub permission: Permission,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grantee {
    #[serde(
        rename = "@xmlns:xsi",
        default = "default_xsi_xmlns",
        skip_serializing_if = "String::is_empty"
    )]
    pub xmlns_xsi: String,
    #[serde(rename = "@xsi:type", alias = "Type", default)]
    pub type_name: String,
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "DisplayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "URI", skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

fn default_xsi_xmlns() -> String {
    "http://www.w3.org/2001/XMLSchema-instance".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Permission {
    FullControl,
    Write,
    WriteAcp,
    Read,
    ReadAcp,
}

fn default_acl_xmlns() -> String {
    ACL_XMLNS.to_string()
}

fn default_owner() -> Owner {
    Owner {
        id: "maxio".to_string(),
        display_name: "maxio".to_string(),
    }
}

fn owner_grantee(owner: &Owner) -> Grantee {
    Grantee {
        xmlns_xsi: default_xsi_xmlns(),
        type_name: "CanonicalUser".to_string(),
        id: Some(owner.id.clone()),
        display_name: Some(owner.display_name.clone()),
        uri: None,
    }
}

fn group_grantee(uri: &str) -> Grantee {
    Grantee {
        xmlns_xsi: default_xsi_xmlns(),
        type_name: "Group".to_string(),
        id: None,
        display_name: None,
        uri: Some(uri.to_string()),
    }
}

fn default_policy() -> AccessControlPolicy {
    let owner = default_owner();
    AccessControlPolicy {
        xmlns: default_acl_xmlns(),
        owner: owner.clone(),
        access_control_list: AccessControlList {
            grants: vec![Grant {
                grantee: owner_grantee(&owner),
                permission: Permission::FullControl,
            }],
        },
    }
}

fn canned_policy(canned_acl: &str) -> Result<AccessControlPolicy, MaxioError> {
    let mut policy = default_policy();

    match canned_acl {
        "private" => {}
        "public-read" => {
            policy.access_control_list.grants.push(Grant {
                grantee: group_grantee(GROUP_ALL_USERS_URI),
                permission: Permission::Read,
            });
        }
        "public-read-write" => {
            policy.access_control_list.grants.push(Grant {
                grantee: group_grantee(GROUP_ALL_USERS_URI),
                permission: Permission::Read,
            });
            policy.access_control_list.grants.push(Grant {
                grantee: group_grantee(GROUP_ALL_USERS_URI),
                permission: Permission::Write,
            });
        }
        "authenticated-read" => {
            policy.access_control_list.grants.push(Grant {
                grantee: group_grantee(GROUP_AUTHENTICATED_USERS_URI),
                permission: Permission::Read,
            });
        }
        other => {
            return Err(MaxioError::InvalidArgument(format!(
                "unsupported x-amz-acl value: {other}"
            )));
        }
    }

    Ok(policy)
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

fn parse_acl_xml(body: &Bytes) -> Result<AccessControlPolicy, MaxioError> {
    let body_str = std::str::from_utf8(body)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid xml body encoding: {err}")))?;
    let mut policy: AccessControlPolicy = xml_from_str(body_str)
        .map_err(|err| MaxioError::InvalidArgument(format!("invalid acl xml body: {err}")))?;
    if policy.xmlns.is_empty() {
        policy.xmlns = default_acl_xmlns();
    }
    Ok(policy)
}

fn resolve_acl_policy(
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<AccessControlPolicy, MaxioError> {
    if let Some(canned_acl) = headers.get(CANNED_ACL_HEADER) {
        let value = canned_acl.to_str().map_err(|err| {
            MaxioError::InvalidArgument(format!("invalid x-amz-acl header: {err}"))
        })?;
        return canned_policy(value.trim());
    }

    if body.is_empty() {
        return Err(MaxioError::InvalidArgument(
            "missing ACL XML body or x-amz-acl header".to_string(),
        ));
    }

    parse_acl_xml(body)
}

fn parse_stored_acl(stored_acl: Option<String>) -> Result<AccessControlPolicy, MaxioError> {
    match stored_acl {
        Some(acl_json) => serde_json::from_str(&acl_json)
            .map_err(|err| MaxioError::InternalError(format!("failed to parse stored ACL: {err}"))),
        None => Ok(default_policy()),
    }
}

pub async fn get_bucket_acl(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
) -> S3Result {
    let policy = parse_stored_acl(store.get_bucket_acl(&bucket).await?)?;
    xml_response(StatusCode::OK, &policy)
}

pub async fn put_bucket_acl(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> S3Result {
    let policy = resolve_acl_policy(&headers, &body)?;
    let acl_json = serde_json::to_string_pretty(&policy).map_err(|err| {
        MaxioError::InternalError(format!("failed to serialize bucket ACL for storage: {err}"))
    })?;
    store.put_bucket_acl(&bucket, &acl_json).await?;
    Ok(StatusCode::OK.into_response())
}

pub async fn get_object_acl(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
) -> S3Result {
    let policy = parse_stored_acl(store.get_object_acl(&bucket, &key).await?)?;
    xml_response(StatusCode::OK, &policy)
}

pub async fn put_object_acl(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> S3Result {
    let policy = resolve_acl_policy(&headers, &body)?;
    let acl_json = serde_json::to_string_pretty(&policy).map_err(|err| {
        MaxioError::InternalError(format!("failed to serialize object ACL for storage: {err}"))
    })?;
    store.put_object_acl(&bucket, &key, &acl_json).await?;
    Ok(StatusCode::OK.into_response())
}
