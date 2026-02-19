use std::sync::Arc;

use axum::{
    Form,
    extract::Extension,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::Utc;
use maxio_common::error::MaxioError;
use maxio_iam::IAMSys;
use serde::Deserialize;
use uuid::Uuid;

use crate::error::S3Error;

type S3Result = Result<Response, S3Error>;

#[derive(Debug, Deserialize)]
pub struct StsRequest {
    #[serde(rename = "Action")]
    pub action: String,
    #[serde(rename = "Version", default)]
    pub version: String,
    #[serde(rename = "DurationSeconds", default)]
    pub duration_seconds: Option<u64>,
    #[serde(rename = "Policy", default)]
    pub policy: Option<String>,
}

pub async fn sts_handler(
    Extension(iam): Extension<Arc<IAMSys>>,
    Form(req): Form<StsRequest>,
) -> S3Result {
    match req.action.as_str() {
        "AssumeRole" => assume_role(iam, req).await,
        _ => Err(S3Error::from(MaxioError::NotImplemented(format!(
            "STS action not supported: {}",
            req.action
        )))),
    }
}

async fn assume_role(_iam: Arc<IAMSys>, req: StsRequest) -> S3Result {
    let duration = req.duration_seconds.unwrap_or(3600).min(43200).max(900);
    let now = Utc::now();
    let expiration = now + chrono::Duration::seconds(duration as i64);

    let session_token = Uuid::new_v4().to_string();
    let temp_access_key = format!("temp-{}", &session_token[..8]);
    let temp_secret_key = Uuid::new_v4().to_string();

    let expiration_str = expiration.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let body = format!(
        r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>{temp_access_key}</AccessKeyId>
      <SecretAccessKey>{temp_secret_key}</SecretAccessKey>
      <SessionToken>{session_token}</SessionToken>
      <Expiration>{expiration_str}</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::0:assumed-role/maxio/{temp_access_key}</Arn>
      <AssumedRoleId>{temp_access_key}</AssumedRoleId>
    </AssumedRoleUser>
    <PackedPolicySize>0</PackedPolicySize>
  </AssumeRoleResult>
  <ResponseMetadata>
    <RequestId>{session_token}</RequestId>
  </ResponseMetadata>
</AssumeRoleResponse>"#
    );

    Ok((
        StatusCode::OK,
        [("Content-Type", "application/xml")],
        body,
    )
        .into_response())
}
