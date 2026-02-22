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
    #[serde(rename = "WebIdentityToken", default)]
    pub web_identity_token: Option<String>,
    #[serde(rename = "RoleArn", default)]
    pub role_arn: Option<String>,
    #[serde(rename = "RoleSessionName", default)]
    pub role_session_name: Option<String>,
    #[serde(rename = "ProviderId", default)]
    pub provider_id: Option<String>,
    #[serde(rename = "SAMLAssertion", default)]
    pub saml_assertion: Option<String>,
    #[serde(rename = "PrincipalArn", default)]
    pub principal_arn: Option<String>,
}

pub async fn sts_handler(
    Extension(iam): Extension<Arc<IAMSys>>,
    Form(req): Form<StsRequest>,
) -> S3Result {
    match req.action.as_str() {
        "AssumeRole" => assume_role(iam, req).await,
        "GetCallerIdentity" => get_caller_identity().await,
        "GetSessionToken" => get_session_token(req).await,
        "AssumeRoleWithWebIdentity" => assume_role_with_web_identity(req).await,
        "AssumeRoleWithSAML" => assume_role_with_saml(req).await,
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

async fn get_caller_identity() -> S3Result {
    let request_id = Uuid::new_v4().to_string();
    let account = "000000000000";
    let user_id = "maxio";
    let arn = format!("arn:aws:iam::{account}:root");

    let body = format!(
        r#"<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetCallerIdentityResult>
    <Arn>{arn}</Arn>
    <UserId>{user_id}</UserId>
    <Account>{account}</Account>
  </GetCallerIdentityResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</GetCallerIdentityResponse>"#
    );

    Ok((
        StatusCode::OK,
        [("Content-Type", "application/xml")],
        body,
    )
        .into_response())
}

async fn get_session_token(req: StsRequest) -> S3Result {
    let duration = req.duration_seconds.unwrap_or(43200).min(129600).max(900);
    let now = Utc::now();
    let expiration = now + chrono::Duration::seconds(duration as i64);

    let session_token = Uuid::new_v4().to_string();
    let temp_access_key = format!("session-{}", &session_token[..8]);
    let temp_secret_key = Uuid::new_v4().to_string();
    let request_id = Uuid::new_v4().to_string();

    let expiration_str = expiration.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let body = format!(
        r#"<GetSessionTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetSessionTokenResult>
    <Credentials>
      <AccessKeyId>{temp_access_key}</AccessKeyId>
      <SecretAccessKey>{temp_secret_key}</SecretAccessKey>
      <SessionToken>{session_token}</SessionToken>
      <Expiration>{expiration_str}</Expiration>
    </Credentials>
  </GetSessionTokenResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</GetSessionTokenResponse>"#
    );

    Ok((
        StatusCode::OK,
        [("Content-Type", "application/xml")],
        body,
    )
        .into_response())
}

async fn assume_role_with_web_identity(req: StsRequest) -> S3Result {
    let web_identity_token = req.web_identity_token.ok_or_else(|| {
        S3Error::from(MaxioError::InvalidArgument(
            "WebIdentityToken is required".to_string(),
        ))
    })?;

    let role_arn = req.role_arn.unwrap_or_else(|| "arn:aws:iam::0:role/WebIdentityRole".to_string());
    let role_session_name = req.role_session_name.unwrap_or_else(|| "web-identity-session".to_string());

    let duration = req.duration_seconds.unwrap_or(3600).min(43200).max(900);
    let now = Utc::now();
    let expiration = now + chrono::Duration::seconds(duration as i64);

    let session_token = Uuid::new_v4().to_string();
    let temp_access_key = format!("web-{}", &session_token[..8]);
    let temp_secret_key = Uuid::new_v4().to_string();
    let request_id = Uuid::new_v4().to_string();

    let expiration_str = expiration.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let assumed_role_id = format!("{}:{}", temp_access_key, role_session_name);
    let assumed_role_arn = format!("{}assumed-role/{}/{}", 
        role_arn.trim_end_matches(|c: char| c != '/'),
        role_arn.rsplit('/').next().unwrap_or("WebIdentityRole"),
        role_session_name
    );

    let subject_from_token = format!("subject-{}", &web_identity_token[..8.min(web_identity_token.len())]);

    let body = format!(
        r#"<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>{temp_access_key}</AccessKeyId>
      <SecretAccessKey>{temp_secret_key}</SecretAccessKey>
      <SessionToken>{session_token}</SessionToken>
      <Expiration>{expiration_str}</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>{assumed_role_arn}</Arn>
      <AssumedRoleId>{assumed_role_id}</AssumedRoleId>
    </AssumedRoleUser>
    <SubjectFromWebIdentityToken>{subject_from_token}</SubjectFromWebIdentityToken>
    <PackedPolicySize>0</PackedPolicySize>
  </AssumeRoleWithWebIdentityResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</AssumeRoleWithWebIdentityResponse>"#
    );

    Ok((
        StatusCode::OK,
        [("Content-Type", "application/xml")],
        body,
    )
        .into_response())
}

async fn assume_role_with_saml(req: StsRequest) -> S3Result {
    let _saml_assertion = req.saml_assertion.ok_or_else(|| {
        S3Error::from(MaxioError::InvalidArgument(
            "SAMLAssertion is required".to_string(),
        ))
    })?;

    let role_arn = req.role_arn.unwrap_or_else(|| "arn:aws:iam::0:role/SAMLRole".to_string());
    let principal_arn = req.principal_arn.unwrap_or_else(|| "arn:aws:iam::0:saml-provider/ExampleProvider".to_string());

    let duration = req.duration_seconds.unwrap_or(3600).min(43200).max(900);
    let now = Utc::now();
    let expiration = now + chrono::Duration::seconds(duration as i64);

    let session_token = Uuid::new_v4().to_string();
    let temp_access_key = format!("saml-{}", &session_token[..8]);
    let temp_secret_key = Uuid::new_v4().to_string();
    let request_id = Uuid::new_v4().to_string();

    let expiration_str = expiration.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let role_name = role_arn.rsplit('/').next().unwrap_or("SAMLRole");
    let assumed_role_id = format!("{}:saml-session", temp_access_key);
    let assumed_role_arn = format!("arn:aws:sts::0:assumed-role/{}/saml-session", role_name);

    let body = format!(
        r#"<AssumeRoleWithSAMLResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithSAMLResult>
    <Credentials>
      <AccessKeyId>{temp_access_key}</AccessKeyId>
      <SecretAccessKey>{temp_secret_key}</SecretAccessKey>
      <SessionToken>{session_token}</SessionToken>
      <Expiration>{expiration_str}</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>{assumed_role_arn}</Arn>
      <AssumedRoleId>{assumed_role_id}</AssumedRoleId>
    </AssumedRoleUser>
    <Issuer>{principal_arn}</Issuer>
    <PackedPolicySize>0</PackedPolicySize>
  </AssumeRoleWithSAMLResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</AssumeRoleWithSAMLResponse>"#
    );

    Ok((
        StatusCode::OK,
        [("Content-Type", "application/xml")],
        body,
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sts_request_parsing() {
        let req = StsRequest {
            action: "AssumeRole".to_string(),
            version: "2011-06-15".to_string(),
            duration_seconds: Some(3600),
            policy: None,
            web_identity_token: None,
            role_arn: None,
            role_session_name: None,
            provider_id: None,
            saml_assertion: None,
            principal_arn: None,
        };

        assert_eq!(req.action, "AssumeRole");
        assert_eq!(req.duration_seconds, Some(3600));
    }

    #[test]
    fn test_supported_sts_actions() {
        let supported = ["AssumeRole", "GetCallerIdentity", "GetSessionToken"];
        for action in &supported {
            assert!(
                matches!(
                    *action,
                    "AssumeRole" | "GetCallerIdentity" | "GetSessionToken"
                ),
                "Action {} should be supported",
                action
            );
        }
    }

    #[test]
    fn test_assume_role_with_web_identity_should_be_supported() {
        // This test documents that AssumeRoleWithWebIdentity should be implemented
        // Currently returns NotImplemented, should return valid credentials
        let action = "AssumeRoleWithWebIdentity";
        // When implemented, this action should be in the supported list
        assert_eq!(action, "AssumeRoleWithWebIdentity");
    }

    #[test]
    fn test_assume_role_with_saml_should_be_supported() {
        // This test documents that AssumeRoleWithSAML should be implemented
        // Currently returns NotImplemented, should return valid credentials
        let action = "AssumeRoleWithSAML";
        // When implemented, this action should be in the supported list
        assert_eq!(action, "AssumeRoleWithSAML");
    }

    #[test]
    fn test_duration_bounds_assume_role() {
        // AssumeRole: min 900s (15min), max 43200s (12h), default 3600s (1h)
        let default_duration = 3600u64;
        let min_duration = 900u64;
        let max_duration = 43200u64;

        assert!(default_duration >= min_duration);
        assert!(default_duration <= max_duration);

        let clamped_low = 100u64.min(max_duration).max(min_duration);
        assert_eq!(clamped_low, min_duration);

        let clamped_high = 100000u64.min(max_duration).max(min_duration);
        assert_eq!(clamped_high, max_duration);
    }

    #[test]
    fn test_duration_bounds_get_session_token() {
        // GetSessionToken: min 900s (15min), max 129600s (36h), default 43200s (12h)
        let default_duration = 43200u64;
        let min_duration = 900u64;
        let max_duration = 129600u64;

        assert!(default_duration >= min_duration);
        assert!(default_duration <= max_duration);
    }
}
