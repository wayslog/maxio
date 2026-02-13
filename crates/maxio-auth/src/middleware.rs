use std::{future::Future, pin::Pin, sync::Arc, task::Poll};

use axum::{
    body::Body,
    response::{IntoResponse, Response},
};
use http::{
    Request, StatusCode,
    header::{AUTHORIZATION, HeaderName},
};
use maxio_common::error::MaxioError;
use tower::{Layer, Service};
use tracing::debug;

use crate::{
    credentials::CredentialProvider,
    parser::parse_auth_header,
    signature_v4::verify_signature,
};

#[derive(Clone)]
pub struct AuthLayer {
    provider: Arc<dyn CredentialProvider>,
}

impl AuthLayer {
    pub fn new(provider: Arc<dyn CredentialProvider>) -> Self {
        Self { provider }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthMiddleware {
            inner,
            provider: Arc::clone(&self.provider),
        }
    }
}

#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    provider: Arc<dyn CredentialProvider>,
}

impl<S, ReqBody> Service<Request<ReqBody>> for AuthMiddleware<S>
where
    S: Service<Request<ReqBody>, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let provider = Arc::clone(&self.provider);

        Box::pin(async move {
            let auth_header = req
                .headers()
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .map(str::trim);

            let Some(auth_header) = auth_header else {
                return inner.call(req).await;
            };

            let parsed = match parse_auth_header(auth_header) {
                Ok(parsed) => parsed,
                Err(err) => {
                    debug!(error = %err, "failed to parse auth header");
                    return Ok(s3_error_response(MaxioError::AccessDenied(
                        "invalid authorization header".to_string(),
                    )));
                }
            };

            if parsed.service != "s3" {
                return Ok(s3_error_response(MaxioError::AccessDenied(
                    "unsupported service in credential scope".to_string(),
                )));
            }

            if !parsed.signed_headers.iter().any(|h| h == "host") {
                return Ok(s3_error_response(MaxioError::AccessDenied(
                    "host must be part of signed headers".to_string(),
                )));
            }

            let Some(credentials) = provider.lookup(&parsed.access_key) else {
                return Ok(s3_error_response(MaxioError::AccessDenied(
                    "access key not found".to_string(),
                )));
            };

            let date_time = req
                .headers()
                .get("x-amz-date")
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|v| !v.is_empty());

            let Some(date_time) = date_time else {
                return Ok(s3_error_response(MaxioError::AccessDenied(
                    "missing x-amz-date".to_string(),
                )));
            };

            if !date_time.starts_with(&parsed.date) {
                return Ok(s3_error_response(MaxioError::SignatureDoesNotMatch));
            }

            let payload_hash = req
                .headers()
                .get("x-amz-content-sha256")
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .unwrap_or("UNSIGNED-PAYLOAD");

            let signed_headers = parsed
                .signed_headers
                .iter()
                .map(|h| h.to_ascii_lowercase())
                .collect::<Vec<_>>();

            let verified = verify_signature(
                &credentials.secret_key,
                req.method().as_str(),
                req.uri().path(),
                req.uri().query().unwrap_or(""),
                req.headers(),
                &signed_headers,
                payload_hash,
                date_time,
                &parsed.date,
                &parsed.region,
                &parsed.signature,
            );

            if !verified {
                return Ok(s3_error_response(MaxioError::SignatureDoesNotMatch));
            }

            inner.call(req).await
        })
    }
}

fn s3_error_response(error: MaxioError) -> Response {
    let status = match error {
        MaxioError::AccessDenied(_) | MaxioError::SignatureDoesNotMatch => StatusCode::FORBIDDEN,
        MaxioError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    let error_code = error.s3_error_code();
    let message = error.to_string();
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{error_code}</Code>
  <Message>{message}</Message>
  <Resource>/</Resource>
  <RequestId>0</RequestId>
</Error>"#
    );

    (
        status,
        [(HeaderName::from_static("content-type"), "application/xml")],
        Body::from(body),
    )
        .into_response()
}
