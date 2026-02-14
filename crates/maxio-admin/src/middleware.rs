use axum::{
    body::Body,
    extract::Request,
    http::{StatusCode, header::AUTHORIZATION},
    middleware::Next,
    response::{IntoResponse, Response},
};
use maxio_auth::{parser::parse_auth_header, signature_v4::verify_signature};
use maxio_common::error::MaxioError;
use tracing::debug;

use crate::AdminSys;

pub async fn admin_auth(admin: axum::extract::State<std::sync::Arc<AdminSys>>, req: Request, next: Next) -> Response {
    let admin = admin.0;

    let auth_header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::trim);

    let Some(auth_header) = auth_header else {
        return json_error(MaxioError::AccessDenied(
            "admin api requires signed request".to_string(),
        ));
    };

    let parsed = match parse_auth_header(auth_header) {
        Ok(parsed) => parsed,
        Err(err) => {
            debug!(error = %err, "failed to parse admin auth header");
            return json_error(MaxioError::AccessDenied(
                "invalid authorization header".to_string(),
            ));
        }
    };

    if parsed.service != "s3" {
        return json_error(MaxioError::AccessDenied(
            "unsupported service in credential scope".to_string(),
        ));
    }

    if !parsed.signed_headers.iter().any(|header| header == "host") {
        return json_error(MaxioError::AccessDenied(
            "host must be part of signed headers".to_string(),
        ));
    }

    let provider = admin.credentials();
    let Some(credentials) = provider.lookup(&parsed.access_key) else {
        return json_error(MaxioError::AccessDenied("access key not found".to_string()));
    };

    let date_time = req
        .headers()
        .get("x-amz-date")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let Some(date_time) = date_time else {
        return json_error(MaxioError::AccessDenied("missing x-amz-date".to_string()));
    };

    if !date_time.starts_with(&parsed.date) {
        return json_error(MaxioError::SignatureDoesNotMatch);
    }

    let payload_hash = req
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("UNSIGNED-PAYLOAD");

    let signed_headers = parsed
        .signed_headers
        .iter()
        .map(|header| header.to_ascii_lowercase())
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
        return json_error(MaxioError::SignatureDoesNotMatch);
    }

    let resource = format!("arn:aws:s3:::admin{}", req.uri().path());
    let action = derive_admin_action(req.method().as_str(), req.uri().path());
    let allowed = provider.is_root_access_key(&parsed.access_key)
        || provider.is_allowed(&parsed.access_key, &action, &resource)
        || provider.is_allowed(&parsed.access_key, "admin:*", &resource);

    if !allowed {
        return json_error(MaxioError::AccessDenied(
            "iam policy denied this admin operation".to_string(),
        ));
    }

    next.run(req).await
}

fn derive_admin_action(method: &str, path: &str) -> String {
    let suffix = path.rsplit('/').next().unwrap_or("unknown");
    format!("admin:{method}:{suffix}")
}

fn json_error(error: MaxioError) -> Response {
    let status = match error {
        MaxioError::AccessDenied(_) | MaxioError::SignatureDoesNotMatch => StatusCode::FORBIDDEN,
        MaxioError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    let body = serde_json::json!({
        "code": error.s3_error_code(),
        "message": error.to_string(),
    });

    (status, Body::from(body.to_string())).into_response()
}
