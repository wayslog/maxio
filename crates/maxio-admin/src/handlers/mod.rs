pub mod health;
pub mod metrics;
pub mod config;
pub mod batch;
pub mod info;
pub mod policy;
pub mod user;

use axum::{Json, http::StatusCode, response::IntoResponse};
use maxio_common::error::MaxioError;

pub struct AdminApiError(pub MaxioError);

impl From<MaxioError> for AdminApiError {
    fn from(value: MaxioError) -> Self {
        Self(value)
    }
}

impl IntoResponse for AdminApiError {
    fn into_response(self) -> axum::response::Response {
        let status = match self.0 {
            MaxioError::AccessDenied(_) | MaxioError::SignatureDoesNotMatch => StatusCode::FORBIDDEN,
            MaxioError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (
            status,
            Json(serde_json::json!({
                "code": self.0.s3_error_code(),
                "message": self.0.to_string(),
            })),
        )
            .into_response()
    }
}
