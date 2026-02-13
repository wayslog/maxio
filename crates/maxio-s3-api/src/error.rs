use axum::response::{IntoResponse, Response};
use http::StatusCode;
use maxio_common::error::MaxioError;

pub struct S3Error(pub MaxioError);

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let error_code = self.0.s3_error_code();
        let message = self.0.to_string();
        let status = match self.0 {
            MaxioError::BucketNotFound(_) | MaxioError::ObjectNotFound { .. } => {
                StatusCode::NOT_FOUND
            }
            MaxioError::BucketAlreadyExists(_) => StatusCode::CONFLICT,
            MaxioError::AccessDenied(_) | MaxioError::SignatureDoesNotMatch => {
                StatusCode::FORBIDDEN
            }
            MaxioError::InvalidBucketName(_)
            | MaxioError::InvalidObjectName(_)
            | MaxioError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            MaxioError::EntityTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            MaxioError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            MaxioError::InternalError(_) | MaxioError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{error_code}</Code>
  <Message>{message}</Message>
  <Resource>/</Resource>
  <RequestId>0</RequestId>
</Error>"#
        );

        (status, [("Content-Type", "application/xml")], body).into_response()
    }
}

impl From<MaxioError> for S3Error {
    fn from(err: MaxioError) -> Self {
        S3Error(err)
    }
}
