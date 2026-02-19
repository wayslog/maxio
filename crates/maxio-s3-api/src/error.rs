use axum::response::{IntoResponse, Response};
use http::StatusCode;
use maxio_common::error::MaxioError;
use maxio_common::request_id;

pub struct S3Error(pub MaxioError);

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let error_code = self.0.s3_error_code();
        let message = self.0.to_string();
        let status = match self.0 {
            MaxioError::BucketNotFound(_)
            | MaxioError::ObjectNotFound { .. }
            | MaxioError::NoSuchBucketPolicy(_)
            | MaxioError::NoSuchCORSConfiguration(_)
            | MaxioError::NoSuchWebsiteConfiguration(_)
            | MaxioError::NoSuchPublicAccessBlockConfiguration(_)
            | MaxioError::OwnershipControlsNotFound(_)
            | MaxioError::ServerSideEncryptionConfigurationNotFound(_)
            | MaxioError::ObjectLockConfigurationNotFound(_)
            | MaxioError::NoSuchUpload(_)
            | MaxioError::NoSuchLifecycleConfiguration(_)
            | MaxioError::NoSuchTagSet(_)
            | MaxioError::NoSuchNotificationConfiguration(_)
            | MaxioError::NoSuchReplicationConfiguration(_) => StatusCode::NOT_FOUND,
            MaxioError::BucketAlreadyExists(_)
            | MaxioError::BucketAlreadyOwnedByYou(_)
            | MaxioError::BucketNotEmpty(_) => StatusCode::CONFLICT,
            MaxioError::AccessDenied(_) | MaxioError::SignatureDoesNotMatch => {
                StatusCode::FORBIDDEN
            }
            MaxioError::InvalidBucketName(_)
            | MaxioError::InvalidObjectName(_)
            | MaxioError::InvalidArgument(_)
            | MaxioError::InvalidRequest(_)
            | MaxioError::InvalidPart
            | MaxioError::InvalidPartOrder
            | MaxioError::MalformedXML(_)
            | MaxioError::XMinioInvalidObjectName(_) => StatusCode::BAD_REQUEST,
            MaxioError::EntityTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            MaxioError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            MaxioError::MethodNotAllowed(_) => StatusCode::METHOD_NOT_ALLOWED,
            MaxioError::InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE,
            MaxioError::PreconditionFailed(_) => StatusCode::PRECONDITION_FAILED,
            MaxioError::NotModified => StatusCode::NOT_MODIFIED,
            MaxioError::SlowDown => StatusCode::SERVICE_UNAVAILABLE,
            MaxioError::InternalError(_) | MaxioError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let request_id = request_id::generate_request_id();
        let host_id = request_id::generate_host_id();

        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{error_code}</Code>
  <Message>{message}</Message>
  <Resource>/</Resource>
  <RequestId>{request_id}</RequestId>
  <HostId>{host_id}</HostId>
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
