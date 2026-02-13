use thiserror::Error;

#[derive(Debug, Error)]
pub enum MaxioError {
    #[error("bucket not found: {0}")]
    BucketNotFound(String),
    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),
    #[error("object not found: {bucket}/{key}")]
    ObjectNotFound { bucket: String, key: String },
    #[error("invalid bucket name: {0}")]
    InvalidBucketName(String),
    #[error("invalid object name: {0}")]
    InvalidObjectName(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("access denied: {0}")]
    AccessDenied(String),
    #[error("signature does not match")]
    SignatureDoesNotMatch,
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("entity too large: size={size}, max_size={max_size}")]
    EntityTooLarge { size: u64, max_size: u64 },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl MaxioError {
    pub fn s3_error_code(&self) -> &'static str {
        match self {
            Self::BucketNotFound(_) => "NoSuchBucket",
            Self::BucketAlreadyExists(_) => "BucketAlreadyExists",
            Self::ObjectNotFound { .. } => "NoSuchKey",
            Self::InvalidBucketName(_) => "InvalidBucketName",
            Self::InvalidObjectName(_) => "InvalidObjectName",
            Self::InternalError(_) => "InternalError",
            Self::NotImplemented(_) => "NotImplemented",
            Self::AccessDenied(_) => "AccessDenied",
            Self::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            Self::InvalidArgument(_) => "InvalidArgument",
            Self::EntityTooLarge { .. } => "EntityTooLarge",
            Self::Io(_) => "InternalError",
        }
    }
}

pub type Result<T> = std::result::Result<T, MaxioError>;
