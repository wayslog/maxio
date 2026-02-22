use thiserror::Error;

#[derive(Debug, Error)]
pub enum S3SelectError {
    #[error("invalid SQL expression: {0}")]
    InvalidSql(String),
    #[error("CSV parse error: {0}")]
    CsvParse(String),
    #[error("JSON parse error: {0}")]
    JsonParse(String),
    #[error("Parquet parse error: {0}")]
    ParquetParse(String),
    #[error("unsupported feature: {0}")]
    Unsupported(String),
    #[error("evaluation error: {0}")]
    Evaluation(String),
}

pub type Result<T> = std::result::Result<T, S3SelectError>;
