pub mod csv;
pub mod error;
pub mod json;
pub mod parquet;
pub mod sql;
pub mod types;

pub use error::{Result, S3SelectError};
pub use types::{InputFormat, OutputFormat, ParquetInputOptions, SelectRequest, SelectResult};

use bytes::Bytes;

pub fn execute_select(request: &SelectRequest, data: &[u8]) -> Result<Bytes> {
    let query = sql::parse(&request.expression)?;

    let records: Vec<Vec<String>> = match &request.input_format {
        InputFormat::Csv(opts) => csv::parse(data, opts)?,
        InputFormat::Json(opts) => json::parse(data, opts)?,
        InputFormat::Parquet(opts) => parquet::parse(data, opts)?,
    };

    let filtered = sql::evaluate(&query, records)?;

    match &request.output_format {
        OutputFormat::Csv => csv::serialize(&filtered),
        OutputFormat::Json => json::serialize(&filtered),
    }
}
