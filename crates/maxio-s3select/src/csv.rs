use bytes::Bytes;

use crate::error::{Result, S3SelectError};
use crate::types::{CsvInputOptions, FileHeaderInfo};

pub fn parse(data: &[u8], opts: &CsvInputOptions) -> Result<Vec<Vec<String>>> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(opts.field_delimiter as u8)
        .quote(opts.quote_character as u8)
        .has_headers(matches!(opts.file_header_info, FileHeaderInfo::Use))
        .flexible(true)
        .from_reader(data);

    let headers: Option<Vec<String>> = if matches!(opts.file_header_info, FileHeaderInfo::Use) {
        Some(
            reader
                .headers()
                .map_err(|e| S3SelectError::CsvParse(e.to_string()))?
                .iter()
                .map(|s| s.to_string())
                .collect(),
        )
    } else {
        None
    };

    let mut records = Vec::new();

    if let Some(hdrs) = headers {
        records.push(hdrs);
    }

    for result in reader.records() {
        let record = result.map_err(|e| S3SelectError::CsvParse(e.to_string()))?;

        if let Some(comment_char) = opts.comments {
            if let Some(first) = record.get(0) {
                if first.starts_with(comment_char) {
                    continue;
                }
            }
        }

        records.push(record.iter().map(|s| s.to_string()).collect());
    }

    Ok(records)
}

pub fn serialize(records: &[Vec<String>]) -> Result<Bytes> {
    let mut writer = csv::Writer::from_writer(Vec::new());

    for record in records {
        writer
            .write_record(record)
            .map_err(|e| S3SelectError::CsvParse(e.to_string()))?;
    }

    let data = writer
        .into_inner()
        .map_err(|e| S3SelectError::CsvParse(e.to_string()))?;

    Ok(Bytes::from(data))
}
