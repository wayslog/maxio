use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{Response, StatusCode},
};
use bytes::Bytes;
use maxio_s3select::{
    InputFormat, OutputFormat, SelectRequest,
    types::{CsvInputOptions, FileHeaderInfo, JsonInputOptions, JsonType},
};
use maxio_storage::traits::ObjectLayer;
use quick_xml::de::from_str;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SelectObjectContentRequest {
    expression: String,
    expression_type: String,
    input_serialization: InputSerialization,
    output_serialization: OutputSerialization,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InputSerialization {
    #[serde(rename = "CSV")]
    csv: Option<CsvInput>,
    #[serde(rename = "JSON")]
    json: Option<JsonInput>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CsvInput {
    file_header_info: Option<String>,
    field_delimiter: Option<String>,
    quote_character: Option<String>,
    record_delimiter: Option<String>,
    comments: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct JsonInput {
    #[serde(rename = "Type")]
    json_type: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct OutputSerialization {
    #[serde(rename = "CSV")]
    csv: Option<CsvOutput>,
    #[serde(rename = "JSON")]
    json: Option<JsonOutput>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CsvOutput {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct JsonOutput {}

pub async fn select_object_content(
    State(store): State<Arc<dyn ObjectLayer>>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> Response<Body> {
    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "InvalidRequest", "Invalid UTF-8 in request body"),
    };

    let request: SelectObjectContentRequest = match from_str(body_str) {
        Ok(r) => r,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, "InvalidRequest", &format!("Invalid XML: {e}")),
    };

    if request.expression_type != "SQL" {
        return error_response(StatusCode::BAD_REQUEST, "InvalidRequest", "Only SQL expression type is supported");
    }

    let (_, object_data) = match store.get_object(&bucket, &key, None).await {
        Ok(result) => result,
        Err(maxio_common::error::MaxioError::ObjectNotFound { .. }) => {
            return error_response(StatusCode::NOT_FOUND, "NoSuchKey", "The specified key does not exist");
        }
        Err(maxio_common::error::MaxioError::BucketNotFound(_)) => {
            return error_response(StatusCode::NOT_FOUND, "NoSuchBucket", "The specified bucket does not exist");
        }
        Err(e) => {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &e.to_string());
        }
    };

    let input_format = if let Some(csv) = &request.input_serialization.csv {
        InputFormat::Csv(CsvInputOptions {
            field_delimiter: csv.field_delimiter.as_ref().and_then(|s| s.chars().next()).unwrap_or(','),
            quote_character: csv.quote_character.as_ref().and_then(|s| s.chars().next()).unwrap_or('"'),
            file_header_info: match csv.file_header_info.as_deref() {
                Some("USE") => FileHeaderInfo::Use,
                Some("IGNORE") => FileHeaderInfo::Ignore,
                _ => FileHeaderInfo::None,
            },
            record_delimiter: csv.record_delimiter.as_ref().and_then(|s| s.chars().next()).unwrap_or('\n'),
            comments: csv.comments.as_ref().and_then(|s| s.chars().next()),
        })
    } else if let Some(json) = &request.input_serialization.json {
        InputFormat::Json(JsonInputOptions {
            json_type: match json.json_type.as_deref() {
                Some("LINES") => JsonType::Lines,
                _ => JsonType::Document,
            },
        })
    } else {
        return error_response(StatusCode::BAD_REQUEST, "InvalidRequest", "No input serialization specified");
    };

    let output_format = if request.output_serialization.csv.is_some() {
        OutputFormat::Csv
    } else if request.output_serialization.json.is_some() {
        OutputFormat::Json
    } else {
        OutputFormat::Csv
    };

    let select_request = SelectRequest {
        expression: request.expression,
        input_format,
        output_format,
    };

    let result = match maxio_s3select::execute_select(&select_request, &object_data) {
        Ok(data) => data,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "InvalidRequest", &e.to_string());
        }
    };

    let payload = build_select_response(&result);

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .header("x-amz-request-id", uuid::Uuid::new_v4().to_string())
        .body(Body::from(payload))
        .unwrap()
}

fn build_select_response(data: &Bytes) -> Vec<u8> {
    let mut response = Vec::new();

    let records_message = build_message(b":message-type", b"event", b":event-type", b"Records", data);
    response.extend_from_slice(&records_message);

    let end_message = build_message(b":message-type", b"event", b":event-type", b"End", &Bytes::new());
    response.extend_from_slice(&end_message);

    response
}

fn build_message(header1_name: &[u8], header1_value: &[u8], header2_name: &[u8], header2_value: &[u8], payload: &Bytes) -> Vec<u8> {
    let mut headers = Vec::new();
    
    headers.push(header1_name.len() as u8);
    headers.extend_from_slice(header1_name);
    headers.push(7);
    headers.extend_from_slice(&(header1_value.len() as u16).to_be_bytes());
    headers.extend_from_slice(header1_value);

    headers.push(header2_name.len() as u8);
    headers.extend_from_slice(header2_name);
    headers.push(7);
    headers.extend_from_slice(&(header2_value.len() as u16).to_be_bytes());
    headers.extend_from_slice(header2_value);

    let headers_len = headers.len() as u32;
    let payload_len = payload.len() as u32;
    let total_len = 4 + 4 + 4 + headers_len + payload_len + 4;

    let mut message = Vec::with_capacity(total_len as usize);
    
    message.extend_from_slice(&total_len.to_be_bytes());
    message.extend_from_slice(&headers_len.to_be_bytes());
    message.extend_from_slice(&[0u8; 4]);
    message.extend_from_slice(&headers);
    message.extend_from_slice(payload);
    message.extend_from_slice(&[0u8; 4]);

    message
}

fn error_response(status: StatusCode, code: &str, message: &str) -> Response<Body> {
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{}</Code>
    <Message>{}</Message>
</Error>"#,
        code, message
    );

    Response::builder()
        .status(status)
        .header("Content-Type", "application/xml")
        .body(Body::from(body))
        .unwrap()
}
