use bytes::Bytes;

use crate::error::{Result, S3SelectError};
use crate::types::{JsonInputOptions, JsonType};

pub fn parse(data: &[u8], opts: &JsonInputOptions) -> Result<Vec<Vec<String>>> {
    let text = std::str::from_utf8(data)
        .map_err(|e| S3SelectError::JsonParse(format!("invalid UTF-8: {e}")))?;

    match opts.json_type {
        JsonType::Document => parse_document(text),
        JsonType::Lines => parse_lines(text),
    }
}

fn parse_document(text: &str) -> Result<Vec<Vec<String>>> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|e| S3SelectError::JsonParse(e.to_string()))?;

    match value {
        serde_json::Value::Array(arr) => {
            let mut records = Vec::new();
            let mut headers: Option<Vec<String>> = None;

            for item in arr {
                if let serde_json::Value::Object(obj) = item {
                    if headers.is_none() {
                        headers = Some(obj.keys().cloned().collect());
                        records.push(headers.clone().unwrap());
                    }
                    let row: Vec<String> = headers
                        .as_ref()
                        .unwrap()
                        .iter()
                        .map(|k| value_to_string(obj.get(k)))
                        .collect();
                    records.push(row);
                }
            }
            Ok(records)
        }
        serde_json::Value::Object(obj) => {
            let headers: Vec<String> = obj.keys().cloned().collect();
            let row: Vec<String> = headers
                .iter()
                .map(|k| value_to_string(obj.get(k)))
                .collect();
            Ok(vec![headers, row])
        }
        _ => Err(S3SelectError::JsonParse(
            "expected JSON object or array".to_string(),
        )),
    }
}

fn parse_lines(text: &str) -> Result<Vec<Vec<String>>> {
    let mut records = Vec::new();
    let mut headers: Option<Vec<String>> = None;

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: serde_json::Value =
            serde_json::from_str(trimmed).map_err(|e| S3SelectError::JsonParse(e.to_string()))?;

        if let serde_json::Value::Object(obj) = value {
            if headers.is_none() {
                headers = Some(obj.keys().cloned().collect());
                records.push(headers.clone().unwrap());
            }
            let row: Vec<String> = headers
                .as_ref()
                .unwrap()
                .iter()
                .map(|k| value_to_string(obj.get(k)))
                .collect();
            records.push(row);
        }
    }

    Ok(records)
}

fn value_to_string(value: Option<&serde_json::Value>) -> String {
    match value {
        None => String::new(),
        Some(serde_json::Value::Null) => String::new(),
        Some(serde_json::Value::Bool(b)) => b.to_string(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
    }
}

pub fn serialize(records: &[Vec<String>]) -> Result<Bytes> {
    let mut output = Vec::new();

    let headers = records.first();
    for record in records.iter().skip(1) {
        let obj: serde_json::Map<String, serde_json::Value> = headers
            .map(|h| {
                h.iter()
                    .zip(record.iter())
                    .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                    .collect()
            })
            .unwrap_or_default();

        let json =
            serde_json::to_string(&obj).map_err(|e| S3SelectError::JsonParse(e.to_string()))?;
        output.extend_from_slice(json.as_bytes());
        output.push(b'\n');
    }

    Ok(Bytes::from(output))
}
