use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectRequest {
    pub expression: String,
    pub input_format: InputFormat,
    pub output_format: OutputFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InputFormat {
    Csv(CsvInputOptions),
    Json(JsonInputOptions),
    Parquet(ParquetInputOptions),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvInputOptions {
    #[serde(default = "default_delimiter")]
    pub field_delimiter: char,
    #[serde(default = "default_quote")]
    pub quote_character: char,
    #[serde(default)]
    pub file_header_info: FileHeaderInfo,
    #[serde(default = "default_newline")]
    pub record_delimiter: char,
    #[serde(default)]
    pub comments: Option<char>,
}

impl Default for CsvInputOptions {
    fn default() -> Self {
        Self {
            field_delimiter: ',',
            quote_character: '"',
            file_header_info: FileHeaderInfo::default(),
            record_delimiter: '\n',
            comments: None,
        }
    }
}

fn default_delimiter() -> char {
    ','
}
fn default_quote() -> char {
    '"'
}
fn default_newline() -> char {
    '\n'
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum FileHeaderInfo {
    #[default]
    None,
    Use,
    Ignore,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonInputOptions {
    #[serde(default)]
    pub json_type: JsonType,
}

impl Default for JsonInputOptions {
    fn default() -> Self {
        Self {
            json_type: JsonType::Document,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum JsonType {
    #[default]
    Document,
    Lines,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParquetInputOptions {
    #[serde(default)]
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum OutputFormat {
    #[default]
    Csv,
    Json,
}

#[derive(Debug, Clone)]
pub struct SelectResult {
    pub records: Vec<Vec<String>>,
}
