use bytes::Bytes;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{Result, S3SelectError};
use crate::types::ParquetInputOptions;

pub fn parse(data: &[u8], opts: &ParquetInputOptions) -> Result<Vec<Vec<String>>> {
    let bytes = Bytes::copy_from_slice(data);

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| S3SelectError::ParquetParse(format!("Failed to read parquet: {e}")))?;

    let schema: SchemaRef = builder.schema().clone();
    let reader = builder
        .build()
        .map_err(|e| S3SelectError::ParquetParse(format!("Failed to build parquet reader: {e}")))?;

    let mut records = Vec::new();

    let headers: Vec<String> = if let Some(ref cols) = opts.columns {
        cols.clone()
    } else {
        schema
            .fields()
            .iter()
            .map(|f: &FieldRef| f.name().clone())
            .collect()
    };
    records.push(headers.clone());

    for batch_result in reader {
        let batch: RecordBatch = batch_result
            .map_err(|e| S3SelectError::ParquetParse(format!("Failed to read batch: {e}")))?;

        let num_rows = batch.num_rows();

        for row_idx in 0..num_rows {
            let mut row = Vec::new();

            for col_name in &headers {
                if let Ok(col_idx) = schema.index_of(col_name) {
                    let column = batch.column(col_idx);
                    let value = array_value_to_string(column, row_idx);
                    row.push(value);
                } else {
                    row.push(String::new());
                }
            }

            records.push(row);
        }
    }

    Ok(records)
}

fn array_value_to_string(array: &ArrayRef, idx: usize) -> String {
    if array.is_null(idx) {
        return String::new();
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_boolean();
            arr.value(idx).to_string()
        }
        DataType::Int8 => {
            let arr = array.as_primitive::<arrow::datatypes::Int8Type>();
            arr.value(idx).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_primitive::<arrow::datatypes::Int16Type>();
            arr.value(idx).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_primitive::<arrow::datatypes::Int32Type>();
            arr.value(idx).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_primitive::<arrow::datatypes::Int64Type>();
            arr.value(idx).to_string()
        }
        DataType::UInt8 => {
            let arr = array.as_primitive::<arrow::datatypes::UInt8Type>();
            arr.value(idx).to_string()
        }
        DataType::UInt16 => {
            let arr = array.as_primitive::<arrow::datatypes::UInt16Type>();
            arr.value(idx).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_primitive::<arrow::datatypes::UInt32Type>();
            arr.value(idx).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_primitive::<arrow::datatypes::UInt64Type>();
            arr.value(idx).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_primitive::<arrow::datatypes::Float32Type>();
            arr.value(idx).to_string()
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<arrow::datatypes::Float64Type>();
            arr.value(idx).to_string()
        }
        DataType::Utf8 => {
            let arr = array.as_string::<i32>();
            arr.value(idx).to_string()
        }
        DataType::LargeUtf8 => {
            let arr = array.as_string::<i64>();
            arr.value(idx).to_string()
        }
        DataType::Binary => {
            let arr = array.as_binary::<i32>();
            format!("{:?}", arr.value(idx))
        }
        DataType::LargeBinary => {
            let arr = array.as_binary::<i64>();
            format!("{:?}", arr.value(idx))
        }
        _ => format!("<unsupported type: {:?}>", array.data_type()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::io::Cursor;
    use std::sync::Arc;

    fn create_test_parquet() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let value_array = Int32Array::from(vec![Some(100), Some(200), None]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        buffer
    }

    #[test]
    fn test_parquet_parse_all_columns() {
        let data = create_test_parquet();
        let opts = ParquetInputOptions::default();

        let result = parse(&data, &opts).unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(result[0], vec!["id", "name", "value"]);
        assert_eq!(result[1][0], "1");
        assert_eq!(result[1][1], "Alice");
        assert_eq!(result[1][2], "100");
    }

    #[test]
    fn test_parquet_parse_selected_columns() {
        let data = create_test_parquet();
        let opts = ParquetInputOptions {
            columns: Some(vec!["name".to_string(), "id".to_string()]),
        };

        let result = parse(&data, &opts).unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(result[0], vec!["name", "id"]);
        assert_eq!(result[1][0], "Alice");
        assert_eq!(result[1][1], "1");
    }

    #[test]
    fn test_parquet_null_handling() {
        let data = create_test_parquet();
        let opts = ParquetInputOptions::default();

        let result = parse(&data, &opts).unwrap();

        assert_eq!(result[3][2], "");
    }

    #[test]
    fn test_parquet_input_options_default() {
        let opts = ParquetInputOptions::default();
        assert!(opts.columns.is_none());
    }
}
