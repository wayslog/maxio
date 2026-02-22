use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Gzip,
    Zstd,
    Lz4,
    Snappy,
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    pub enabled: bool,
    pub algorithm: CompressionAlgorithm,
    pub level: i32,
    #[serde(default)]
    pub min_size: u64,
    #[serde(default)]
    pub extensions: Vec<String>,
    #[serde(default)]
    pub mime_types: Vec<String>,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: CompressionAlgorithm::Zstd,
            level: 3,
            min_size: 1024,
            extensions: vec![],
            mime_types: vec![
                "text/plain".to_string(),
                "text/csv".to_string(),
                "application/json".to_string(),
                "application/xml".to_string(),
            ],
        }
    }
}

impl CompressionConfig {
    pub fn should_compress(&self, size: u64, content_type: Option<&str>, key: &str) -> bool {
        if !self.enabled || self.algorithm == CompressionAlgorithm::None {
            return false;
        }

        if size < self.min_size {
            return false;
        }

        if !self.extensions.is_empty() {
            let has_ext = self.extensions.iter().any(|ext| key.ends_with(ext));
            if has_ext {
                return true;
            }
        }

        if let Some(ct) = content_type {
            if !self.mime_types.is_empty() {
                return self.mime_types.iter().any(|mt| ct.starts_with(mt));
            }
        }

        self.extensions.is_empty() && self.mime_types.is_empty()
    }
}
