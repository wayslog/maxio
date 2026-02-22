use bytes::Bytes;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use maxio_common::error::{MaxioError, Result};
use std::io::{Read, Write};

use super::config::{CompressionAlgorithm, CompressionConfig};

pub struct Compressor {
    config: CompressionConfig,
}

impl Compressor {
    pub fn new(config: CompressionConfig) -> Self {
        Self { config }
    }

    pub fn compress(&self, data: Bytes) -> Result<Bytes> {
        if !self.config.enabled {
            return Ok(data);
        }

        match self.config.algorithm {
            CompressionAlgorithm::None => Ok(data),
            CompressionAlgorithm::Gzip => self.compress_gzip(data),
            CompressionAlgorithm::Zstd => self.compress_zstd(data),
            CompressionAlgorithm::Lz4 => self.compress_lz4(data),
            CompressionAlgorithm::Snappy => self.compress_snappy(data),
        }
    }

    pub fn decompress(&self, data: Bytes, algorithm: CompressionAlgorithm) -> Result<Bytes> {
        match algorithm {
            CompressionAlgorithm::None => Ok(data),
            CompressionAlgorithm::Gzip => self.decompress_gzip(data),
            CompressionAlgorithm::Zstd => self.decompress_zstd(data),
            CompressionAlgorithm::Lz4 => self.decompress_lz4(data),
            CompressionAlgorithm::Snappy => self.decompress_snappy(data),
        }
    }

    pub fn algorithm(&self) -> CompressionAlgorithm {
        if self.config.enabled {
            self.config.algorithm
        } else {
            CompressionAlgorithm::None
        }
    }

    fn compress_gzip(&self, data: Bytes) -> Result<Bytes> {
        let level = self.config.level.clamp(0, 9) as u32;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level));
        encoder
            .write_all(&data)
            .map_err(|e| MaxioError::InternalError(format!("gzip compression failed: {e}")))?;
        let compressed = encoder
            .finish()
            .map_err(|e| MaxioError::InternalError(format!("gzip finish failed: {e}")))?;
        Ok(Bytes::from(compressed))
    }

    fn decompress_gzip(&self, data: Bytes) -> Result<Bytes> {
        let mut decoder = GzDecoder::new(&data[..]);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| MaxioError::InternalError(format!("gzip decompression failed: {e}")))?;
        Ok(Bytes::from(decompressed))
    }

    fn compress_zstd(&self, data: Bytes) -> Result<Bytes> {
        let level = self.config.level.clamp(1, 22);
        let compressed = zstd::encode_all(&data[..], level)
            .map_err(|e| MaxioError::InternalError(format!("zstd compression failed: {e}")))?;
        Ok(Bytes::from(compressed))
    }

    fn decompress_zstd(&self, data: Bytes) -> Result<Bytes> {
        let decompressed = zstd::decode_all(&data[..])
            .map_err(|e| MaxioError::InternalError(format!("zstd decompression failed: {e}")))?;
        Ok(Bytes::from(decompressed))
    }

    fn compress_lz4(&self, data: Bytes) -> Result<Bytes> {
        let compressed = lz4_flex::compress_prepend_size(&data);
        Ok(Bytes::from(compressed))
    }

    fn decompress_lz4(&self, data: Bytes) -> Result<Bytes> {
        let decompressed = lz4_flex::decompress_size_prepended(&data)
            .map_err(|e| MaxioError::InternalError(format!("lz4 decompression failed: {e}")))?;
        Ok(Bytes::from(decompressed))
    }

    fn compress_snappy(&self, data: Bytes) -> Result<Bytes> {
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder
            .compress_vec(&data)
            .map_err(|e| MaxioError::InternalError(format!("snappy compression failed: {e}")))?;
        Ok(Bytes::from(compressed))
    }

    fn decompress_snappy(&self, data: Bytes) -> Result<Bytes> {
        let mut decoder = snap::raw::Decoder::new();
        let decompressed = decoder
            .decompress_vec(&data)
            .map_err(|e| MaxioError::InternalError(format!("snappy decompression failed: {e}")))?;
        Ok(Bytes::from(decompressed))
    }
}
