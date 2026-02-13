use maxio_common::error::{MaxioError, Result};
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use serde::{Deserialize, Serialize};

pub mod objects;
pub mod storage;

pub const DEFAULT_DATA_SHARDS: usize = 4;
pub const DEFAULT_PARITY_SHARDS: usize = 2;
pub const DEFAULT_BLOCK_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureConfig {
    pub data_shards: usize,
    pub parity_shards: usize,
    pub block_size: usize,
}

impl Default for ErasureConfig {
    fn default() -> Self {
        Self {
            data_shards: DEFAULT_DATA_SHARDS,
            parity_shards: DEFAULT_PARITY_SHARDS,
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }
}

impl ErasureConfig {
    pub fn total_shards(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    pub fn shard_size(&self) -> Result<usize> {
        validate_config(self)?;
        let mut shard_size = self.block_size.div_ceil(self.data_shards);
        if shard_size % 2 != 0 {
            shard_size += 1;
        }
        Ok(shard_size)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureInfo {
    pub data_shards: usize,
    pub parity_shards: usize,
    pub block_size: usize,
    pub total_size: i64,
    pub block_checksums: Vec<String>,
}

pub fn encode_block(data: &[u8], config: &ErasureConfig) -> Result<Vec<Vec<u8>>> {
    validate_config(config)?;

    if data.len() > config.block_size {
        return Err(MaxioError::InvalidArgument(format!(
            "block size {} exceeds configured block_size {}",
            data.len(),
            config.block_size
        )));
    }

    let shard_size = config.shard_size()?;
    let data_payload_size = shard_size * config.data_shards;
    let mut payload = vec![0_u8; data_payload_size];
    payload[..data.len()].copy_from_slice(data);

    let mut encoder = ReedSolomonEncoder::new(config.data_shards, config.parity_shards, shard_size)
        .map_err(map_reed_solomon_error)?;

    let mut shards = Vec::with_capacity(config.total_shards());
    for shard_idx in 0..config.data_shards {
        let start = shard_idx * shard_size;
        let end = start + shard_size;
        let shard = &payload[start..end];
        encoder
            .add_original_shard(shard)
            .map_err(map_reed_solomon_error)?;
        shards.push(shard.to_vec());
    }

    let encoded = encoder.encode().map_err(map_reed_solomon_error)?;
    for recovery in encoded.recovery_iter() {
        shards.push(recovery.to_vec());
    }

    Ok(shards)
}

pub fn decode_block(shards: Vec<Option<Vec<u8>>>, config: &ErasureConfig) -> Result<Vec<u8>> {
    validate_config(config)?;

    if shards.len() != config.total_shards() {
        return Err(MaxioError::InvalidArgument(format!(
            "invalid shard count: expected {}, got {}",
            config.total_shards(),
            shards.len()
        )));
    }

    let shard_size = config.shard_size()?;
    let available_shards = shards.iter().filter(|shard| shard.is_some()).count();
    if available_shards < config.data_shards {
        return Err(MaxioError::InvalidArgument(format!(
            "insufficient shards: need at least {}, got {}",
            config.data_shards, available_shards
        )));
    }

    let mut decoder = ReedSolomonDecoder::new(config.data_shards, config.parity_shards, shard_size)
        .map_err(map_reed_solomon_error)?;

    for (idx, shard) in shards.iter().take(config.data_shards).enumerate() {
        if let Some(bytes) = shard {
            validate_shard_size(idx, bytes, shard_size)?;
            decoder
                .add_original_shard(idx, bytes)
                .map_err(map_reed_solomon_error)?;
        }
    }

    for (parity_idx, shard) in shards.iter().skip(config.data_shards).enumerate() {
        if let Some(bytes) = shard {
            validate_shard_size(config.data_shards + parity_idx, bytes, shard_size)?;
            decoder
                .add_recovery_shard(parity_idx, bytes)
                .map_err(map_reed_solomon_error)?;
        }
    }

    let decoded = decoder.decode().map_err(map_reed_solomon_error)?;
    let mut originals = vec![vec![0_u8; shard_size]; config.data_shards];
    let mut restored = vec![false; config.data_shards];

    for (idx, shard) in shards.iter().take(config.data_shards).enumerate() {
        if let Some(bytes) = shard {
            originals[idx].copy_from_slice(bytes);
            restored[idx] = true;
        }
    }

    for (idx, bytes) in decoded.restored_original_iter() {
        if idx < config.data_shards {
            originals[idx] = bytes.to_vec();
            restored[idx] = true;
        }
    }

    if restored.iter().any(|value| !value) {
        return Err(MaxioError::InternalError(
            "decoder did not restore all original shards".to_string(),
        ));
    }

    let mut block = Vec::with_capacity(config.data_shards * shard_size);
    for shard in originals {
        block.extend_from_slice(&shard);
    }

    Ok(block)
}

fn validate_config(config: &ErasureConfig) -> Result<()> {
    if config.data_shards == 0 {
        return Err(MaxioError::InvalidArgument(
            "data_shards must be greater than zero".to_string(),
        ));
    }
    if config.parity_shards == 0 {
        return Err(MaxioError::InvalidArgument(
            "parity_shards must be greater than zero".to_string(),
        ));
    }
    if config.block_size == 0 {
        return Err(MaxioError::InvalidArgument(
            "block_size must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_shard_size(shard_index: usize, shard: &[u8], expected_size: usize) -> Result<()> {
    if shard.len() != expected_size {
        return Err(MaxioError::InvalidArgument(format!(
            "invalid shard size for shard {}: expected {}, got {}",
            shard_index,
            expected_size,
            shard.len()
        )));
    }
    Ok(())
}

fn map_reed_solomon_error(error: reed_solomon_simd::Error) -> MaxioError {
    MaxioError::InternalError(format!("reed-solomon error: {error}"))
}
