pub mod config;
pub mod compressor;
#[cfg(test)]
mod tests;

pub use config::{CompressionConfig, CompressionAlgorithm};
pub use compressor::Compressor;
