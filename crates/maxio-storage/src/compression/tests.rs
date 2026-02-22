#[cfg(test)]
mod tests {
    use crate::compression::{CompressionAlgorithm, CompressionConfig, Compressor};
    use bytes::Bytes;

    #[test]
    fn test_compression_algorithms() {
        assert_ne!(CompressionAlgorithm::None, CompressionAlgorithm::Gzip);
        assert_ne!(CompressionAlgorithm::Gzip, CompressionAlgorithm::Zstd);
        assert_ne!(CompressionAlgorithm::Zstd, CompressionAlgorithm::Lz4);
        assert_ne!(CompressionAlgorithm::Lz4, CompressionAlgorithm::Snappy);
    }

    #[test]
    fn test_default_config() {
        let config = CompressionConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.algorithm, CompressionAlgorithm::Zstd);
        assert_eq!(config.level, 3);
        assert_eq!(config.min_size, 1024);
    }

    #[test]
    fn test_should_compress_disabled() {
        let config = CompressionConfig::default();
        assert!(!config.should_compress(10000, Some("text/plain"), "file.txt"));
    }

    #[test]
    fn test_should_compress_by_size() {
        let config = CompressionConfig {
            enabled: true,
            algorithm: CompressionAlgorithm::Zstd,
            level: 3,
            min_size: 1024,
            extensions: vec![],
            mime_types: vec![],
        };

        assert!(!config.should_compress(512, None, "file.txt"));
        assert!(config.should_compress(2048, None, "file.txt"));
    }

    #[test]
    fn test_should_compress_by_extension() {
        let config = CompressionConfig {
            enabled: true,
            algorithm: CompressionAlgorithm::Gzip,
            level: 6,
            min_size: 0,
            extensions: vec![".txt".to_string(), ".json".to_string()],
            mime_types: vec![],
        };

        assert!(config.should_compress(100, None, "data.txt"));
        assert!(config.should_compress(100, None, "config.json"));
        assert!(!config.should_compress(100, None, "image.png"));
    }

    #[test]
    fn test_should_compress_by_mime_type() {
        let config = CompressionConfig {
            enabled: true,
            algorithm: CompressionAlgorithm::Lz4,
            level: 1,
            min_size: 0,
            extensions: vec![],
            mime_types: vec!["text/".to_string(), "application/json".to_string()],
        };

        assert!(config.should_compress(100, Some("text/plain"), "file"));
        assert!(config.should_compress(100, Some("text/csv"), "file"));
        assert!(config.should_compress(100, Some("application/json"), "file"));
        assert!(!config.should_compress(100, Some("image/png"), "file"));
    }

    #[test]
    fn test_compressor_disabled() {
        let config = CompressionConfig::default();
        let compressor = Compressor::new(config);

        let data = Bytes::from("test data");
        let result = compressor.compress(data.clone());

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_compressor_algorithm() {
        let mut config = CompressionConfig::default();
        config.enabled = true;
        config.algorithm = CompressionAlgorithm::Zstd;

        let compressor = Compressor::new(config);
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_gzip_compress() {
        let config = CompressionConfig {
            enabled: true,
            algorithm: CompressionAlgorithm::Gzip,
            level: 6,
            min_size: 0,
            extensions: vec![],
            mime_types: vec![],
        };

        let compressor = Compressor::new(config);
        let data = Bytes::from("test data to compress");
        let result = compressor.compress(data);

        assert!(result.is_ok(), "Gzip compression should succeed");
    }

    #[test]
    fn test_zstd_compress() {
        let config = CompressionConfig {
            enabled: true,
            algorithm: CompressionAlgorithm::Zstd,
            level: 3,
            min_size: 0,
            extensions: vec![],
            mime_types: vec![],
        };

        let compressor = Compressor::new(config);
        let data = Bytes::from("test data to compress");
        let result = compressor.compress(data);

        assert!(result.is_ok(), "Zstd compression should succeed");
    }

    #[test]
    fn test_lz4_compress() {
        let config = CompressionConfig {
            enabled: true,
            algorithm: CompressionAlgorithm::Lz4,
            level: 1,
            min_size: 0,
            extensions: vec![],
            mime_types: vec![],
        };

        let compressor = Compressor::new(config);
        let data = Bytes::from("test data to compress");
        let result = compressor.compress(data);

        assert!(result.is_ok(), "Lz4 compression should succeed");
    }

    #[test]
    fn test_snappy_compress() {
        let config = CompressionConfig {
            enabled: true,
            algorithm: CompressionAlgorithm::Snappy,
            level: 0,
            min_size: 0,
            extensions: vec![],
            mime_types: vec![],
        };

        let compressor = Compressor::new(config);
        let data = Bytes::from("test data to compress");
        let result = compressor.compress(data);

        assert!(result.is_ok(), "Snappy compression should succeed");
    }

    #[test]
    fn test_roundtrip_gzip() {
        let config = CompressionConfig {
            enabled: true,
            algorithm: CompressionAlgorithm::Gzip,
            level: 6,
            min_size: 0,
            extensions: vec![],
            mime_types: vec![],
        };

        let compressor = Compressor::new(config);
        let original = Bytes::from("test data for roundtrip");

        let compressed = compressor.compress(original.clone()).unwrap();
        let decompressed = compressor
            .decompress(compressed, CompressionAlgorithm::Gzip)
            .unwrap();

        assert_eq!(
            original, decompressed,
            "Gzip roundtrip should preserve data"
        );
    }

    #[test]
    fn test_roundtrip_zstd() {
        let config = CompressionConfig {
            enabled: true,
            algorithm: CompressionAlgorithm::Zstd,
            level: 3,
            min_size: 0,
            extensions: vec![],
            mime_types: vec![],
        };

        let compressor = Compressor::new(config);
        let original = Bytes::from("test data for roundtrip");

        let compressed = compressor.compress(original.clone()).unwrap();
        let decompressed = compressor
            .decompress(compressed, CompressionAlgorithm::Zstd)
            .unwrap();

        assert_eq!(
            original, decompressed,
            "Zstd roundtrip should preserve data"
        );
    }
}
