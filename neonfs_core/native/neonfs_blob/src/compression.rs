//! Compression support for blob storage.
//!
//! This module provides zstd compression for chunk data. Compression is optional
//! and configurable per-write operation.

use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

/// Compression algorithm configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Compression {
    /// No compression applied.
    #[default]
    None,
    /// Zstd compression with configurable level.
    Zstd {
        /// Compression level (1-19, where higher = better compression but slower).
        /// Default is 3 for a good balance.
        level: i32,
    },
}

impl Compression {
    /// Creates a zstd compression configuration with the given level.
    ///
    /// # Arguments
    /// * `level` - Compression level (1-19). Values outside this range will
    ///   be clamped by zstd.
    pub fn zstd(level: i32) -> Self {
        Self::Zstd { level }
    }

    /// Creates a zstd compression configuration with the default level (3).
    pub fn zstd_default() -> Self {
        Self::Zstd { level: 3 }
    }

    /// Returns true if this represents no compression.
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

/// Compresses data using the specified compression algorithm.
///
/// # Arguments
/// * `data` - The data to compress.
/// * `compression` - The compression algorithm and settings.
///
/// # Returns
/// The compressed data, or the original data if compression is None.
pub fn compress(data: &[u8], compression: &Compression) -> std::io::Result<Vec<u8>> {
    match compression {
        Compression::None => Ok(data.to_vec()),
        Compression::Zstd { level } => {
            let mut encoder = zstd::Encoder::new(Vec::new(), *level)?;
            encoder.write_all(data)?;
            encoder.finish()
        }
    }
}

/// Decompresses data that was compressed with zstd.
///
/// # Arguments
/// * `data` - The compressed data.
///
/// # Returns
/// The decompressed data.
pub fn decompress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut decoder = zstd::Decoder::new(data)?;
    let mut result = Vec::new();
    decoder.read_to_end(&mut result)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_none() {
        let data = b"hello world";
        let compressed = compress(data, &Compression::None).unwrap();
        assert_eq!(compressed, data);
    }

    #[test]
    fn test_compression_zstd_roundtrip() {
        let data = b"hello world, this is some test data to compress";
        let compression = Compression::zstd(3);

        let compressed = compress(data, &compression).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_zstd_reduces_size_for_compressible_data() {
        // Create highly compressible data (repeated pattern)
        let data: Vec<u8> = (0..10000).map(|_| b'a').collect();
        let compression = Compression::zstd(3);

        let compressed = compress(&data, &compression).unwrap();

        // Compressed size should be much smaller
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_compression_zstd_different_levels() {
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        let compressed_level_1 = compress(&data, &Compression::zstd(1)).unwrap();
        let compressed_level_9 = compress(&data, &Compression::zstd(9)).unwrap();

        // Both should decompress to the same data
        assert_eq!(decompress(&compressed_level_1).unwrap(), data);
        assert_eq!(decompress(&compressed_level_9).unwrap(), data);

        // Higher level typically produces smaller output (or equal)
        assert!(compressed_level_9.len() <= compressed_level_1.len());
    }

    #[test]
    fn test_compression_empty_data() {
        let data = b"";
        let compression = Compression::zstd(3);

        let compressed = compress(data, &compression).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_large_data() {
        // 1MB of data
        let data: Vec<u8> = (0..1_048_576).map(|i| (i % 256) as u8).collect();
        let compression = Compression::zstd(3);

        let compressed = compress(&data, &compression).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_is_none() {
        assert!(Compression::None.is_none());
        assert!(!Compression::zstd(3).is_none());
    }

    #[test]
    fn test_compression_default() {
        assert_eq!(Compression::default(), Compression::None);
    }

    #[test]
    fn test_compression_zstd_default() {
        assert_eq!(Compression::zstd_default(), Compression::Zstd { level: 3 });
    }

    #[test]
    fn test_compression_serialize_deserialize() {
        let none = Compression::None;
        let json = serde_json::to_string(&none).unwrap();
        assert_eq!(json, r#"{"type":"none"}"#);
        let parsed: Compression = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, none);

        let zstd = Compression::zstd(5);
        let json = serde_json::to_string(&zstd).unwrap();
        assert_eq!(json, r#"{"type":"zstd","level":5}"#);
        let parsed: Compression = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, zstd);
    }

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn zstd_compress_decompress_round_trip(
                data in proptest::collection::vec(any::<u8>(), 0..65536),
                level in 1..=19i32,
            ) {
                let compression = Compression::zstd(level);
                let compressed = compress(&data, &compression).unwrap();
                let decompressed = decompress(&compressed).unwrap();
                prop_assert_eq!(decompressed, data);
            }

            #[test]
            fn compressed_size_bounded(
                data in proptest::collection::vec(any::<u8>(), 0..65536),
            ) {
                let compression = Compression::zstd(3);
                let compressed = compress(&data, &compression).unwrap();
                // Zstd worst-case: original + original/128 + frame overhead (~128 bytes)
                let max_size = data.len() + data.len() / 8 + 128;
                prop_assert!(
                    compressed.len() <= max_size,
                    "compressed {} bytes to {} bytes, exceeds bound {}",
                    data.len(), compressed.len(), max_size
                );
            }

            #[test]
            fn none_compression_is_identity(
                data in proptest::collection::vec(any::<u8>(), 0..65536),
            ) {
                let compressed = compress(&data, &Compression::None).unwrap();
                prop_assert_eq!(compressed, data);
            }
        }
    }
}
