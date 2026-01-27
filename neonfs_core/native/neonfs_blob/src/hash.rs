//! SHA-256 hashing module for content-addressed storage.
//!
//! This module provides the core hashing functionality for NeonFS chunks.
//! Hashes are always computed on original (uncompressed, unencrypted) data
//! to enable deduplication across the storage system.

use sha2::{Digest, Sha256};
use std::fmt;
use thiserror::Error;

/// Errors that can occur during hash operations.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum HashError {
    /// The hex string has an invalid length (must be 64 characters).
    #[error("invalid hex length: expected 64 characters, got {0}")]
    InvalidHexLength(usize),

    /// The hex string contains invalid characters.
    #[error("invalid hex character at position {0}")]
    InvalidHexChar(usize),
}

/// A SHA-256 hash value (32 bytes).
///
/// This type wraps a 32-byte array representing a SHA-256 hash.
/// It is the primary identifier for content-addressed chunks in NeonFS.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Hash([u8; 32]);

impl Hash {
    /// Creates a Hash from raw bytes.
    ///
    /// # Arguments
    /// * `bytes` - A 32-byte array containing the hash value.
    #[inline]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the hash as a byte slice reference.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Converts the hash to a lowercase hexadecimal string.
    ///
    /// # Returns
    /// A 64-character lowercase hex string.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parses a hash from a hexadecimal string.
    ///
    /// # Arguments
    /// * `s` - A 64-character hex string (case-insensitive).
    ///
    /// # Errors
    /// Returns `HashError::InvalidHexLength` if the string is not 64 characters.
    /// Returns `HashError::InvalidHexChar` if the string contains non-hex characters.
    pub fn from_hex(s: &str) -> Result<Self, HashError> {
        if s.len() != 64 {
            return Err(HashError::InvalidHexLength(s.len()));
        }

        let bytes = hex::decode(s).map_err(|e| match e {
            hex::FromHexError::InvalidHexCharacter { index, .. } => {
                HashError::InvalidHexChar(index)
            }
            hex::FromHexError::OddLength | hex::FromHexError::InvalidStringLength => {
                HashError::InvalidHexLength(s.len())
            }
        })?;
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({})", self.to_hex())
    }
}

/// Computes the SHA-256 hash of the given data.
///
/// This is the primary hashing function used for content-addressed storage.
/// The hash is always computed on original (uncompressed, unencrypted) data
/// to enable deduplication.
///
/// # Arguments
/// * `data` - The data to hash.
///
/// # Returns
/// A `Hash` containing the SHA-256 digest.
pub fn sha256(data: &[u8]) -> Hash {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&result);
    Hash(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test vector: empty string
    // SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    const EMPTY_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    // Test vector: "hello world"
    // SHA-256("hello world") = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
    const HELLO_WORLD_HASH: &str =
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";

    #[test]
    fn test_sha256_empty() {
        let hash = sha256(b"");
        assert_eq!(hash.to_hex(), EMPTY_HASH);
    }

    #[test]
    fn test_sha256_hello_world() {
        let hash = sha256(b"hello world");
        assert_eq!(hash.to_hex(), HELLO_WORLD_HASH);
    }

    #[test]
    fn test_hash_to_hex() {
        let hash = sha256(b"test");
        let hex = hash.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
        // Should be lowercase
        assert!(hex.chars().all(|c| !c.is_ascii_uppercase()));
    }

    #[test]
    fn test_hash_from_hex_valid() {
        let hash = Hash::from_hex(EMPTY_HASH).unwrap();
        assert_eq!(hash.to_hex(), EMPTY_HASH);
    }

    #[test]
    fn test_hash_from_hex_uppercase() {
        let hash = Hash::from_hex(&EMPTY_HASH.to_uppercase()).unwrap();
        assert_eq!(hash.to_hex(), EMPTY_HASH);
    }

    #[test]
    fn test_hash_from_hex_invalid_length_short() {
        let result = Hash::from_hex("abc123");
        assert!(matches!(result, Err(HashError::InvalidHexLength(6))));
    }

    #[test]
    fn test_hash_from_hex_invalid_length_long() {
        let long = format!("{}00", EMPTY_HASH);
        let result = Hash::from_hex(&long);
        assert!(matches!(result, Err(HashError::InvalidHexLength(66))));
    }

    #[test]
    fn test_hash_from_hex_invalid_chars() {
        let invalid = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        let result = Hash::from_hex(invalid);
        // The first 'z' is at position 0
        assert!(matches!(result, Err(HashError::InvalidHexChar(0))));
    }

    #[test]
    fn test_hash_from_bytes() {
        let bytes = [0u8; 32];
        let hash = Hash::from_bytes(bytes);
        assert_eq!(
            hash.to_hex(),
            "0000000000000000000000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn test_hash_as_bytes() {
        let hash = sha256(b"test");
        let bytes = hash.as_bytes();
        assert_eq!(bytes.len(), 32);
        // Round-trip through from_bytes
        let hash2 = Hash::from_bytes(*bytes);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_display() {
        let hash = Hash::from_hex(EMPTY_HASH).unwrap();
        assert_eq!(format!("{}", hash), EMPTY_HASH);
    }

    #[test]
    fn test_hash_debug() {
        let hash = Hash::from_hex(EMPTY_HASH).unwrap();
        assert_eq!(format!("{:?}", hash), format!("Hash({})", EMPTY_HASH));
    }

    #[test]
    fn test_hash_equality() {
        let hash1 = sha256(b"test");
        let hash2 = sha256(b"test");
        let hash3 = sha256(b"different");
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_hash_clone() {
        let hash1 = sha256(b"test");
        let hash2 = hash1;
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_in_hashmap() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        let hash = sha256(b"key");
        map.insert(hash, "value");
        assert_eq!(map.get(&hash), Some(&"value"));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_hex_roundtrip(bytes: [u8; 32]) {
            let hash = Hash::from_bytes(bytes);
            let hex = hash.to_hex();
            let hash2 = Hash::from_hex(&hex).unwrap();
            prop_assert_eq!(hash, hash2);
        }

        #[test]
        fn test_sha256_deterministic(data: Vec<u8>) {
            let hash1 = sha256(&data);
            let hash2 = sha256(&data);
            prop_assert_eq!(hash1, hash2);
        }

        #[test]
        fn test_to_hex_length(bytes: [u8; 32]) {
            let hash = Hash::from_bytes(bytes);
            prop_assert_eq!(hash.to_hex().len(), 64);
        }
    }
}
