//! Codec suffix: a stable fingerprint identifying the codec tuple a chunk was
//! written with (compression algorithm + level, encryption algorithm + nonce).
//!
//! ## Why
//!
//! Chunks are content-addressed by SHA-256 of the plaintext, but the on-disk
//! bytes depend on compression and encryption settings. Without a discriminator
//! in the path, two volumes writing the same plaintext with different codec
//! settings produce different byte streams that overwrite each other at the
//! same path — a silent data-corruption bug (issue #270).
//!
//! Appending a codec suffix makes different codec variants coexist as separate
//! files. Identical codec tuples share a file (still enables dedup for
//! unencrypted chunks across volumes with matching compression). Encrypted
//! writes with fresh random nonces get unique suffixes and never collide.
//!
//! ## Format
//!
//! `<hash>.<16-hex-chars>` where the 16 hex chars are the first 8 bytes of
//! `sha256` over a canonical byte sequence:
//!
//! ```text
//! version:     u8   = 0x01
//! comp_tag:    u8   (0x00 = None, 0x01 = Zstd)
//! comp_level:  i32 little-endian
//! enc_tag:     u8   (0x00 = None, 0x01 = AesGcm256)
//! nonce:       12 bytes (only when enc_tag != None)
//! ```
//!
//! 8 bytes (64 bits) of collision resistance is vastly more than needed for
//! the ~10 distinct codec tuples a real deployment will ever see; for encrypted
//! writes the 12-byte (96-bit) nonce drives the differentiation.

use crate::compression::Compression;
use crate::encryption::EncryptionParams;
use sha2::{Digest, Sha256};

const VERSION: u8 = 0x01;
const COMP_TAG_NONE: u8 = 0x00;
const COMP_TAG_ZSTD: u8 = 0x01;
const ENC_TAG_NONE: u8 = 0x00;
const ENC_TAG_AES_GCM_256: u8 = 0x01;

/// Length of the hex-encoded codec suffix (first 8 bytes of SHA-256 = 16 hex chars).
pub const SUFFIX_HEX_LEN: usize = 16;

/// Canonical hex suffix identifying a chunk's codec variant.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CodecSuffix(String);

impl CodecSuffix {
    /// The suffix used when a chunk is written without compression and without
    /// encryption. Computed once from the canonical encoding of `(None, None)`.
    pub fn plain() -> Self {
        Self::new(None, None)
    }

    /// Build the suffix from a compression choice and optional encryption
    /// parameters.
    pub fn new(compression: Option<&Compression>, encryption: Option<&EncryptionParams>) -> Self {
        let mut bytes = Vec::with_capacity(1 + 1 + 4 + 1 + 12);
        bytes.push(VERSION);

        match compression {
            None | Some(Compression::None) => {
                bytes.push(COMP_TAG_NONE);
                bytes.extend_from_slice(&0i32.to_le_bytes());
            }
            Some(Compression::Zstd { level }) => {
                bytes.push(COMP_TAG_ZSTD);
                bytes.extend_from_slice(&level.to_le_bytes());
            }
        }

        match encryption {
            None => bytes.push(ENC_TAG_NONE),
            Some(params) => {
                bytes.push(ENC_TAG_AES_GCM_256);
                bytes.extend_from_slice(&params.nonce);
            }
        }

        let digest = Sha256::digest(&bytes);
        Self(hex::encode(&digest[..8]))
    }

    /// The suffix as a lowercase hex string (without leading dot).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CodecSuffix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nonce(byte: u8) -> EncryptionParams {
        EncryptionParams {
            key: [0u8; 32],
            nonce: [byte; 12],
        }
    }

    #[test]
    fn plain_matches_none_none() {
        assert_eq!(CodecSuffix::plain(), CodecSuffix::new(None, None));
        assert_eq!(
            CodecSuffix::plain(),
            CodecSuffix::new(Some(&Compression::None), None)
        );
    }

    #[test]
    fn suffix_is_sixteen_hex_chars() {
        let s = CodecSuffix::plain();
        assert_eq!(s.as_str().len(), SUFFIX_HEX_LEN);
        assert!(s.as_str().chars().all(|c| c.is_ascii_hexdigit()));
        assert!(s.as_str().chars().all(|c| !c.is_ascii_uppercase()));
    }

    #[test]
    fn different_compression_levels_differ() {
        let a = CodecSuffix::new(Some(&Compression::zstd(1)), None);
        let b = CodecSuffix::new(Some(&Compression::zstd(9)), None);
        assert_ne!(a, b);
    }

    #[test]
    fn same_compression_level_matches() {
        let a = CodecSuffix::new(Some(&Compression::zstd(3)), None);
        let b = CodecSuffix::new(Some(&Compression::zstd(3)), None);
        assert_eq!(a, b);
    }

    #[test]
    fn compression_differs_from_plain() {
        assert_ne!(
            CodecSuffix::plain(),
            CodecSuffix::new(Some(&Compression::zstd(3)), None)
        );
    }

    #[test]
    fn encryption_changes_suffix() {
        let plain = CodecSuffix::plain();
        let encrypted = CodecSuffix::new(None, Some(&nonce(0x01)));
        assert_ne!(plain, encrypted);
    }

    #[test]
    fn different_nonces_produce_different_suffixes() {
        let a = CodecSuffix::new(None, Some(&nonce(0x01)));
        let b = CodecSuffix::new(None, Some(&nonce(0x02)));
        assert_ne!(a, b);
    }

    #[test]
    fn same_nonce_different_key_produces_same_suffix() {
        // The key is not part of the canonical encoding (we'd need a key id
        // to include it without leaking secret material). For the bug we're
        // fixing, nonce uniqueness is what matters — fresh random nonces give
        // per-write unique suffixes, which is the property we rely on.
        let a = CodecSuffix::new(
            None,
            Some(&EncryptionParams {
                key: [0xAA; 32],
                nonce: [0x42; 12],
            }),
        );
        let b = CodecSuffix::new(
            None,
            Some(&EncryptionParams {
                key: [0xBB; 32],
                nonce: [0x42; 12],
            }),
        );
        assert_eq!(a, b);
    }

    #[test]
    fn compressed_and_encrypted_combine() {
        let c = CodecSuffix::new(Some(&Compression::zstd(3)), Some(&nonce(0x10)));
        assert_ne!(c, CodecSuffix::new(Some(&Compression::zstd(3)), None));
        assert_ne!(c, CodecSuffix::new(None, Some(&nonce(0x10))));
        assert_ne!(c, CodecSuffix::plain());
    }

    #[test]
    fn display_matches_as_str() {
        let s = CodecSuffix::plain();
        assert_eq!(format!("{}", s), s.as_str());
    }
}
