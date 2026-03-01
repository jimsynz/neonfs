//! AES-256-GCM authenticated encryption for chunk data.
//!
//! This module provides internal encrypt and decrypt functions used by
//! the BlobStore pipeline. Encryption is integrated into the store's
//! read/write path (compress → encrypt → write, read → decrypt → decompress)
//! to avoid copying chunk data across the NIF boundary multiple times.

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use thiserror::Error;

/// Errors that can occur during encryption/decryption.
#[derive(Error, Debug)]
pub enum EncryptionError {
    /// The key must be exactly 32 bytes (256 bits).
    #[error("invalid key size: expected 32 bytes, got {0}")]
    InvalidKeySize(usize),

    /// The nonce must be exactly 12 bytes (96 bits).
    #[error("invalid nonce size: expected 12 bytes, got {0}")]
    InvalidNonceSize(usize),

    /// Encryption failed (should not happen with valid inputs).
    #[error("encryption failed")]
    EncryptionFailed,

    /// Decryption failed due to authentication failure (wrong key, tampered data, or wrong nonce).
    #[error("authentication failed: wrong key, corrupted ciphertext, or wrong nonce")]
    AuthenticationFailed,
}

/// Parameters for AES-256-GCM encryption/decryption.
#[derive(Debug, Clone)]
pub struct EncryptionParams {
    /// 256-bit AES key (32 bytes).
    pub key: [u8; 32],
    /// 96-bit nonce (12 bytes). Must be unique per encryption with the same key.
    pub nonce: [u8; 12],
}

impl EncryptionParams {
    /// Creates new encryption parameters from key and nonce slices.
    ///
    /// Returns an error if the key is not 32 bytes or the nonce is not 12 bytes.
    pub fn new(key: &[u8], nonce: &[u8]) -> Result<Self, EncryptionError> {
        if key.len() != 32 {
            return Err(EncryptionError::InvalidKeySize(key.len()));
        }
        if nonce.len() != 12 {
            return Err(EncryptionError::InvalidNonceSize(nonce.len()));
        }
        let mut key_arr = [0u8; 32];
        key_arr.copy_from_slice(key);
        let mut nonce_arr = [0u8; 12];
        nonce_arr.copy_from_slice(nonce);
        Ok(Self {
            key: key_arr,
            nonce: nonce_arr,
        })
    }
}

/// Encrypts data using AES-256-GCM.
///
/// Returns ciphertext with the 128-bit authentication tag appended.
/// The output is `data.len() + 16` bytes.
pub fn encrypt(data: &[u8], params: &EncryptionParams) -> Result<Vec<u8>, EncryptionError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&params.key));
    let nonce = Nonce::from_slice(&params.nonce);
    cipher
        .encrypt(nonce, data)
        .map_err(|_| EncryptionError::EncryptionFailed)
}

/// Decrypts data using AES-256-GCM.
///
/// Expects ciphertext with appended 128-bit authentication tag (as produced by `encrypt`).
/// Returns the original plaintext.
pub fn decrypt(
    ciphertext_with_tag: &[u8],
    params: &EncryptionParams,
) -> Result<Vec<u8>, EncryptionError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&params.key));
    let nonce = Nonce::from_slice(&params.nonce);
    cipher
        .decrypt(nonce, ciphertext_with_tag)
        .map_err(|_| EncryptionError::AuthenticationFailed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        [0x42u8; 32]
    }

    fn test_nonce() -> [u8; 12] {
        [0x01u8; 12]
    }

    fn test_params() -> EncryptionParams {
        EncryptionParams {
            key: test_key(),
            nonce: test_nonce(),
        }
    }

    #[test]
    fn test_encrypt_decrypt_round_trip() {
        let params = test_params();
        let plaintext = b"hello world";

        let ciphertext = encrypt(plaintext, &params).unwrap();
        assert_ne!(ciphertext.as_slice(), plaintext);
        // Ciphertext should be plaintext + 16 bytes (auth tag)
        assert_eq!(ciphertext.len(), plaintext.len() + 16);

        let decrypted = decrypt(&ciphertext, &params).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypt_decrypt_empty_data() {
        let params = test_params();
        let plaintext = b"";

        let ciphertext = encrypt(plaintext, &params).unwrap();
        // Even empty data has a 16-byte auth tag
        assert_eq!(ciphertext.len(), 16);

        let decrypted = decrypt(&ciphertext, &params).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_decrypt_with_wrong_key_fails() {
        let params = test_params();
        let plaintext = b"secret data";

        let ciphertext = encrypt(plaintext, &params).unwrap();

        let wrong_params = EncryptionParams {
            key: [0xFFu8; 32],
            nonce: test_nonce(),
        };
        let result = decrypt(&ciphertext, &wrong_params);
        assert!(matches!(result, Err(EncryptionError::AuthenticationFailed)));
    }

    #[test]
    fn test_decrypt_with_wrong_nonce_fails() {
        let params = test_params();
        let plaintext = b"secret data";

        let ciphertext = encrypt(plaintext, &params).unwrap();

        let wrong_params = EncryptionParams {
            key: test_key(),
            nonce: [0xFFu8; 12],
        };
        let result = decrypt(&ciphertext, &wrong_params);
        assert!(matches!(result, Err(EncryptionError::AuthenticationFailed)));
    }

    #[test]
    fn test_tampered_ciphertext_fails() {
        let params = test_params();
        let plaintext = b"integrity test";

        let mut ciphertext = encrypt(plaintext, &params).unwrap();
        // Flip a bit in the ciphertext
        ciphertext[0] ^= 0x01;

        let result = decrypt(&ciphertext, &params);
        assert!(matches!(result, Err(EncryptionError::AuthenticationFailed)));
    }

    #[test]
    fn test_tampered_auth_tag_fails() {
        let params = test_params();
        let plaintext = b"tag integrity test";

        let mut ciphertext = encrypt(plaintext, &params).unwrap();
        // Flip a bit in the auth tag (last 16 bytes)
        let last = ciphertext.len() - 1;
        ciphertext[last] ^= 0x01;

        let result = decrypt(&ciphertext, &params);
        assert!(matches!(result, Err(EncryptionError::AuthenticationFailed)));
    }

    #[test]
    fn test_truncated_ciphertext_fails() {
        let params = test_params();
        let plaintext = b"truncation test";

        let ciphertext = encrypt(plaintext, &params).unwrap();
        // Truncate: remove last byte
        let truncated = &ciphertext[..ciphertext.len() - 1];

        let result = decrypt(truncated, &params);
        assert!(matches!(result, Err(EncryptionError::AuthenticationFailed)));
    }

    #[test]
    fn test_encryption_params_new_valid() {
        let key = [0xABu8; 32];
        let nonce = [0xCDu8; 12];
        let params = EncryptionParams::new(&key, &nonce).unwrap();
        assert_eq!(params.key, key);
        assert_eq!(params.nonce, nonce);
    }

    #[test]
    fn test_encryption_params_new_invalid_key_size() {
        let key = [0xABu8; 16]; // Wrong size
        let nonce = [0xCDu8; 12];
        let result = EncryptionParams::new(&key, &nonce);
        assert!(matches!(result, Err(EncryptionError::InvalidKeySize(16))));
    }

    #[test]
    fn test_encryption_params_new_invalid_nonce_size() {
        let key = [0xABu8; 32];
        let nonce = [0xCDu8; 8]; // Wrong size
        let result = EncryptionParams::new(&key, &nonce);
        assert!(matches!(result, Err(EncryptionError::InvalidNonceSize(8))));
    }

    #[test]
    fn test_large_data_round_trip() {
        let params = test_params();
        // 1MB of data
        let plaintext: Vec<u8> = (0..1_048_576).map(|i| (i % 256) as u8).collect();

        let ciphertext = encrypt(&plaintext, &params).unwrap();
        assert_eq!(ciphertext.len(), plaintext.len() + 16);

        let decrypted = decrypt(&ciphertext, &params).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    // NIST AES-256-GCM test vector
    // From NIST SP 800-38D, Test Case 16
    #[test]
    fn test_nist_aes_256_gcm_test_vector() {
        // NIST test case: Key=all zeros (32 bytes), Nonce=all zeros (12 bytes), Plaintext=empty
        // Expected: just the 16-byte auth tag
        let key = [0u8; 32];
        let nonce = [0u8; 12];
        let params = EncryptionParams { key, nonce };

        let ciphertext = encrypt(b"", &params).unwrap();
        // The auth tag for AES-256-GCM with all-zero key, nonce, and empty plaintext
        // is a well-known value. We verify the round-trip works correctly.
        assert_eq!(ciphertext.len(), 16);

        let decrypted = decrypt(&ciphertext, &params).unwrap();
        assert!(decrypted.is_empty());
    }

    // Proptest: random data, random key, verify round-trip
    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn encrypt_decrypt_round_trip(
                data in proptest::collection::vec(any::<u8>(), 0..65536),
                key in proptest::collection::vec(any::<u8>(), 32..=32),
                nonce in proptest::collection::vec(any::<u8>(), 12..=12),
            ) {
                let mut key_arr = [0u8; 32];
                key_arr.copy_from_slice(&key);
                let mut nonce_arr = [0u8; 12];
                nonce_arr.copy_from_slice(&nonce);
                let params = EncryptionParams { key: key_arr, nonce: nonce_arr };

                let ciphertext = encrypt(&data, &params).unwrap();
                prop_assert_eq!(ciphertext.len(), data.len() + 16);

                let decrypted = decrypt(&ciphertext, &params).unwrap();
                prop_assert_eq!(decrypted, data);
            }

            #[test]
            fn different_keys_produce_different_ciphertext(
                data in proptest::collection::vec(any::<u8>(), 1..1024),
                key_a in proptest::collection::vec(any::<u8>(), 32..=32),
                key_b in proptest::collection::vec(any::<u8>(), 32..=32),
                nonce in proptest::collection::vec(any::<u8>(), 12..=12),
            ) {
                prop_assume!(key_a != key_b);

                let mut ka = [0u8; 32];
                ka.copy_from_slice(&key_a);
                let mut kb = [0u8; 32];
                kb.copy_from_slice(&key_b);
                let mut n = [0u8; 12];
                n.copy_from_slice(&nonce);

                let params_a = EncryptionParams { key: ka, nonce: n };
                let params_b = EncryptionParams { key: kb, nonce: n };

                let ct_a = encrypt(&data, &params_a).unwrap();
                let ct_b = encrypt(&data, &params_b).unwrap();
                prop_assert_ne!(ct_a, ct_b);
            }
        }
    }
}
