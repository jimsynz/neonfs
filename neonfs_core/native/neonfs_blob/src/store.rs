//! Blob store implementation for content-addressed chunk storage.
//!
//! This module provides the core blob storage operations: writing chunks to disk
//! and reading them back. Writes are atomic (write to temp file, then rename)
//! to prevent partial chunks.

use crate::compression::{compress, decompress, Compression};
use crate::encryption::{self, EncryptionParams};
use crate::error::StoreError;
use crate::hash::{sha256, Hash};
use crate::path::{chunk_path, ensure_parent_dirs, metadata_path, Tier};
use rand::RngExt;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Configuration for a blob store.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Number of prefix directory levels (1=256 dirs, 2=65K dirs).
    pub prefix_depth: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self { prefix_depth: 2 }
    }
}

/// Options for reading chunks from the store.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// If true, verify the chunk data matches the expected hash after reading.
    /// This adds CPU overhead but detects corruption.
    pub verify: bool,
    /// If true, decompress the data after reading.
    /// The Rust layer doesn't track compression state - the caller must know
    /// whether the chunk was compressed.
    pub decompress: bool,
    /// If set, decrypt the data after reading (before decompression).
    /// The read pipeline is: read → decrypt → decompress → verify.
    pub encryption: Option<EncryptionParams>,
}

impl ReadOptions {
    /// Creates options with verification enabled.
    pub fn with_verify() -> Self {
        Self {
            verify: true,
            decompress: false,
            encryption: None,
        }
    }

    /// Creates options with decompression enabled.
    pub fn with_decompress() -> Self {
        Self {
            verify: false,
            decompress: true,
            encryption: None,
        }
    }

    /// Creates options with both verification and decompression enabled.
    pub fn with_verify_and_decompress() -> Self {
        Self {
            verify: true,
            decompress: true,
            encryption: None,
        }
    }
}

/// Options for writing chunks to the store.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Compression to apply to the chunk data before writing.
    pub compression: Option<Compression>,
    /// If set, encrypt the data after compression (before writing).
    /// The write pipeline is: compress → encrypt → write.
    pub encryption: Option<EncryptionParams>,
}

impl WriteOptions {
    /// Creates options with the specified compression.
    pub fn with_compression(compression: Compression) -> Self {
        Self {
            compression: Some(compression),
            encryption: None,
        }
    }
}

/// Information about a written chunk.
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    /// The hash of the original (uncompressed) data.
    pub hash: Hash,
    /// The size of the original data in bytes.
    pub original_size: usize,
    /// The size of the stored data in bytes (may differ if compressed/encrypted).
    pub stored_size: usize,
    /// The compression applied to the chunk.
    pub compression: Compression,
    /// The encryption algorithm used, if any (e.g. "aes-256-gcm").
    pub encryption_algorithm: Option<String>,
    /// The nonce used for encryption, if any (echoed back for metadata storage).
    pub encryption_nonce: Option<Vec<u8>>,
}

/// A content-addressed blob store.
///
/// The `BlobStore` manages chunk storage on disk using a content-addressed
/// directory layout. Chunks are identified by their SHA-256 hash and stored
/// in a sharded directory hierarchy.
#[derive(Debug)]
pub struct BlobStore {
    /// Base directory for all blob storage.
    base_dir: PathBuf,
    /// Configuration for this store.
    config: StoreConfig,
}

impl BlobStore {
    /// Creates a new blob store at the given base directory.
    ///
    /// # Arguments
    /// * `base_dir` - The base directory for blob storage.
    /// * `config` - Configuration for the store.
    ///
    /// # Errors
    /// Returns an error if the base directory cannot be created.
    pub fn new(base_dir: impl AsRef<Path>, config: StoreConfig) -> Result<Self, StoreError> {
        let base_dir = base_dir.as_ref().to_path_buf();

        // Ensure base directory exists
        if !base_dir.exists() {
            fs::create_dir_all(&base_dir).map_err(|e| StoreError::io_error(&base_dir, e))?;
        }

        Ok(Self { base_dir, config })
    }

    /// Returns the base directory for this store.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Returns the configuration for this store.
    pub fn config(&self) -> &StoreConfig {
        &self.config
    }

    /// Computes the path for a chunk.
    fn chunk_path(&self, hash: &Hash, tier: Tier) -> PathBuf {
        chunk_path(&self.base_dir, hash, tier, self.config.prefix_depth)
    }

    /// Writes a chunk to the store atomically without compression.
    ///
    /// The chunk is first written to a temporary file, then renamed to its
    /// final location. This ensures that partial writes never leave corrupt
    /// chunks on disk.
    ///
    /// # Arguments
    /// * `hash` - The SHA-256 hash of the chunk data.
    /// * `data` - The chunk data to write.
    /// * `tier` - The storage tier for this chunk.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    ///
    /// # Note
    /// If a chunk with the same hash already exists, this operation is idempotent
    /// (the existing chunk is replaced with identical content).
    pub fn write_chunk(&self, hash: &Hash, data: &[u8], tier: Tier) -> Result<(), StoreError> {
        self.write_chunk_with_options(hash, data, tier, &WriteOptions::default())?;
        Ok(())
    }

    /// Writes a chunk to the store atomically with configurable options.
    ///
    /// The chunk is first written to a temporary file, then renamed to its
    /// final location. Compression is applied if specified in options.
    ///
    /// # Arguments
    /// * `hash` - The SHA-256 hash of the original (uncompressed) chunk data.
    /// * `data` - The chunk data to write (will be compressed if options specify).
    /// * `tier` - The storage tier for this chunk.
    /// * `options` - Options controlling write behavior (e.g., compression).
    ///
    /// # Returns
    /// `ChunkInfo` with details about the written chunk.
    ///
    /// # Errors
    /// Returns an error if the write or compression fails.
    pub fn write_chunk_with_options(
        &self,
        hash: &Hash,
        data: &[u8],
        tier: Tier,
        options: &WriteOptions,
    ) -> Result<ChunkInfo, StoreError> {
        let final_path = self.chunk_path(hash, tier);
        let temp_path = self.temp_path(&final_path);
        let original_size = data.len();

        // Step 1: Apply compression if specified
        let (compressed_data, compression) = if let Some(ref compression) = options.compression {
            if compression.is_none() {
                (data.to_vec(), Compression::None)
            } else {
                let compressed = compress(data, compression)
                    .map_err(|e| StoreError::io_error(&final_path, e))?;
                (compressed, compression.clone())
            }
        } else {
            (data.to_vec(), Compression::None)
        };

        // Step 2: Apply encryption if specified (after compression)
        let (data_to_write, encryption_algorithm, encryption_nonce) =
            if let Some(ref enc_params) = options.encryption {
                let encrypted = encryption::encrypt(&compressed_data, enc_params)?;
                (
                    encrypted,
                    Some("aes-256-gcm".to_string()),
                    Some(enc_params.nonce.to_vec()),
                )
            } else {
                (compressed_data, None, None)
            };

        let stored_size = data_to_write.len();

        // Ensure parent directories exist
        ensure_parent_dirs(&final_path).map_err(|e| StoreError::io_error(&final_path, e))?;

        // Write to temporary file
        let mut file = File::create(&temp_path).map_err(|e| StoreError::io_error(&temp_path, e))?;
        file.write_all(&data_to_write)
            .map_err(|e| StoreError::io_error(&temp_path, e))?;
        file.sync_all()
            .map_err(|e| StoreError::io_error(&temp_path, e))?;

        // Atomic rename to final path
        fs::rename(&temp_path, &final_path).map_err(|e| StoreError::io_error(&final_path, e))?;

        Ok(ChunkInfo {
            hash: *hash,
            original_size,
            stored_size,
            compression,
            encryption_algorithm,
            encryption_nonce,
        })
    }

    /// Reads a chunk from the store without verification.
    ///
    /// This is a convenience method that calls `read_chunk_with_options` with
    /// verification disabled. For verified reads, use `read_chunk_with_options`
    /// with `ReadOptions::with_verify()`.
    ///
    /// # Arguments
    /// * `hash` - The SHA-256 hash of the chunk to read.
    /// * `tier` - The storage tier to read from.
    ///
    /// # Errors
    /// Returns `ChunkNotFound` if the chunk does not exist.
    /// Returns `IoError` if the read fails.
    pub fn read_chunk(&self, hash: &Hash, tier: Tier) -> Result<Vec<u8>, StoreError> {
        self.read_chunk_with_options(hash, tier, &ReadOptions::default())
    }

    /// Reads a chunk from the store with configurable options.
    ///
    /// # Arguments
    /// * `hash` - The SHA-256 hash of the chunk to read.
    /// * `tier` - The storage tier to read from.
    /// * `options` - Options controlling read behavior (e.g., verification, decompression).
    ///
    /// # Errors
    /// Returns `ChunkNotFound` if the chunk does not exist.
    /// Returns `IoError` if the read or decompression fails.
    /// Returns `CorruptChunk` if verification is enabled and the data doesn't match.
    ///
    /// # Note
    /// When both `decompress` and `verify` are enabled, verification is performed
    /// on the decompressed data (which should match the original hash).
    pub fn read_chunk_with_options(
        &self,
        hash: &Hash,
        tier: Tier,
        options: &ReadOptions,
    ) -> Result<Vec<u8>, StoreError> {
        let path = self.chunk_path(hash, tier);

        if !path.exists() {
            return Err(StoreError::ChunkNotFound(hash.to_hex()));
        }

        let raw_data = fs::read(&path).map_err(|e| StoreError::io_error(&path, e))?;

        // Step 1: Optionally decrypt the data (before decompression)
        let decrypted_data = if let Some(ref enc_params) = options.encryption {
            encryption::decrypt(&raw_data, enc_params)?
        } else {
            raw_data
        };

        // Step 2: Optionally decompress the data
        let data = if options.decompress {
            decompress(&decrypted_data).map_err(|e| StoreError::io_error(&path, e))?
        } else {
            decrypted_data
        };

        // Step 3: Optionally verify the data matches the expected hash
        // Verification is done on the decompressed data (original content)
        if options.verify {
            let actual_hash = sha256(&data);
            if &actual_hash != hash {
                return Err(StoreError::CorruptChunk {
                    expected: hash.to_hex(),
                    actual: actual_hash.to_hex(),
                });
            }
        }

        Ok(data)
    }

    /// Deletes a chunk from the store.
    ///
    /// # Arguments
    /// * `hash` - The SHA-256 hash of the chunk to delete.
    /// * `tier` - The storage tier to delete from.
    ///
    /// # Errors
    /// Returns `ChunkNotFound` if the chunk does not exist.
    /// Returns `IoError` if the delete fails.
    pub fn delete_chunk(&self, hash: &Hash, tier: Tier) -> Result<u64, StoreError> {
        let path = self.chunk_path(hash, tier);

        if !path.exists() {
            return Err(StoreError::ChunkNotFound(hash.to_hex()));
        }

        let file_size = fs::metadata(&path)
            .map_err(|e| StoreError::io_error(&path, e))?
            .len();

        fs::remove_file(&path).map_err(|e| StoreError::io_error(&path, e))?;

        Ok(file_size)
    }

    /// Checks if a chunk exists in the store.
    ///
    /// # Arguments
    /// * `hash` - The SHA-256 hash of the chunk.
    /// * `tier` - The storage tier to check.
    ///
    /// # Returns
    /// `true` if the chunk exists, `false` otherwise.
    pub fn chunk_exists(&self, hash: &Hash, tier: Tier) -> bool {
        self.chunk_path(hash, tier).exists()
    }

    /// Migrates a chunk from one tier to another.
    ///
    /// This operation is atomic: the chunk is written to the destination tier,
    /// verified, and only then deleted from the source tier. If any step fails,
    /// the migration is aborted and the source chunk remains intact.
    ///
    /// # Arguments
    /// * `hash` - The SHA-256 hash of the chunk to migrate.
    /// * `from_tier` - The source storage tier.
    /// * `to_tier` - The destination storage tier.
    ///
    /// # Errors
    /// Returns `ChunkNotFound` if the chunk does not exist in the source tier.
    /// Returns `IoError` if any read/write/delete operation fails.
    /// Returns `CorruptChunk` if verification fails during migration.
    ///
    /// # Note
    /// If `from_tier` equals `to_tier`, this is a no-op and returns Ok(()).
    /// Verification is enabled during migration to ensure data integrity.
    pub fn migrate_chunk(
        &self,
        hash: &Hash,
        from_tier: Tier,
        to_tier: Tier,
    ) -> Result<(), StoreError> {
        // No-op if migrating to the same tier
        if from_tier == to_tier {
            return Ok(());
        }

        // Read from source with verification enabled
        let options = ReadOptions {
            verify: true,
            decompress: false, // Read raw data (may be compressed)
            encryption: None,
        };
        let data = self.read_chunk_with_options(hash, from_tier, &options)?;

        // Write to destination tier
        self.write_chunk(hash, &data, to_tier)?;

        // Verify the destination (belt and suspenders approach)
        let verify_options = ReadOptions {
            verify: true,
            decompress: false,
            encryption: None,
        };
        let _ = self.read_chunk_with_options(hash, to_tier, &verify_options)?;

        // Only delete source after successful destination write and verification
        self.delete_chunk(hash, from_tier)?;

        Ok(())
    }

    /// Re-encrypts a chunk in place: decrypts with old key/nonce, re-encrypts
    /// with new key/nonce, writes back atomically.
    ///
    /// The entire decrypt-old → encrypt-new cycle happens in Rust without any
    /// data crossing the NIF boundary. Compression is preserved (only the
    /// encryption layer changes).
    ///
    /// # Arguments
    /// * `hash` - The SHA-256 hash of the chunk.
    /// * `tier` - The storage tier.
    /// * `old_params` - Encryption parameters used to encrypt the current data.
    /// * `new_params` - Encryption parameters for the new encryption.
    ///
    /// # Returns
    /// The stored size of the re-encrypted chunk.
    pub fn reencrypt_chunk(
        &self,
        hash: &Hash,
        tier: Tier,
        old_params: &EncryptionParams,
        new_params: &EncryptionParams,
    ) -> Result<usize, StoreError> {
        let path = self.chunk_path(hash, tier);

        if !path.exists() {
            return Err(StoreError::ChunkNotFound(hash.to_hex()));
        }

        // Read raw encrypted data from disk
        let raw_data = fs::read(&path).map_err(|e| StoreError::io_error(&path, e))?;

        // Decrypt with old key/nonce (result is compressed data or raw data)
        let intermediate = encryption::decrypt(&raw_data, old_params)?;

        // Re-encrypt with new key/nonce
        let new_encrypted = encryption::encrypt(&intermediate, new_params)?;

        let stored_size = new_encrypted.len();

        // Write back atomically
        let temp_path = self.temp_path(&path);
        let mut file = File::create(&temp_path).map_err(|e| StoreError::io_error(&temp_path, e))?;
        file.write_all(&new_encrypted)
            .map_err(|e| StoreError::io_error(&temp_path, e))?;
        file.sync_all()
            .map_err(|e| StoreError::io_error(&temp_path, e))?;

        fs::rename(&temp_path, &path).map_err(|e| StoreError::io_error(&path, e))?;

        Ok(stored_size)
    }

    // Metadata operations

    /// Computes the path for a metadata file.
    fn metadata_path(&self, segment_id_hex: &str, key_hash: &Hash) -> PathBuf {
        metadata_path(
            &self.base_dir,
            segment_id_hex,
            key_hash,
            self.config.prefix_depth,
        )
    }

    /// Writes metadata to the store atomically.
    ///
    /// Uses the same write-then-rename pattern as chunk writes for crash safety.
    /// No compression is applied to metadata.
    ///
    /// # Arguments
    /// * `segment_id_hex` - Hex string identifying the metadata segment.
    /// * `key_hash` - SHA-256 hash of the metadata key.
    /// * `data` - Raw metadata bytes to write.
    pub fn write_metadata(
        &self,
        segment_id_hex: &str,
        key_hash: &Hash,
        data: &[u8],
    ) -> Result<(), StoreError> {
        let final_path = self.metadata_path(segment_id_hex, key_hash);
        let temp_path = self.temp_path(&final_path);

        ensure_parent_dirs(&final_path).map_err(|e| StoreError::io_error(&final_path, e))?;

        let mut file = File::create(&temp_path).map_err(|e| StoreError::io_error(&temp_path, e))?;
        file.write_all(data)
            .map_err(|e| StoreError::io_error(&temp_path, e))?;
        file.sync_all()
            .map_err(|e| StoreError::io_error(&temp_path, e))?;

        fs::rename(&temp_path, &final_path).map_err(|e| StoreError::io_error(&final_path, e))?;

        Ok(())
    }

    /// Reads metadata from the store.
    ///
    /// # Arguments
    /// * `segment_id_hex` - Hex string identifying the metadata segment.
    /// * `key_hash` - SHA-256 hash of the metadata key.
    ///
    /// # Errors
    /// Returns `MetadataNotFound` if the key does not exist.
    pub fn read_metadata(
        &self,
        segment_id_hex: &str,
        key_hash: &Hash,
    ) -> Result<Vec<u8>, StoreError> {
        let path = self.metadata_path(segment_id_hex, key_hash);

        if !path.exists() {
            return Err(StoreError::MetadataNotFound(key_hash.to_hex()));
        }

        fs::read(&path).map_err(|e| StoreError::io_error(&path, e))
    }

    /// Deletes metadata from the store.
    ///
    /// # Arguments
    /// * `segment_id_hex` - Hex string identifying the metadata segment.
    /// * `key_hash` - SHA-256 hash of the metadata key.
    ///
    /// # Errors
    /// Returns `MetadataNotFound` if the key does not exist.
    pub fn delete_metadata(&self, segment_id_hex: &str, key_hash: &Hash) -> Result<(), StoreError> {
        let path = self.metadata_path(segment_id_hex, key_hash);

        if !path.exists() {
            return Err(StoreError::MetadataNotFound(key_hash.to_hex()));
        }

        fs::remove_file(&path).map_err(|e| StoreError::io_error(&path, e))
    }

    /// Lists all metadata keys in a segment.
    ///
    /// Recursively walks the segment directory and parses leaf filenames as
    /// hex-encoded hashes. Temporary files (`.tmp` suffix) are skipped.
    /// Returns an empty vec for nonexistent segment directories.
    ///
    /// # Arguments
    /// * `segment_id_hex` - Hex string identifying the metadata segment.
    pub fn list_metadata_segment(&self, segment_id_hex: &str) -> Result<Vec<Hash>, StoreError> {
        let segment_dir = self.base_dir.join("meta").join(segment_id_hex);

        if !segment_dir.exists() {
            return Ok(Vec::new());
        }

        let mut hashes = Vec::new();
        Self::walk_metadata_dir(&segment_dir, &mut hashes)?;
        Ok(hashes)
    }

    /// Recursively walks a directory collecting metadata key hashes from leaf files.
    fn walk_metadata_dir(dir: &Path, hashes: &mut Vec<Hash>) -> Result<(), StoreError> {
        let entries = fs::read_dir(dir).map_err(|e| StoreError::io_error(dir, e))?;

        for entry in entries {
            let entry = entry.map_err(|e| StoreError::io_error(dir, e))?;
            let path = entry.path();

            if path.is_dir() {
                Self::walk_metadata_dir(&path, hashes)?;
            } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                // Skip temporary files
                if name.contains(".tmp") {
                    continue;
                }
                // Parse filename as hex hash
                if let Ok(hash) = Hash::from_hex(name) {
                    hashes.push(hash);
                }
            }
        }

        Ok(())
    }

    /// Generates a temporary file path for atomic writes.
    fn temp_path(&self, final_path: &Path) -> PathBuf {
        let random_id: u64 = rand::rng().random();
        let temp_name = format!(
            "{}.tmp.{:016x}",
            final_path.file_name().unwrap_or_default().to_string_lossy(),
            random_id
        );
        final_path.with_file_name(temp_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_store() -> (BlobStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = StoreConfig { prefix_depth: 2 };
        let store = BlobStore::new(temp_dir.path(), config).unwrap();
        (store, temp_dir)
    }

    #[test]
    fn test_new_creates_base_dir() {
        let temp_dir = TempDir::new().unwrap();
        let store_path = temp_dir.path().join("new_store");
        assert!(!store_path.exists());

        let config = StoreConfig::default();
        let store = BlobStore::new(&store_path, config).unwrap();

        assert!(store_path.exists());
        assert_eq!(store.base_dir(), store_path);
    }

    #[test]
    fn test_write_then_read_returns_same_data() {
        let (store, _temp) = create_test_store();
        let data = b"hello world";
        let hash = sha256(data);

        store.write_chunk(&hash, data, Tier::Hot).unwrap();
        let read_data = store.read_chunk(&hash, Tier::Hot).unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_read_nonexistent_chunk_returns_error() {
        let (store, _temp) = create_test_store();
        let hash = sha256(b"nonexistent");

        let result = store.read_chunk(&hash, Tier::Hot);

        assert!(matches!(result, Err(StoreError::ChunkNotFound(_))));
    }

    #[test]
    fn test_delete_removes_chunk() {
        let (store, _temp) = create_test_store();
        let data = b"to be deleted";
        let hash = sha256(data);

        store.write_chunk(&hash, data, Tier::Hot).unwrap();
        assert!(store.chunk_exists(&hash, Tier::Hot));

        store.delete_chunk(&hash, Tier::Hot).unwrap();
        assert!(!store.chunk_exists(&hash, Tier::Hot));
    }

    #[test]
    fn test_delete_nonexistent_returns_error() {
        let (store, _temp) = create_test_store();
        let hash = sha256(b"nonexistent");

        let result = store.delete_chunk(&hash, Tier::Hot);

        assert!(matches!(result, Err(StoreError::ChunkNotFound(_))));
    }

    #[test]
    fn test_chunk_exists_returns_correct_boolean() {
        let (store, _temp) = create_test_store();
        let data = b"test data";
        let hash = sha256(data);

        assert!(!store.chunk_exists(&hash, Tier::Hot));

        store.write_chunk(&hash, data, Tier::Hot).unwrap();

        assert!(store.chunk_exists(&hash, Tier::Hot));
    }

    #[test]
    fn test_write_creates_parent_directories() {
        let (store, _temp) = create_test_store();
        let data = b"test data";
        let hash = sha256(data);

        // The directory structure shouldn't exist yet
        let chunk_path = chunk_path(store.base_dir(), &hash, Tier::Hot, 2);
        assert!(!chunk_path.parent().unwrap().exists());

        store.write_chunk(&hash, data, Tier::Hot).unwrap();

        // Now it should exist
        assert!(chunk_path.exists());
        assert!(chunk_path.parent().unwrap().exists());
    }

    #[test]
    fn test_write_same_hash_is_idempotent() {
        let (store, _temp) = create_test_store();
        let data = b"content addressed data";
        let hash = sha256(data);

        // Write twice - both should succeed
        store.write_chunk(&hash, data, Tier::Hot).unwrap();
        store.write_chunk(&hash, data, Tier::Hot).unwrap();

        // Data should still be correct
        let read_data = store.read_chunk(&hash, Tier::Hot).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_different_tiers_are_independent() {
        let (store, _temp) = create_test_store();
        let data = b"tier test";
        let hash = sha256(data);

        store.write_chunk(&hash, data, Tier::Hot).unwrap();

        assert!(store.chunk_exists(&hash, Tier::Hot));
        assert!(!store.chunk_exists(&hash, Tier::Warm));
        assert!(!store.chunk_exists(&hash, Tier::Cold));
    }

    #[test]
    fn test_large_chunk() {
        let (store, _temp) = create_test_store();
        // 1MB of data
        let data: Vec<u8> = (0..1_048_576).map(|i| (i % 256) as u8).collect();
        let hash = sha256(&data);

        store.write_chunk(&hash, &data, Tier::Hot).unwrap();
        let read_data = store.read_chunk(&hash, Tier::Hot).unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_empty_chunk() {
        let (store, _temp) = create_test_store();
        let data = b"";
        let hash = sha256(data);

        store.write_chunk(&hash, data, Tier::Hot).unwrap();
        let read_data = store.read_chunk(&hash, Tier::Hot).unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_config_default() {
        let config = StoreConfig::default();
        assert_eq!(config.prefix_depth, 2);
    }

    #[test]
    fn test_read_options_default() {
        let options = ReadOptions::default();
        assert!(!options.verify);
        assert!(!options.decompress);
    }

    #[test]
    fn test_read_options_with_verify() {
        let options = ReadOptions::with_verify();
        assert!(options.verify);
        assert!(!options.decompress);
    }

    #[test]
    fn test_read_options_with_decompress() {
        let options = ReadOptions::with_decompress();
        assert!(!options.verify);
        assert!(options.decompress);
    }

    #[test]
    fn test_read_options_with_verify_and_decompress() {
        let options = ReadOptions::with_verify_and_decompress();
        assert!(options.verify);
        assert!(options.decompress);
    }

    #[test]
    fn test_write_options_default() {
        let options = WriteOptions::default();
        assert!(options.compression.is_none());
    }

    #[test]
    fn test_write_options_with_compression() {
        let options = WriteOptions::with_compression(Compression::zstd(5));
        assert_eq!(options.compression, Some(Compression::Zstd { level: 5 }));
    }

    #[test]
    fn test_read_with_verify_valid_chunk_succeeds() {
        let (store, _temp) = create_test_store();
        let data = b"valid data for verification";
        let hash = sha256(data);

        store.write_chunk(&hash, data, Tier::Hot).unwrap();
        let options = ReadOptions::with_verify();
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &options)
            .unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_read_without_verify_valid_chunk_succeeds() {
        let (store, _temp) = create_test_store();
        let data = b"valid data no verification";
        let hash = sha256(data);

        store.write_chunk(&hash, data, Tier::Hot).unwrap();
        let options = ReadOptions::default();
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &options)
            .unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_read_with_verify_corrupt_chunk_fails() {
        let (store, temp_dir) = create_test_store();
        let data = b"original data";
        let hash = sha256(data);

        store.write_chunk(&hash, data, Tier::Hot).unwrap();

        // Manually corrupt the chunk file
        let chunk_path = chunk_path(temp_dir.path(), &hash, Tier::Hot, 2);
        fs::write(&chunk_path, b"corrupted data").unwrap();

        let options = ReadOptions::with_verify();
        let result = store.read_chunk_with_options(&hash, Tier::Hot, &options);

        match result {
            Err(StoreError::CorruptChunk { expected, actual }) => {
                assert_eq!(expected, hash.to_hex());
                assert_ne!(actual, hash.to_hex());
            }
            _ => panic!("Expected CorruptChunk error"),
        }
    }

    #[test]
    fn test_read_without_verify_corrupt_chunk_returns_corrupt_data() {
        let (store, temp_dir) = create_test_store();
        let data = b"original data";
        let hash = sha256(data);

        store.write_chunk(&hash, data, Tier::Hot).unwrap();

        // Manually corrupt the chunk file
        let chunk_path = chunk_path(temp_dir.path(), &hash, Tier::Hot, 2);
        let corrupt_data = b"corrupted data";
        fs::write(&chunk_path, corrupt_data).unwrap();

        // Without verification, we get the corrupt data back without error
        let options = ReadOptions::default();
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &options)
            .unwrap();

        assert_eq!(read_data, corrupt_data);
        assert_ne!(read_data, data);
    }

    #[test]
    fn test_convenience_read_chunk_uses_no_verify() {
        let (store, temp_dir) = create_test_store();
        let data = b"original data for convenience test";
        let hash = sha256(data);

        store.write_chunk(&hash, data, Tier::Hot).unwrap();

        // Corrupt the file
        let chunk_path = chunk_path(temp_dir.path(), &hash, Tier::Hot, 2);
        let corrupt_data = b"corrupted convenience data";
        fs::write(&chunk_path, corrupt_data).unwrap();

        // read_chunk should return corrupt data without error (no verification)
        let read_data = store.read_chunk(&hash, Tier::Hot).unwrap();
        assert_eq!(read_data, corrupt_data);
    }

    // Compression tests

    #[test]
    fn test_write_compressed_read_decompressed_returns_original() {
        let (store, _temp) = create_test_store();
        let data = b"hello world, this is some test data for compression";
        let hash = sha256(data);

        // Write with compression
        let write_options = WriteOptions::with_compression(Compression::zstd(3));
        let chunk_info = store
            .write_chunk_with_options(&hash, data, Tier::Hot, &write_options)
            .unwrap();

        assert_eq!(chunk_info.original_size, data.len());
        assert_eq!(chunk_info.hash, hash);

        // Read with decompression
        let read_options = ReadOptions::with_decompress();
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &read_options)
            .unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_write_uncompressed_read_returns_original() {
        let (store, _temp) = create_test_store();
        let data = b"uncompressed test data";
        let hash = sha256(data);

        // Write without compression
        let write_options = WriteOptions::default();
        let chunk_info = store
            .write_chunk_with_options(&hash, data, Tier::Hot, &write_options)
            .unwrap();

        assert_eq!(chunk_info.original_size, data.len());
        assert_eq!(chunk_info.stored_size, data.len());
        assert_eq!(chunk_info.compression, Compression::None);

        // Read without decompression
        let read_data = store.read_chunk(&hash, Tier::Hot).unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_compression_reduces_size_for_compressible_data() {
        let (store, _temp) = create_test_store();
        // Create highly compressible data (repeated pattern)
        let data: Vec<u8> = (0..10000).map(|_| b'a').collect();
        let hash = sha256(&data);

        let write_options = WriteOptions::with_compression(Compression::zstd(3));
        let chunk_info = store
            .write_chunk_with_options(&hash, &data, Tier::Hot, &write_options)
            .unwrap();

        // Stored size should be smaller than original
        assert!(chunk_info.stored_size < chunk_info.original_size);
    }

    #[test]
    fn test_compression_different_levels() {
        let (store, _temp) = create_test_store();
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let hash = sha256(&data);

        // Write with level 1
        let write_options_1 = WriteOptions::with_compression(Compression::zstd(1));
        let chunk_info_1 = store
            .write_chunk_with_options(&hash, &data, Tier::Hot, &write_options_1)
            .unwrap();

        // Delete and write again with level 9
        store.delete_chunk(&hash, Tier::Hot).unwrap();
        let write_options_9 = WriteOptions::with_compression(Compression::zstd(9));
        let chunk_info_9 = store
            .write_chunk_with_options(&hash, &data, Tier::Hot, &write_options_9)
            .unwrap();

        // Higher level typically produces smaller or equal output
        assert!(chunk_info_9.stored_size <= chunk_info_1.stored_size);
    }

    #[test]
    fn test_compressed_chunk_with_verification() {
        let (store, _temp) = create_test_store();
        let data = b"data for compression and verification";
        let hash = sha256(data);

        // Write compressed
        let write_options = WriteOptions::with_compression(Compression::zstd(3));
        store
            .write_chunk_with_options(&hash, data, Tier::Hot, &write_options)
            .unwrap();

        // Read with decompression and verification
        let read_options = ReadOptions::with_verify_and_decompress();
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &read_options)
            .unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_chunk_info_fields() {
        let (store, _temp) = create_test_store();
        let data = b"test data for chunk info";
        let hash = sha256(data);

        let write_options = WriteOptions::with_compression(Compression::zstd(5));
        let chunk_info = store
            .write_chunk_with_options(&hash, data, Tier::Hot, &write_options)
            .unwrap();

        assert_eq!(chunk_info.hash, hash);
        assert_eq!(chunk_info.original_size, data.len());
        assert_eq!(chunk_info.compression, Compression::Zstd { level: 5 });
        // stored_size can vary
        assert!(chunk_info.stored_size > 0);
    }

    #[test]
    fn test_compression_with_empty_data() {
        let (store, _temp) = create_test_store();
        let data = b"";
        let hash = sha256(data);

        let write_options = WriteOptions::with_compression(Compression::zstd(3));
        let chunk_info = store
            .write_chunk_with_options(&hash, data, Tier::Hot, &write_options)
            .unwrap();

        assert_eq!(chunk_info.original_size, 0);

        // Read with decompression
        let read_options = ReadOptions::with_decompress();
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &read_options)
            .unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_compression_with_large_data() {
        let (store, _temp) = create_test_store();
        // 1MB of data
        let data: Vec<u8> = (0..1_048_576).map(|i| (i % 256) as u8).collect();
        let hash = sha256(&data);

        let write_options = WriteOptions::with_compression(Compression::zstd(3));
        let chunk_info = store
            .write_chunk_with_options(&hash, &data, Tier::Hot, &write_options)
            .unwrap();

        assert_eq!(chunk_info.original_size, 1_048_576);

        // Read with decompression
        let read_options = ReadOptions::with_decompress();
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &read_options)
            .unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_migrate_chunk_hot_to_cold() {
        let (store, _temp) = create_test_store();
        let data = b"data to migrate from hot to cold";
        let hash = sha256(data);

        // Write to hot tier
        store.write_chunk(&hash, data, Tier::Hot).unwrap();
        assert!(store.chunk_exists(&hash, Tier::Hot));
        assert!(!store.chunk_exists(&hash, Tier::Cold));

        // Migrate to cold tier
        store.migrate_chunk(&hash, Tier::Hot, Tier::Cold).unwrap();

        // Verify chunk moved: in cold, not in hot
        assert!(!store.chunk_exists(&hash, Tier::Hot));
        assert!(store.chunk_exists(&hash, Tier::Cold));

        // Verify data integrity
        let read_data = store.read_chunk(&hash, Tier::Cold).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_migrate_chunk_cold_to_hot() {
        let (store, _temp) = create_test_store();
        let data = b"data to migrate from cold to hot";
        let hash = sha256(data);

        // Write to cold tier
        store.write_chunk(&hash, data, Tier::Cold).unwrap();

        // Migrate to hot tier
        store.migrate_chunk(&hash, Tier::Cold, Tier::Hot).unwrap();

        // Verify chunk moved
        assert!(store.chunk_exists(&hash, Tier::Hot));
        assert!(!store.chunk_exists(&hash, Tier::Cold));

        // Verify data integrity
        let read_data = store.read_chunk(&hash, Tier::Hot).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_migrate_chunk_same_tier_is_noop() {
        let (store, _temp) = create_test_store();
        let data = b"no-op migration";
        let hash = sha256(data);

        // Write to hot tier
        store.write_chunk(&hash, data, Tier::Hot).unwrap();

        // "Migrate" to same tier should be no-op
        store.migrate_chunk(&hash, Tier::Hot, Tier::Hot).unwrap();

        // Verify: still in hot tier
        assert!(store.chunk_exists(&hash, Tier::Hot));
        let read_data = store.read_chunk(&hash, Tier::Hot).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_migrate_nonexistent_chunk_returns_error() {
        let (store, _temp) = create_test_store();
        let hash = sha256(b"nonexistent");

        let result = store.migrate_chunk(&hash, Tier::Hot, Tier::Cold);
        assert!(matches!(result, Err(StoreError::ChunkNotFound(_))));
    }

    #[test]
    fn test_migrate_preserves_data_integrity() {
        let (store, _temp) = create_test_store();
        // Use larger data to test integrity
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let hash = sha256(&data);

        // Write to hot tier
        store.write_chunk(&hash, &data, Tier::Hot).unwrap();

        // Migrate through all tiers
        store.migrate_chunk(&hash, Tier::Hot, Tier::Warm).unwrap();
        let read1 = store.read_chunk(&hash, Tier::Warm).unwrap();
        assert_eq!(read1, data);

        store.migrate_chunk(&hash, Tier::Warm, Tier::Cold).unwrap();
        let read2 = store.read_chunk(&hash, Tier::Cold).unwrap();
        assert_eq!(read2, data);

        store.migrate_chunk(&hash, Tier::Cold, Tier::Hot).unwrap();
        let read3 = store.read_chunk(&hash, Tier::Hot).unwrap();
        assert_eq!(read3, data);

        // Verify hash still matches
        assert_eq!(sha256(&read3), hash);
    }

    #[test]
    fn test_migrate_empty_chunk() {
        let (store, _temp) = create_test_store();
        let data = b"";
        let hash = sha256(data);

        store.write_chunk(&hash, data, Tier::Hot).unwrap();
        store.migrate_chunk(&hash, Tier::Hot, Tier::Cold).unwrap();

        assert!(!store.chunk_exists(&hash, Tier::Hot));
        assert!(store.chunk_exists(&hash, Tier::Cold));

        let read_data = store.read_chunk(&hash, Tier::Cold).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_migrate_chunk_verifies_integrity() {
        let (store, _temp) = create_test_store();
        let data = b"test verification during migration";
        let hash = sha256(data);

        // Write to hot tier
        store.write_chunk(&hash, data, Tier::Hot).unwrap();

        // Migration should succeed with valid data
        // (internally uses verification)
        store.migrate_chunk(&hash, Tier::Hot, Tier::Cold).unwrap();

        // Chunk should be in cold, not hot
        assert!(!store.chunk_exists(&hash, Tier::Hot));
        assert!(store.chunk_exists(&hash, Tier::Cold));
    }

    // Metadata tests

    const TEST_SEGMENT_HEX: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn test_metadata_write_read_roundtrip() {
        let (store, _temp) = create_test_store();
        let key_hash = sha256(b"chunk:abc123");
        let data = b"serialised metadata record";

        store
            .write_metadata(TEST_SEGMENT_HEX, &key_hash, data)
            .unwrap();
        let read_data = store.read_metadata(TEST_SEGMENT_HEX, &key_hash).unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_metadata_read_nonexistent() {
        let (store, _temp) = create_test_store();
        let key_hash = sha256(b"nonexistent");

        let result = store.read_metadata(TEST_SEGMENT_HEX, &key_hash);
        assert!(matches!(result, Err(StoreError::MetadataNotFound(_))));
    }

    #[test]
    fn test_metadata_delete() {
        let (store, _temp) = create_test_store();
        let key_hash = sha256(b"to_delete");
        let data = b"will be deleted";

        store
            .write_metadata(TEST_SEGMENT_HEX, &key_hash, data)
            .unwrap();
        store.delete_metadata(TEST_SEGMENT_HEX, &key_hash).unwrap();

        let result = store.read_metadata(TEST_SEGMENT_HEX, &key_hash);
        assert!(matches!(result, Err(StoreError::MetadataNotFound(_))));
    }

    #[test]
    fn test_metadata_delete_nonexistent() {
        let (store, _temp) = create_test_store();
        let key_hash = sha256(b"nonexistent");

        let result = store.delete_metadata(TEST_SEGMENT_HEX, &key_hash);
        assert!(matches!(result, Err(StoreError::MetadataNotFound(_))));
    }

    #[test]
    fn test_metadata_list_segment() {
        let (store, _temp) = create_test_store();
        let key1 = sha256(b"key_one");
        let key2 = sha256(b"key_two");
        let key3 = sha256(b"key_three");

        store
            .write_metadata(TEST_SEGMENT_HEX, &key1, b"val1")
            .unwrap();
        store
            .write_metadata(TEST_SEGMENT_HEX, &key2, b"val2")
            .unwrap();
        store
            .write_metadata(TEST_SEGMENT_HEX, &key3, b"val3")
            .unwrap();

        let mut hashes = store.list_metadata_segment(TEST_SEGMENT_HEX).unwrap();
        hashes.sort_by_key(|h| h.to_hex());

        assert_eq!(hashes.len(), 3);
        let mut expected = vec![key1, key2, key3];
        expected.sort_by_key(|h| h.to_hex());
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_metadata_list_empty_segment() {
        let (store, _temp) = create_test_store();

        let hashes = store.list_metadata_segment(TEST_SEGMENT_HEX).unwrap();
        assert!(hashes.is_empty());
    }

    #[test]
    fn test_metadata_list_ignores_temp_files() {
        let (store, temp_dir) = create_test_store();
        let key_hash = sha256(b"real_key");

        store
            .write_metadata(TEST_SEGMENT_HEX, &key_hash, b"data")
            .unwrap();

        // Create a temp file in the segment directory
        let meta_path = store.metadata_path(TEST_SEGMENT_HEX, &key_hash);
        let temp_file = meta_path.with_file_name(format!(
            "{}.tmp.0000000000000001",
            meta_path.file_name().unwrap().to_str().unwrap()
        ));
        fs::write(&temp_file, b"partial write").unwrap();

        let hashes = store.list_metadata_segment(TEST_SEGMENT_HEX).unwrap();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0], key_hash);

        // Cleanup
        drop(store);
        drop(temp_dir);
    }

    #[test]
    fn test_metadata_overwrite() {
        let (store, _temp) = create_test_store();
        let key_hash = sha256(b"overwrite_key");

        store
            .write_metadata(TEST_SEGMENT_HEX, &key_hash, b"version_1")
            .unwrap();
        store
            .write_metadata(TEST_SEGMENT_HEX, &key_hash, b"version_2")
            .unwrap();

        let data = store.read_metadata(TEST_SEGMENT_HEX, &key_hash).unwrap();
        assert_eq!(data, b"version_2");
    }

    #[test]
    fn test_metadata_binary_data_with_nulls() {
        let (store, _temp) = create_test_store();
        let key_hash = sha256(b"binary_key");
        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();

        store
            .write_metadata(TEST_SEGMENT_HEX, &key_hash, &data)
            .unwrap();
        let read_data = store.read_metadata(TEST_SEGMENT_HEX, &key_hash).unwrap();

        assert_eq!(read_data, data);
    }

    // Encryption store tests

    fn test_encryption_params() -> EncryptionParams {
        EncryptionParams {
            key: [0x42u8; 32],
            nonce: [0x01u8; 12],
        }
    }

    #[test]
    fn test_write_encrypted_read_decrypted_round_trip() {
        let (store, _temp) = create_test_store();
        let data = b"hello encrypted world";
        let hash = sha256(data);
        let enc = test_encryption_params();

        let write_opts = WriteOptions {
            compression: None,
            encryption: Some(enc.clone()),
        };
        let chunk_info = store
            .write_chunk_with_options(&hash, data, Tier::Hot, &write_opts)
            .unwrap();

        assert_eq!(chunk_info.original_size, data.len());
        // Encrypted data is larger (plaintext + 16 byte tag)
        assert_eq!(chunk_info.stored_size, data.len() + 16);
        assert_eq!(
            chunk_info.encryption_algorithm.as_deref(),
            Some("aes-256-gcm")
        );
        assert_eq!(chunk_info.encryption_nonce.as_deref(), Some(&enc.nonce[..]));

        let read_opts = ReadOptions {
            verify: false,
            decompress: false,
            encryption: Some(enc),
        };
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &read_opts)
            .unwrap();

        assert_eq!(read_data, data);
    }

    #[test]
    fn test_write_compressed_encrypted_read_round_trip() {
        let (store, _temp) = create_test_store();
        let data = b"compressed and encrypted data that is repeated ";
        let repeated: Vec<u8> = data.iter().cycle().take(10000).copied().collect();
        let hash = sha256(&repeated);
        let enc = test_encryption_params();

        let write_opts = WriteOptions {
            compression: Some(Compression::zstd(3)),
            encryption: Some(enc.clone()),
        };
        let chunk_info = store
            .write_chunk_with_options(&hash, &repeated, Tier::Hot, &write_opts)
            .unwrap();

        assert_eq!(chunk_info.original_size, repeated.len());
        // Compressed+encrypted should be smaller than original (compression helps)
        assert!(chunk_info.stored_size < repeated.len());

        let read_opts = ReadOptions {
            verify: true,
            decompress: true,
            encryption: Some(enc),
        };
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &read_opts)
            .unwrap();

        assert_eq!(read_data, repeated);
    }

    #[test]
    fn test_encrypted_read_with_wrong_key_fails() {
        let (store, _temp) = create_test_store();
        let data = b"secret data";
        let hash = sha256(data);
        let enc = test_encryption_params();

        let write_opts = WriteOptions {
            compression: None,
            encryption: Some(enc),
        };
        store
            .write_chunk_with_options(&hash, data, Tier::Hot, &write_opts)
            .unwrap();

        let wrong_enc = EncryptionParams {
            key: [0xFFu8; 32],
            nonce: [0x01u8; 12],
        };
        let read_opts = ReadOptions {
            verify: false,
            decompress: false,
            encryption: Some(wrong_enc),
        };
        let result = store.read_chunk_with_options(&hash, Tier::Hot, &read_opts);
        assert!(matches!(result, Err(StoreError::EncryptionError(_))));
    }

    #[test]
    fn test_write_without_encryption_chunk_info_has_no_encryption() {
        let (store, _temp) = create_test_store();
        let data = b"no encryption";
        let hash = sha256(data);

        let write_opts = WriteOptions::default();
        let chunk_info = store
            .write_chunk_with_options(&hash, data, Tier::Hot, &write_opts)
            .unwrap();

        assert!(chunk_info.encryption_algorithm.is_none());
        assert!(chunk_info.encryption_nonce.is_none());
    }

    #[test]
    fn test_reencrypt_chunk_round_trip() {
        let (store, _temp) = create_test_store();
        let data = b"data to reencrypt";
        let hash = sha256(data);

        let old_enc = EncryptionParams {
            key: [0xAAu8; 32],
            nonce: [0x01u8; 12],
        };

        // Write with old encryption
        let write_opts = WriteOptions {
            compression: None,
            encryption: Some(old_enc.clone()),
        };
        store
            .write_chunk_with_options(&hash, data, Tier::Hot, &write_opts)
            .unwrap();

        // Re-encrypt with new key
        let new_enc = EncryptionParams {
            key: [0xBBu8; 32],
            nonce: [0x02u8; 12],
        };
        let stored_size = store
            .reencrypt_chunk(&hash, Tier::Hot, &old_enc, &new_enc)
            .unwrap();

        assert_eq!(stored_size, data.len() + 16);

        // Old key should fail
        let old_read = ReadOptions {
            verify: false,
            decompress: false,
            encryption: Some(old_enc),
        };
        assert!(store
            .read_chunk_with_options(&hash, Tier::Hot, &old_read)
            .is_err());

        // New key should work
        let new_read = ReadOptions {
            verify: false,
            decompress: false,
            encryption: Some(new_enc),
        };
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &new_read)
            .unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_reencrypt_chunk_with_compression_preserves_data() {
        let (store, _temp) = create_test_store();
        let data: Vec<u8> = b"compressed reencrypt "
            .iter()
            .cycle()
            .take(5000)
            .copied()
            .collect();
        let hash = sha256(&data);

        let old_enc = EncryptionParams {
            key: [0xCCu8; 32],
            nonce: [0x03u8; 12],
        };

        // Write compressed + encrypted
        let write_opts = WriteOptions {
            compression: Some(Compression::zstd(3)),
            encryption: Some(old_enc.clone()),
        };
        store
            .write_chunk_with_options(&hash, &data, Tier::Hot, &write_opts)
            .unwrap();

        // Re-encrypt
        let new_enc = EncryptionParams {
            key: [0xDDu8; 32],
            nonce: [0x04u8; 12],
        };
        store
            .reencrypt_chunk(&hash, Tier::Hot, &old_enc, &new_enc)
            .unwrap();

        // Read with new key + decompress and verify
        let read_opts = ReadOptions {
            verify: true,
            decompress: true,
            encryption: Some(new_enc),
        };
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &read_opts)
            .unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_reencrypt_nonexistent_chunk_fails() {
        let (store, _temp) = create_test_store();
        let hash = sha256(b"nonexistent");
        let old_enc = test_encryption_params();
        let new_enc = EncryptionParams {
            key: [0xFFu8; 32],
            nonce: [0xFFu8; 12],
        };

        let result = store.reencrypt_chunk(&hash, Tier::Hot, &old_enc, &new_enc);
        assert!(matches!(result, Err(StoreError::ChunkNotFound(_))));
    }

    #[test]
    fn test_encrypted_empty_data() {
        let (store, _temp) = create_test_store();
        let data = b"";
        let hash = sha256(data);
        let enc = test_encryption_params();

        let write_opts = WriteOptions {
            compression: None,
            encryption: Some(enc.clone()),
        };
        let chunk_info = store
            .write_chunk_with_options(&hash, data, Tier::Hot, &write_opts)
            .unwrap();

        // Empty data + 16 byte auth tag
        assert_eq!(chunk_info.stored_size, 16);

        let read_opts = ReadOptions {
            verify: false,
            decompress: false,
            encryption: Some(enc),
        };
        let read_data = store
            .read_chunk_with_options(&hash, Tier::Hot, &read_opts)
            .unwrap();

        assert_eq!(read_data, data);
    }
}
