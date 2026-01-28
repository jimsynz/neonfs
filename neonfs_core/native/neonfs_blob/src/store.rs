//! Blob store implementation for content-addressed chunk storage.
//!
//! This module provides the core blob storage operations: writing chunks to disk
//! and reading them back. Writes are atomic (write to temp file, then rename)
//! to prevent partial chunks.

use crate::error::StoreError;
use crate::hash::{sha256, Hash};
use crate::path::{chunk_path, ensure_parent_dirs, Tier};
use rand::Rng;
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
}

impl ReadOptions {
    /// Creates options with verification enabled.
    pub fn with_verify() -> Self {
        Self { verify: true }
    }
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

    /// Writes a chunk to the store atomically.
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
        let final_path = self.chunk_path(hash, tier);
        let temp_path = self.temp_path(&final_path);

        // Ensure parent directories exist
        ensure_parent_dirs(&final_path).map_err(|e| StoreError::io_error(&final_path, e))?;

        // Write to temporary file
        let mut file = File::create(&temp_path).map_err(|e| StoreError::io_error(&temp_path, e))?;
        file.write_all(data)
            .map_err(|e| StoreError::io_error(&temp_path, e))?;
        file.sync_all()
            .map_err(|e| StoreError::io_error(&temp_path, e))?;

        // Atomic rename to final path
        fs::rename(&temp_path, &final_path).map_err(|e| StoreError::io_error(&final_path, e))?;

        Ok(())
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
    /// * `options` - Options controlling read behavior (e.g., verification).
    ///
    /// # Errors
    /// Returns `ChunkNotFound` if the chunk does not exist.
    /// Returns `IoError` if the read fails.
    /// Returns `CorruptChunk` if verification is enabled and the data doesn't match.
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

        let data = fs::read(&path).map_err(|e| StoreError::io_error(&path, e))?;

        // Optionally verify the data matches the expected hash
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
    pub fn delete_chunk(&self, hash: &Hash, tier: Tier) -> Result<(), StoreError> {
        let path = self.chunk_path(hash, tier);

        if !path.exists() {
            return Err(StoreError::ChunkNotFound(hash.to_hex()));
        }

        fs::remove_file(&path).map_err(|e| StoreError::io_error(&path, e))
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
    }

    #[test]
    fn test_read_options_with_verify() {
        let options = ReadOptions::with_verify();
        assert!(options.verify);
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
}
