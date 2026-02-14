//! Error types for blob store operations.

use crate::encryption::EncryptionError;
use std::io;
use std::path::PathBuf;
use thiserror::Error;

/// Errors that can occur during blob store operations.
#[derive(Error, Debug)]
pub enum StoreError {
    /// The requested chunk does not exist in the store.
    #[error("chunk not found: {0}")]
    ChunkNotFound(String),

    /// The requested metadata key does not exist in the store.
    #[error("metadata not found: {0}")]
    MetadataNotFound(String),

    /// An I/O error occurred during store operations.
    #[error("I/O error at {path}: {source}")]
    IoError {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// The chunk data is corrupt (hash mismatch during verification).
    #[error("corrupt chunk: expected {expected}, got {actual}")]
    CorruptChunk {
        /// The expected hash (from the chunk identifier).
        expected: String,
        /// The actual hash computed from the data.
        actual: String,
    },

    /// The base directory does not exist and could not be created.
    #[error("invalid base directory: {0}")]
    InvalidBaseDir(PathBuf),

    /// An encryption or decryption error occurred.
    #[error("encryption error: {0}")]
    EncryptionError(String),
}

impl StoreError {
    /// Creates a new `IoError` variant with the given path and error.
    pub fn io_error(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::IoError {
            path: path.into(),
            source,
        }
    }
}

impl From<EncryptionError> for StoreError {
    fn from(e: EncryptionError) -> Self {
        Self::EncryptionError(e.to_string())
    }
}
