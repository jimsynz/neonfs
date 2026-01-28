//! CLI error types

use thiserror::Error;

/// Errors that can occur during CLI operations
#[derive(Debug, Error)]
pub enum CliError {
    /// Failed to communicate with daemon
    #[error("Failed to connect to daemon: {0}")]
    #[allow(dead_code)]
    DaemonConnection(String),

    /// Operation failed on the daemon side
    #[error("Daemon operation failed: {0}")]
    #[allow(dead_code)]
    DaemonOperation(String),

    /// Invalid command-line arguments
    #[error("Invalid argument: {0}")]
    #[allow(dead_code)]
    InvalidArgument(String),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Generic error
    #[error("{0}")]
    #[allow(dead_code)]
    Other(String),
}

/// CLI result type
pub type Result<T> = std::result::Result<T, CliError>;
