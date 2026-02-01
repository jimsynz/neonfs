//! CLI error types

use thiserror::Error;

/// Errors that can occur during CLI operations
#[derive(Debug, Error)]
pub enum CliError {
    // Connection errors
    /// Failed to connect to daemon
    #[error("{0}")]
    ConnectionFailed(String),

    // Cookie authentication errors
    /// Cookie file not found
    #[error("{0}")]
    CookieNotFound(String),

    /// Failed to read cookie file
    #[error("{0}")]
    CookieReadError(String),

    // RPC errors
    /// RPC call failed
    #[error("RPC call failed: {0}")]
    RpcError(String),

    /// RPC call failed
    #[error("{0}")]
    RpcFailed(String),

    /// Term conversion error
    #[error("Term conversion error: {0}")]
    TermConversionError(String),

    /// Invalid argument
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    // General errors
    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

/// CLI result type
pub type Result<T> = std::result::Result<T, CliError>;
