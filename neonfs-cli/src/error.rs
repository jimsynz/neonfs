//! CLI error types

use thiserror::Error;

/// Errors that can occur during CLI operations
#[derive(Debug, Error)]
pub enum CliError {
    // Connection errors
    /// EPMD (Erlang Port Mapper Daemon) is not running
    #[error("{0}")]
    EpmdNotRunning(String),

    /// Node not found in EPMD
    #[error("{0}")]
    NodeNotFound(String),

    /// Failed to connect to daemon
    #[error("{0}")]
    ConnectionFailed(String),

    /// Connection timeout
    #[error("Connection timed out. The daemon may be overloaded.")]
    Timeout,

    // Cookie authentication errors
    /// Cookie file not found
    #[error("{0}")]
    CookieNotFound(String),

    /// Permission denied reading cookie file
    #[error("{0}")]
    CookiePermissionDenied(String),

    /// Failed to read cookie file
    #[error("{0}")]
    CookieReadError(String),

    // RPC errors
    /// EPMD lookup failed
    #[error("EPMD lookup failed: {0}")]
    EpmdLookupFailed(String),

    /// RPC call failed
    #[error("RPC call failed: {0}")]
    RpcError(String),

    /// Operation failed on the daemon side
    #[error("Daemon operation failed: {0}")]
    #[allow(dead_code)]
    DaemonOperation(String),

    // General errors
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
