//! CLI error types

use std::collections::HashMap;
use thiserror::Error;

/// Error class from NeonFS structured errors.
///
/// Maps to the five Splode error classes defined in `NeonFS.Error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorClass {
    /// Bad input or validation failure
    Invalid,
    /// Resource not found
    NotFound,
    /// Permission denied
    Forbidden,
    /// Cluster or quorum unavailable
    Unavailable,
    /// Unexpected internal failure
    Internal,
}

impl ErrorClass {
    /// Parse an error class from an Erlang atom name.
    pub fn from_atom(name: &str) -> Option<Self> {
        match name {
            "invalid" => Some(ErrorClass::Invalid),
            "not_found" => Some(ErrorClass::NotFound),
            "forbidden" => Some(ErrorClass::Forbidden),
            "unavailable" => Some(ErrorClass::Unavailable),
            "internal" => Some(ErrorClass::Internal),
            _ => None,
        }
    }

    /// Process exit code for this error class.
    pub fn exit_code(&self) -> i32 {
        match self {
            ErrorClass::Invalid | ErrorClass::NotFound => 1,
            ErrorClass::Forbidden => 2,
            ErrorClass::Unavailable => 3,
            ErrorClass::Internal => 4,
        }
    }

    /// Format a message with the appropriate prefix for this error class.
    pub fn format_message(&self, message: &str) -> String {
        match self {
            ErrorClass::Invalid | ErrorClass::NotFound => message.to_string(),
            ErrorClass::Forbidden => format!("Permission denied: {message}"),
            ErrorClass::Unavailable => format!("Cluster unavailable: {message}"),
            ErrorClass::Internal => format!("Internal error: {message}"),
        }
    }
}

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

    /// Structured NeonFS error with class, message, and details
    #[error("{}", .class.format_message(.message))]
    NeonfsError {
        class: ErrorClass,
        message: String,
        details: HashMap<String, String>,
    },

    /// Term conversion error
    #[error("Term conversion error: {0}")]
    TermConversionError(String),

    /// Node health check reported unhealthy status
    #[error("node is unhealthy")]
    HealthCheckFailed,

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

impl CliError {
    /// Process exit code for this error.
    ///
    /// Structured NeonFS errors use class-based exit codes;
    /// all other errors default to exit code 1.
    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::NeonfsError { class, .. } => class.exit_code(),
            CliError::HealthCheckFailed => 2,
            _ => 1,
        }
    }

    /// Extract the error message string for pattern matching.
    pub fn error_message(&self) -> String {
        match self {
            CliError::NeonfsError { message, .. } => message.clone(),
            CliError::RpcError(msg) | CliError::RpcFailed(msg) => msg.clone(),
            other => format!("{other}"),
        }
    }
}

/// CLI result type
pub type Result<T> = std::result::Result<T, CliError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_class_from_atom() {
        assert_eq!(ErrorClass::from_atom("invalid"), Some(ErrorClass::Invalid));
        assert_eq!(
            ErrorClass::from_atom("not_found"),
            Some(ErrorClass::NotFound)
        );
        assert_eq!(
            ErrorClass::from_atom("forbidden"),
            Some(ErrorClass::Forbidden)
        );
        assert_eq!(
            ErrorClass::from_atom("unavailable"),
            Some(ErrorClass::Unavailable)
        );
        assert_eq!(
            ErrorClass::from_atom("internal"),
            Some(ErrorClass::Internal)
        );
        assert_eq!(ErrorClass::from_atom("unknown"), None);
    }

    #[test]
    fn test_exit_codes() {
        assert_eq!(ErrorClass::Invalid.exit_code(), 1);
        assert_eq!(ErrorClass::NotFound.exit_code(), 1);
        assert_eq!(ErrorClass::Forbidden.exit_code(), 2);
        assert_eq!(ErrorClass::Unavailable.exit_code(), 3);
        assert_eq!(ErrorClass::Internal.exit_code(), 4);
    }

    #[test]
    fn test_format_message_user_errors() {
        assert_eq!(ErrorClass::Invalid.format_message("bad input"), "bad input");
        assert_eq!(
            ErrorClass::NotFound.format_message("volume not found"),
            "volume not found"
        );
    }

    #[test]
    fn test_format_message_prefixed_errors() {
        assert_eq!(
            ErrorClass::Forbidden.format_message("not allowed"),
            "Permission denied: not allowed"
        );
        assert_eq!(
            ErrorClass::Unavailable.format_message("no quorum"),
            "Cluster unavailable: no quorum"
        );
        assert_eq!(
            ErrorClass::Internal.format_message("unexpected"),
            "Internal error: unexpected"
        );
    }

    #[test]
    fn test_cli_error_exit_codes() {
        let neonfs_err = CliError::NeonfsError {
            class: ErrorClass::Forbidden,
            message: "denied".to_string(),
            details: HashMap::new(),
        };
        assert_eq!(neonfs_err.exit_code(), 2);

        let rpc_err = CliError::RpcError("failed".to_string());
        assert_eq!(rpc_err.exit_code(), 1);
    }

    #[test]
    fn test_health_check_failed_exit_code() {
        let err = CliError::HealthCheckFailed;
        assert_eq!(err.exit_code(), 2);
        assert_eq!(format!("{err}"), "node is unhealthy");
    }

    #[test]
    fn test_neonfs_error_display() {
        let err = CliError::NeonfsError {
            class: ErrorClass::Internal,
            message: "something broke".to_string(),
            details: HashMap::new(),
        };
        assert_eq!(format!("{err}"), "Internal error: something broke");

        let err = CliError::NeonfsError {
            class: ErrorClass::NotFound,
            message: "Volume 'mydata' not found".to_string(),
            details: HashMap::new(),
        };
        assert_eq!(format!("{err}"), "Volume 'mydata' not found");
    }
}
