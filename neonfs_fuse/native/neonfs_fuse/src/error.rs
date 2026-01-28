/// FUSE error handling and errno mapping
///
/// Provides utilities for converting Elixir error atoms to POSIX errno codes
/// and back. This enables consistent error handling across the Rust-Elixir boundary.
use rustler::{Encoder, Env, Term};
use thiserror::Error;

/// Errors that can occur in FUSE operations
#[derive(Debug, Error)]
#[allow(dead_code)] // Infrastructure for FUSE operations (task 0012+)
pub enum FuseError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel send error: {0}")]
    ChannelSend(String),

    #[error("Operation timeout")]
    Timeout,

    #[error("Server shutdown")]
    Shutdown,

    #[error("Invalid reply from Elixir")]
    InvalidReply,

    #[error("POSIX error: {0}")]
    Errno(i32),
}

impl FuseError {
    /// Convert to POSIX errno code
    #[allow(dead_code)] // Used when fuse feature is enabled
    pub fn to_errno(&self) -> i32 {
        match self {
            FuseError::Io(e) => {
                use std::io::ErrorKind;
                match e.kind() {
                    ErrorKind::NotFound => libc::ENOENT,
                    ErrorKind::PermissionDenied => libc::EACCES,
                    ErrorKind::AlreadyExists => libc::EEXIST,
                    ErrorKind::InvalidInput => libc::EINVAL,
                    ErrorKind::TimedOut => libc::ETIMEDOUT,
                    _ => libc::EIO,
                }
            }
            FuseError::ChannelSend(_) | FuseError::InvalidReply => libc::EIO,
            FuseError::Timeout => libc::ETIMEDOUT,
            FuseError::Shutdown => libc::ESHUTDOWN,
            FuseError::Errno(code) => *code,
        }
    }

    /// Create from an errno code
    #[allow(dead_code)] // Used when fuse feature is enabled
    pub fn from_errno(errno: i32) -> Self {
        FuseError::Errno(errno)
    }
}

/// Convert Elixir error atom to errno code
///
/// Common mappings:
/// - :enoent -> ENOENT (2)
/// - :eacces -> EACCES (13)
/// - :eexist -> EEXIST (17)
/// - :einval -> EINVAL (22)
/// - :eio -> EIO (5)
#[allow(dead_code)] // Used when fuse feature is enabled
pub fn atom_to_errno(atom: &str) -> i32 {
    match atom {
        "enoent" => libc::ENOENT,
        "eacces" => libc::EACCES,
        "eexist" => libc::EEXIST,
        "einval" => libc::EINVAL,
        "eio" => libc::EIO,
        "eisdir" => libc::EISDIR,
        "enotdir" => libc::ENOTDIR,
        "enotempty" => libc::ENOTEMPTY,
        "enametoolong" => libc::ENAMETOOLONG,
        "enospc" => libc::ENOSPC,
        "erofs" => libc::EROFS,
        "etimedout" => libc::ETIMEDOUT,
        _ => libc::EIO, // Default to generic I/O error
    }
}

/// Convert errno code to Elixir error atom
#[allow(dead_code)] // Used when fuse feature is enabled
pub fn errno_to_atom<'a>(env: Env<'a>, errno: i32) -> Term<'a> {
    let atom_str = match errno {
        libc::ENOENT => "enoent",
        libc::EACCES => "eacces",
        libc::EEXIST => "eexist",
        libc::EINVAL => "einval",
        libc::EIO => "eio",
        libc::EISDIR => "eisdir",
        libc::ENOTDIR => "enotdir",
        libc::ENOTEMPTY => "enotempty",
        libc::ENAMETOOLONG => "enametoolong",
        libc::ENOSPC => "enospc",
        libc::EROFS => "erofs",
        libc::ETIMEDOUT => "etimedout",
        _ => "eio",
    };

    rustler::types::atom::Atom::from_str(env, atom_str)
        .unwrap()
        .encode(env)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atom_to_errno() {
        assert_eq!(atom_to_errno("enoent"), libc::ENOENT);
        assert_eq!(atom_to_errno("eacces"), libc::EACCES);
        assert_eq!(atom_to_errno("eexist"), libc::EEXIST);
        assert_eq!(atom_to_errno("einval"), libc::EINVAL);
        assert_eq!(atom_to_errno("eio"), libc::EIO);

        // Unknown atoms default to EIO
        assert_eq!(atom_to_errno("unknown"), libc::EIO);
    }

    #[test]
    fn test_fuse_error_to_errno() {
        let err = FuseError::Errno(libc::ENOENT);
        assert_eq!(err.to_errno(), libc::ENOENT);

        let err = FuseError::Timeout;
        assert_eq!(err.to_errno(), libc::ETIMEDOUT);

        let err = FuseError::Shutdown;
        assert_eq!(err.to_errno(), libc::ESHUTDOWN);
    }

    #[test]
    fn test_io_error_mapping() {
        let io_err = std::io::Error::from(std::io::ErrorKind::NotFound);
        let fuse_err = FuseError::from(io_err);
        assert_eq!(fuse_err.to_errno(), libc::ENOENT);

        let io_err = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        let fuse_err = FuseError::from(io_err);
        assert_eq!(fuse_err.to_errno(), libc::EACCES);
    }
}
