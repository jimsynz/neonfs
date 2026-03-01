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

impl From<String> for FuseError {
    fn from(s: String) -> Self {
        FuseError::ChannelSend(s)
    }
}

impl FuseError {
    /// Convert to fuser::Errno for FUSE reply errors
    #[allow(dead_code)] // Used when fuse feature is enabled
    pub fn to_errno(&self) -> fuser::Errno {
        match self {
            FuseError::Io(e) => {
                use std::io::ErrorKind;
                match e.kind() {
                    ErrorKind::NotFound => fuser::Errno::ENOENT,
                    ErrorKind::PermissionDenied => fuser::Errno::EACCES,
                    ErrorKind::AlreadyExists => fuser::Errno::EEXIST,
                    ErrorKind::InvalidInput => fuser::Errno::EINVAL,
                    ErrorKind::TimedOut => fuser::Errno::ETIMEDOUT,
                    _ => fuser::Errno::EIO,
                }
            }
            FuseError::ChannelSend(_) | FuseError::InvalidReply => fuser::Errno::EIO,
            FuseError::Timeout => fuser::Errno::ETIMEDOUT,
            FuseError::Shutdown => fuser::Errno::ESHUTDOWN,
            FuseError::Errno(code) => fuser::Errno::from_i32(*code),
        }
    }

    /// Create from an errno code
    #[allow(dead_code)] // Used when fuse feature is enabled
    pub fn from_errno(errno: i32) -> Self {
        FuseError::Errno(errno)
    }
}

/// Convert Elixir error atom to fuser::Errno
///
/// Common mappings:
/// - :enoent -> ENOENT (2)
/// - :eacces -> EACCES (13)
/// - :eexist -> EEXIST (17)
/// - :einval -> EINVAL (22)
/// - :eio -> EIO (5)
#[allow(dead_code)] // Used when fuse feature is enabled
pub fn atom_to_errno(atom: &str) -> fuser::Errno {
    match atom {
        "enoent" => fuser::Errno::ENOENT,
        "eacces" => fuser::Errno::EACCES,
        "eexist" => fuser::Errno::EEXIST,
        "einval" => fuser::Errno::EINVAL,
        "eio" => fuser::Errno::EIO,
        "eisdir" => fuser::Errno::EISDIR,
        "enotdir" => fuser::Errno::ENOTDIR,
        "enotempty" => fuser::Errno::ENOTEMPTY,
        "enametoolong" => fuser::Errno::ENAMETOOLONG,
        "enospc" => fuser::Errno::ENOSPC,
        "erofs" => fuser::Errno::EROFS,
        "etimedout" => fuser::Errno::ETIMEDOUT,
        _ => fuser::Errno::EIO, // Default to generic I/O error
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
        assert_eq!(atom_to_errno("enoent").code(), fuser::Errno::ENOENT.code());
        assert_eq!(atom_to_errno("eacces").code(), fuser::Errno::EACCES.code());
        assert_eq!(atom_to_errno("eexist").code(), fuser::Errno::EEXIST.code());
        assert_eq!(atom_to_errno("einval").code(), fuser::Errno::EINVAL.code());
        assert_eq!(atom_to_errno("eio").code(), fuser::Errno::EIO.code());

        // Unknown atoms default to EIO
        assert_eq!(atom_to_errno("unknown").code(), fuser::Errno::EIO.code());
    }

    #[test]
    fn test_fuse_error_to_errno() {
        let err = FuseError::Errno(libc::ENOENT);
        assert_eq!(err.to_errno().code(), fuser::Errno::ENOENT.code());

        let err = FuseError::Timeout;
        assert_eq!(err.to_errno().code(), fuser::Errno::ETIMEDOUT.code());

        let err = FuseError::Shutdown;
        assert_eq!(err.to_errno().code(), fuser::Errno::ESHUTDOWN.code());
    }

    #[test]
    fn test_io_error_mapping() {
        let io_err = std::io::Error::from(std::io::ErrorKind::NotFound);
        let fuse_err = FuseError::from(io_err);
        assert_eq!(fuse_err.to_errno().code(), fuser::Errno::ENOENT.code());

        let io_err = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        let fuse_err = FuseError::from(io_err);
        assert_eq!(fuse_err.to_errno().code(), fuser::Errno::EACCES.code());
    }
}
