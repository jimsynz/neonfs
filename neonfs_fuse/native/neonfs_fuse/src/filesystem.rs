#![cfg(feature = "fuse")]

/// FUSE filesystem implementation using fuser library
///
/// This module implements the fuser::Filesystem trait, which is the core interface
/// for handling FUSE operations. Each FUSE kernel request is forwarded to Elixir
/// for handling, and the reply is sent back through channels.
use crate::error::{atom_to_errno, FuseError};
use crate::operation::{DirEntry, FileKind, FuseOperation, FuseReply};
use crate::server::FuseServer;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
    FUSE_ROOT_ID,
};
use std::ffi::OsStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Timeout for waiting on Elixir replies
const REPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// TTL for FUSE entry and attribute caching
const TTL: Duration = Duration::from_secs(1);

/// NeonFS FUSE filesystem implementation
pub struct NeonFilesystem {
    /// Server managing communication with Elixir
    server: Arc<Mutex<FuseServer>>,
}

impl NeonFilesystem {
    /// Create a new filesystem instance
    pub fn new(server: FuseServer) -> Self {
        Self {
            server: Arc::new(Mutex::new(server)),
        }
    }

    /// Call Elixir with an operation and wait for reply
    fn call_elixir(&self, operation: FuseOperation) -> Result<FuseReply, FuseError> {
        let server = self
            .server
            .lock()
            .map_err(|_| FuseError::ChannelSend("Failed to lock server".to_string()))?;

        let reply_rx = server.submit_operation(operation)?;

        // Drop the lock so we don't block while waiting for reply
        drop(server);

        // Wait for reply with timeout
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                // We're in a tokio context
                handle.block_on(async {
                    tokio::time::timeout(REPLY_TIMEOUT, reply_rx)
                        .await
                        .map_err(|_| FuseError::Timeout)?
                        .map_err(|_| FuseError::InvalidReply)
                })
            }
            Err(_) => {
                // Create a new runtime for this operation
                let rt = tokio::runtime::Runtime::new().map_err(|e| FuseError::Io(e))?;
                rt.block_on(async {
                    tokio::time::timeout(REPLY_TIMEOUT, reply_rx)
                        .await
                        .map_err(|_| FuseError::Timeout)?
                        .map_err(|_| FuseError::InvalidReply)
                })
            }
        }
    }

    /// Convert FileKind to fuser::FileType
    fn file_kind_to_type(kind: FileKind) -> FileType {
        match kind {
            FileKind::File => FileType::RegularFile,
            FileKind::Directory => FileType::Directory,
            FileKind::Symlink => FileType::Symlink,
        }
    }

    /// Create FileAttr from reply data
    fn make_file_attr(ino: u64, size: u64, kind: FileKind) -> FileAttr {
        let now = SystemTime::now();
        FileAttr {
            ino,
            size,
            blocks: (size + 511) / 512, // Round up to 512-byte blocks
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: Self::file_kind_to_type(kind),
            perm: if kind == FileKind::Directory {
                0o755
            } else {
                0o644
            },
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }
}

impl Filesystem for NeonFilesystem {
    /// Look up a directory entry by name
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy().to_string();

        let operation = FuseOperation::Lookup {
            parent,
            name: name_str,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::LookupOk { ino, size, kind }) => {
                let attr = Self::make_file_attr(ino, size, kind);
                reply.entry(&TTL, &attr, 0);
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(errno);
            }
            Err(e) => {
                log::error!("Lookup error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for lookup operation");
                reply.error(libc::EIO);
            }
        }
    }

    /// Get file attributes
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let operation = FuseOperation::GetAttr { ino };

        match self.call_elixir(operation) {
            Ok(FuseReply::AttrOk { ino, size, kind }) => {
                let attr = Self::make_file_attr(ino, size, kind);
                reply.attr(&TTL, &attr);
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(errno);
            }
            Err(e) => {
                log::error!("Getattr error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for getattr operation");
                reply.error(libc::EIO);
            }
        }
    }

    /// Read data from a file
    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let operation = FuseOperation::Read {
            ino,
            offset: offset as u64,
            size,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::ReadOk { data }) => {
                reply.data(&data);
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(errno);
            }
            Err(e) => {
                log::error!("Read error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for read operation");
                reply.error(libc::EIO);
            }
        }
    }

    /// Read directory entries
    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let operation = FuseOperation::ReadDir { ino, offset };

        match self.call_elixir(operation) {
            Ok(FuseReply::ReadDirOk { entries }) => {
                for (idx, entry) in entries.iter().enumerate().skip(offset as usize) {
                    let kind = Self::file_kind_to_type(entry.kind);
                    let full = reply.add(entry.ino, (idx + 1) as i64, kind, &entry.name);
                    if full {
                        break;
                    }
                }
                reply.ok();
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(errno);
            }
            Err(e) => {
                log::error!("Readdir error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for readdir operation");
                reply.error(libc::EIO);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_kind_conversion() {
        assert_eq!(
            NeonFilesystem::file_kind_to_type(FileKind::File),
            FileType::RegularFile
        );
        assert_eq!(
            NeonFilesystem::file_kind_to_type(FileKind::Directory),
            FileType::Directory
        );
        assert_eq!(
            NeonFilesystem::file_kind_to_type(FileKind::Symlink),
            FileType::Symlink
        );
    }

    #[test]
    fn test_make_file_attr() {
        let attr = NeonFilesystem::make_file_attr(42, 1024, FileKind::File);
        assert_eq!(attr.ino, 42);
        assert_eq!(attr.size, 1024);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.blocks, 2); // 1024 bytes = 2 blocks of 512

        let attr = NeonFilesystem::make_file_attr(1, 0, FileKind::Directory);
        assert_eq!(attr.ino, 1);
        assert_eq!(attr.size, 0);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }
}
