/// FUSE filesystem implementation using fuser library
///
/// This module implements the fuser::Filesystem trait, which is the core interface
/// for handling FUSE operations. Each FUSE kernel request is forwarded to Elixir
/// for handling, and the reply is sent back through channels.
use crate::error::FuseError;
use crate::operation::{FileKind, FuseOperation, FuseReply};
use crate::server::FuseServer;
use fuser::{
    BsdFileFlags, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, LockOwner, OpenFlags, RenameFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request, WriteFlags,
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
                let rt = tokio::runtime::Runtime::new().map_err(FuseError::Io)?;
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
            ino: INodeNo(ino),
            size,
            blocks: size.div_ceil(512), // Round up to 512-byte blocks
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
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy().to_string();

        let operation = FuseOperation::Lookup {
            parent: parent.0,
            name: name_str,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::LookupOk { ino, size, kind }) => {
                let attr = Self::make_file_attr(ino, size, kind);
                reply.entry(&TTL, &attr, Generation(0));
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Lookup error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for lookup operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Get file attributes
    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let operation = FuseOperation::GetAttr { ino: ino.0 };

        match self.call_elixir(operation) {
            Ok(FuseReply::AttrOk { ino, size, kind }) => {
                let attr = Self::make_file_attr(ino, size, kind);
                reply.attr(&TTL, &attr);
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Getattr error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for getattr operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Read data from a file
    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let operation = FuseOperation::Read {
            ino: ino.0,
            offset,
            size,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::ReadOk { data }) => {
                reply.data(&data);
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Read error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for read operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Read directory entries
    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let operation = FuseOperation::ReadDir {
            ino: ino.0,
            offset: offset as i64,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::ReadDirOk { entries }) => {
                for (idx, entry) in entries.iter().enumerate().skip(offset as usize) {
                    let kind = Self::file_kind_to_type(entry.kind);
                    let full = reply.add(INodeNo(entry.ino), (idx + 1) as u64, kind, &entry.name);
                    if full {
                        break;
                    }
                }
                reply.ok();
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Readdir error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for readdir operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Write data to a file
    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        let operation = FuseOperation::Write {
            ino: ino.0,
            offset,
            data: data.to_vec(),
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::WriteOk { size }) => {
                reply.written(size);
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Write error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for write operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Open a file
    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: fuser::ReplyOpen) {
        let operation = FuseOperation::Open {
            ino: ino.0,
            flags: flags.0,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::OpenOk { fh }) => {
                reply.opened(FileHandle(fh), FopenFlags::empty());
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Open error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for open operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Release (close) a file
    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let operation = FuseOperation::Release {
            ino: ino.0,
            fh: fh.0,
            flags: flags.0,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::Ok) => {
                reply.ok();
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Release error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for release operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Create a file
    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let name_str = name.to_string_lossy().to_string();

        let operation = FuseOperation::Create {
            parent: parent.0,
            name: name_str,
            mode,
            flags,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::EntryOk {
                ino,
                size,
                kind,
                fh,
            }) => {
                let attr = Self::make_file_attr(ino, size, kind);
                reply.created(
                    &TTL,
                    &attr,
                    Generation(0),
                    FileHandle(fh),
                    FopenFlags::empty(),
                );
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Create error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for create operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Create a directory
    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_str = name.to_string_lossy().to_string();

        let operation = FuseOperation::MkDir {
            parent: parent.0,
            name: name_str,
            mode,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::LookupOk { ino, size, kind }) => {
                let attr = Self::make_file_attr(ino, size, kind);
                reply.entry(&TTL, &attr, Generation(0));
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Mkdir error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for mkdir operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Remove a file
    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: fuser::ReplyEmpty) {
        let name_str = name.to_string_lossy().to_string();

        let operation = FuseOperation::Unlink {
            parent: parent.0,
            name: name_str,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::Ok) => {
                reply.ok();
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Unlink error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for unlink operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Remove a directory
    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: fuser::ReplyEmpty) {
        let name_str = name.to_string_lossy().to_string();

        let operation = FuseOperation::RmDir {
            parent: parent.0,
            name: name_str,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::Ok) => {
                reply.ok();
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Rmdir error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for rmdir operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Rename a file or directory
    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: fuser::ReplyEmpty,
    ) {
        let old_name = name.to_string_lossy().to_string();
        let new_name = newname.to_string_lossy().to_string();

        let operation = FuseOperation::Rename {
            old_parent: parent.0,
            old_name,
            new_parent: newparent.0,
            new_name,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::Ok) => {
                reply.ok();
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Rename error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for rename operation");
                reply.error(Errno::EIO);
            }
        }
    }

    /// Set file attributes
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        // Convert TimeOrNow to Option<(i64, u32)>
        let atime_tuple = atime.and_then(|t| match t {
            fuser::TimeOrNow::SpecificTime(st) => st
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|d| (d.as_secs() as i64, d.subsec_nanos())),
            fuser::TimeOrNow::Now => {
                let now = SystemTime::now();
                now.duration_since(SystemTime::UNIX_EPOCH)
                    .ok()
                    .map(|d| (d.as_secs() as i64, d.subsec_nanos()))
            }
        });

        let mtime_tuple = mtime.and_then(|t| match t {
            fuser::TimeOrNow::SpecificTime(st) => st
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|d| (d.as_secs() as i64, d.subsec_nanos())),
            fuser::TimeOrNow::Now => {
                let now = SystemTime::now();
                now.duration_since(SystemTime::UNIX_EPOCH)
                    .ok()
                    .map(|d| (d.as_secs() as i64, d.subsec_nanos()))
            }
        });

        let operation = FuseOperation::SetAttr {
            ino: ino.0,
            mode,
            uid,
            gid,
            size,
            atime: atime_tuple,
            mtime: mtime_tuple,
        };

        match self.call_elixir(operation) {
            Ok(FuseReply::AttrOk { ino, size, kind }) => {
                let attr = Self::make_file_attr(ino, size, kind);
                reply.attr(&TTL, &attr);
            }
            Ok(FuseReply::Error { errno }) => {
                reply.error(Errno::from_i32(errno));
            }
            Err(e) => {
                log::error!("Setattr error: {}", e);
                reply.error(e.to_errno());
            }
            _ => {
                log::error!("Invalid reply for setattr operation");
                reply.error(Errno::EIO);
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
        assert_eq!(attr.ino, INodeNo(42));
        assert_eq!(attr.size, 1024);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.blocks, 2); // 1024 bytes = 2 blocks of 512

        let attr = NeonFilesystem::make_file_attr(1, 0, FileKind::Directory);
        assert_eq!(attr.ino, INodeNo(1));
        assert_eq!(attr.size, 0);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }
}
