/// FUSE operation types that flow from FUSE thread to Elixir
///
/// Each operation variant contains the necessary parameters for that FUSE operation.
/// Operations are sent to Elixir for handling, and replies flow back via channels.
use rustler::{Encoder, Env, Term};
use std::collections::HashMap;

/// Represents a FUSE filesystem operation to be handled by Elixir
#[derive(Debug, Clone)]
#[allow(dead_code)] // Infrastructure for future FUSE operations (task 0012+)
pub enum FuseOperation {
    /// Read data from a file
    Read { ino: u64, offset: u64, size: u32 },
    /// Write data to a file
    Write {
        ino: u64,
        offset: u64,
        data: Vec<u8>,
    },
    /// Look up a directory entry
    Lookup { parent: u64, name: String },
    /// Get file attributes
    GetAttr { ino: u64 },
    /// Read directory entries
    ReadDir { ino: u64, offset: i64 },
    /// Create a file
    Create {
        parent: u64,
        name: String,
        mode: u32,
    },
    /// Remove a file
    Unlink { parent: u64, name: String },
    /// Create a directory
    MkDir {
        parent: u64,
        name: String,
        mode: u32,
    },
    /// Remove a directory
    RmDir { parent: u64, name: String },
    /// Open a file
    Open { ino: u64, flags: i32 },
    /// Release (close) a file
    Release { ino: u64, fh: u64, flags: i32 },
    /// Rename a file or directory
    Rename {
        old_parent: u64,
        old_name: String,
        new_parent: u64,
        new_name: String,
    },
    /// Set file attributes
    SetAttr {
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<(i64, u32)>,
        mtime: Option<(i64, u32)>,
    },
}

impl FuseOperation {
    /// Encode operation as Elixir term for sending to Elixir process
    #[allow(dead_code)] // Will be used when sending operations to Elixir (task 0012+)
    pub fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            FuseOperation::Read { ino, offset, size } => {
                let mut map = HashMap::new();
                map.insert("ino", ino.encode(env));
                map.insert("offset", offset.encode(env));
                map.insert("size", size.encode(env));
                ("read", map).encode(env)
            }
            FuseOperation::Write { ino, offset, data } => {
                let mut map = HashMap::new();
                map.insert("ino", ino.encode(env));
                map.insert("offset", offset.encode(env));
                map.insert("data", data.encode(env));
                ("write", map).encode(env)
            }
            FuseOperation::Lookup { parent, name } => {
                let mut map = HashMap::new();
                map.insert("parent", parent.encode(env));
                map.insert("name", name.encode(env));
                ("lookup", map).encode(env)
            }
            FuseOperation::GetAttr { ino } => {
                let mut map = HashMap::new();
                map.insert("ino", ino.encode(env));
                ("getattr", map).encode(env)
            }
            FuseOperation::ReadDir { ino, offset } => {
                let mut map = HashMap::new();
                map.insert("ino", ino.encode(env));
                map.insert("offset", offset.encode(env));
                ("readdir", map).encode(env)
            }
            FuseOperation::Create { parent, name, mode } => {
                let mut map = HashMap::new();
                map.insert("parent", parent.encode(env));
                map.insert("name", name.encode(env));
                map.insert("mode", mode.encode(env));
                ("create", map).encode(env)
            }
            FuseOperation::Unlink { parent, name } => {
                let mut map = HashMap::new();
                map.insert("parent", parent.encode(env));
                map.insert("name", name.encode(env));
                ("unlink", map).encode(env)
            }
            FuseOperation::MkDir { parent, name, mode } => {
                let mut map = HashMap::new();
                map.insert("parent", parent.encode(env));
                map.insert("name", name.encode(env));
                map.insert("mode", mode.encode(env));
                ("mkdir", map).encode(env)
            }
            FuseOperation::RmDir { parent, name } => {
                let mut map = HashMap::new();
                map.insert("parent", parent.encode(env));
                map.insert("name", name.encode(env));
                ("rmdir", map).encode(env)
            }
            FuseOperation::Open { ino, flags } => {
                let mut map = HashMap::new();
                map.insert("ino", ino.encode(env));
                map.insert("flags", flags.encode(env));
                ("open", map).encode(env)
            }
            FuseOperation::Release { ino, fh, flags } => {
                let mut map = HashMap::new();
                map.insert("ino", ino.encode(env));
                map.insert("fh", fh.encode(env));
                map.insert("flags", flags.encode(env));
                ("release", map).encode(env)
            }
            FuseOperation::Rename {
                old_parent,
                old_name,
                new_parent,
                new_name,
            } => {
                let mut map = HashMap::new();
                map.insert("old_parent", old_parent.encode(env));
                map.insert("old_name", old_name.encode(env));
                map.insert("new_parent", new_parent.encode(env));
                map.insert("new_name", new_name.encode(env));
                ("rename", map).encode(env)
            }
            FuseOperation::SetAttr {
                ino,
                mode,
                uid,
                gid,
                size,
                atime,
                mtime,
            } => {
                let mut map = HashMap::new();
                map.insert("ino", ino.encode(env));
                if let Some(m) = mode {
                    map.insert("mode", m.encode(env));
                }
                if let Some(u) = uid {
                    map.insert("uid", u.encode(env));
                }
                if let Some(g) = gid {
                    map.insert("gid", g.encode(env));
                }
                if let Some(s) = size {
                    map.insert("size", s.encode(env));
                }
                if let Some((sec, nsec)) = atime {
                    map.insert("atime", (sec, nsec).encode(env));
                }
                if let Some((sec, nsec)) = mtime {
                    map.insert("mtime", (sec, nsec).encode(env));
                }
                ("setattr", map).encode(env)
            }
        }
    }
}

/// Reply from Elixir back to the FUSE thread
#[derive(Debug, Clone)]
#[allow(dead_code)] // Infrastructure for future FUSE operations (task 0012+)
pub enum FuseReply {
    /// Successful read with data
    ReadOk { data: Vec<u8> },
    /// Successful write with bytes written
    WriteOk { size: u32 },
    /// Successful lookup with inode attributes
    LookupOk { ino: u64, size: u64, kind: FileKind },
    /// Successful getattr with attributes
    AttrOk { ino: u64, size: u64, kind: FileKind },
    /// Successful readdir with entries
    ReadDirOk { entries: Vec<DirEntry> },
    /// Successful create with inode
    CreateOk { ino: u64 },
    /// Successful create with entry (for create operation)
    EntryOk {
        ino: u64,
        size: u64,
        kind: FileKind,
        fh: u64,
    },
    /// Successful open with file handle
    OpenOk { fh: u64 },
    /// Successful operation with no data
    Ok,
    /// Error with errno code
    Error { errno: i32 },
}

/// File type for FUSE attributes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Infrastructure for future FUSE operations (task 0012+)
pub enum FileKind {
    File,
    Directory,
    Symlink,
}

/// Directory entry
#[derive(Debug, Clone)]
#[allow(dead_code)] // Infrastructure for future FUSE operations (task 0012+)
pub struct DirEntry {
    pub ino: u64,
    pub name: String,
    pub kind: FileKind,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_types() {
        // Test that operation types can be constructed
        let _read = FuseOperation::Read {
            ino: 1,
            offset: 0,
            size: 4096,
        };
        let _write = FuseOperation::Write {
            ino: 1,
            offset: 0,
            data: vec![1, 2, 3],
        };
        let _lookup = FuseOperation::Lookup {
            parent: 1,
            name: "file.txt".to_string(),
        };
    }

    #[test]
    fn test_reply_types() {
        // Test that reply types can be constructed
        let _read_ok = FuseReply::ReadOk {
            data: vec![1, 2, 3],
        };
        let _write_ok = FuseReply::WriteOk { size: 1024 };
        let _error = FuseReply::Error { errno: 2 }; // ENOENT
    }
}
