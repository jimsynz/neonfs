/// Channel management for NFS-Elixir communication
///
/// Provides the channel infrastructure for passing operations from
/// the NFS server thread to Elixir and receiving replies back.
use nfs3_server::nfs3_types::nfs3::nfsstat3;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// Unique identifier for tracking operation requests/replies
pub type RequestId = u64;

/// Manages pending operations waiting for replies
#[derive(Clone)]
pub struct ReplyManager {
    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<NfsReply>>>>,
    next_id: Arc<Mutex<RequestId>>,
}

/// Reply from Elixir back to the NFS server
#[derive(Debug, Clone)]
pub enum NfsReply {
    /// Successful operation with data (encoded as Elixir term)
    Ok(NfsReplyData),
    /// Error with NFS status code
    Error(nfsstat3),
}

/// Data payload for successful NFS replies
#[derive(Debug, Clone)]
pub enum NfsReplyData {
    /// No data (for operations like remove, mkdir that just succeed)
    Empty,
    /// File attributes
    Attrs(FileAttrs),
    /// Lookup result: file ID + attributes + optional volume ID override
    Lookup(u64, FileAttrs, Option<Vec<u8>>),
    /// Read result: data bytes + EOF flag
    Read(Vec<u8>, bool),
    /// Directory entries for readdirplus
    DirEntries(Vec<DirEntryInfo>),
    /// Create result: file ID + attributes
    Create(u64, FileAttrs),
    /// Write result: bytes written + new file attributes
    Write(u32, FileAttrs),
    /// Readlink result: target path
    Readlink(String),
}

/// File attributes returned from Elixir
#[derive(Debug, Clone, Default)]
pub struct FileAttrs {
    pub file_id: u64,
    pub size: u64,
    pub kind: FileKind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub atime_secs: i64,
    pub atime_nsecs: u32,
    pub mtime_secs: i64,
    pub mtime_nsecs: u32,
    pub ctime_secs: i64,
    pub ctime_nsecs: u32,
}

/// File type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FileKind {
    #[default]
    File,
    Directory,
    Symlink,
}

/// Directory entry information for readdirplus
#[derive(Debug, Clone)]
pub struct DirEntryInfo {
    pub file_id: u64,
    pub name: String,
    pub attrs: FileAttrs,
    /// Optional volume ID override (used for virtual root entries)
    pub volume_id: Option<Vec<u8>>,
}

impl ReplyManager {
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(0)),
        }
    }

    /// Register a new operation and get a request ID + receiver
    pub fn register(&self) -> (RequestId, oneshot::Receiver<NfsReply>) {
        let (tx, rx) = oneshot::channel();

        let mut next_id = self.next_id.lock().unwrap();
        let id = *next_id;
        *next_id = next_id.wrapping_add(1);

        let mut pending = self.pending.lock().unwrap();
        pending.insert(id, tx);

        (id, rx)
    }

    /// Send a reply for a pending operation
    pub fn reply(&self, id: RequestId, reply: NfsReply) -> Result<(), String> {
        let mut pending = self.pending.lock().unwrap();
        if let Some(tx) = pending.remove(&id) {
            tx.send(reply)
                .map_err(|_| format!("Failed to send reply for request {}", id))
        } else {
            Err(format!("No pending request with id {}", id))
        }
    }

    /// Get count of pending operations
    pub fn pending_count(&self) -> usize {
        let pending = self.pending.lock().unwrap();
        pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reply_manager_register() {
        let manager = ReplyManager::new();

        let (id1, _rx1) = manager.register();
        let (id2, _rx2) = manager.register();

        assert_ne!(id1, id2);
        assert_eq!(manager.pending_count(), 2);
    }

    #[test]
    fn test_reply_manager_reply() {
        let manager = ReplyManager::new();
        let (id, mut rx) = manager.register();
        assert_eq!(manager.pending_count(), 1);

        let reply = NfsReply::Ok(NfsReplyData::Empty);
        manager.reply(id, reply).unwrap();

        assert_eq!(manager.pending_count(), 0);
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn test_reply_manager_reply_unknown() {
        let manager = ReplyManager::new();
        let reply = NfsReply::Ok(NfsReplyData::Empty);
        let result = manager.reply(999, reply);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No pending request"));
    }
}
