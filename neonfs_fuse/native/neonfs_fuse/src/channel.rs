/// Channel management for FUSE-Elixir communication
///
/// This module provides the channel infrastructure for passing operations from
/// the FUSE thread to Elixir and receiving replies back.
use crate::operation::{FuseOperation, FuseReply};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

/// Unique identifier for tracking operation requests/replies
pub type RequestId = u64;

/// Message sent from FUSE thread to Elixir
#[derive(Debug)]
pub struct OperationRequest {
    pub id: RequestId,
    pub operation: FuseOperation,
    #[allow(dead_code)] // Will be used for async FUSE operations (task 0012+)
    pub reply_tx: oneshot::Sender<FuseReply>,
}

/// Manages pending operations waiting for replies
#[derive(Clone)]
pub struct ReplyManager {
    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<FuseReply>>>>,
    next_id: Arc<Mutex<RequestId>>,
}

impl ReplyManager {
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(0)),
        }
    }

    /// Register a new operation and get a request ID
    pub fn register(&self, reply_tx: oneshot::Sender<FuseReply>) -> RequestId {
        let mut next_id = self.next_id.lock().unwrap();
        let id = *next_id;
        *next_id = next_id.wrapping_add(1);

        let mut pending = self.pending.lock().unwrap();
        pending.insert(id, reply_tx);

        id
    }

    /// Send a reply for a pending operation
    pub fn reply(&self, id: RequestId, reply: FuseReply) -> Result<(), String> {
        let mut pending = self.pending.lock().unwrap();
        if let Some(tx) = pending.remove(&id) {
            tx.send(reply)
                .map_err(|_| format!("Failed to send reply for request {}", id))
        } else {
            Err(format!("No pending request with id {}", id))
        }
    }

    /// Cancel a pending operation (e.g., on timeout)
    #[allow(dead_code)] // Will be used for timeout handling (task 0012+)
    pub fn cancel(&self, id: RequestId) {
        let mut pending = self.pending.lock().unwrap();
        pending.remove(&id);
    }

    /// Get count of pending operations
    pub fn pending_count(&self) -> usize {
        let pending = self.pending.lock().unwrap();
        pending.len()
    }
}

/// Channels for FUSE-Elixir communication
pub struct Channels {
    /// Channel for sending operations from FUSE to Elixir
    pub operation_tx: mpsc::UnboundedSender<OperationRequest>,
    /// Channel for receiving operations in the coordinator
    pub operation_rx: mpsc::UnboundedReceiver<OperationRequest>,
    /// Manager for pending replies
    pub reply_manager: ReplyManager,
}

impl Channels {
    pub fn new() -> Self {
        let (operation_tx, operation_rx) = mpsc::unbounded_channel();
        let reply_manager = ReplyManager::new();

        Self {
            operation_tx,
            operation_rx,
            reply_manager,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reply_manager_register() {
        let manager = ReplyManager::new();
        let (_tx, _rx) = oneshot::channel();

        let id1 = manager.register(_tx);
        let (_tx2, _rx2) = oneshot::channel();
        let id2 = manager.register(_tx2);

        assert_ne!(id1, id2);
        assert_eq!(manager.pending_count(), 2);
    }

    #[test]
    fn test_reply_manager_reply() {
        let manager = ReplyManager::new();
        let (tx, mut rx) = oneshot::channel();

        let id = manager.register(tx);
        assert_eq!(manager.pending_count(), 1);

        let reply = FuseReply::Ok;
        manager.reply(id, reply).unwrap();

        assert_eq!(manager.pending_count(), 0);
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn test_reply_manager_cancel() {
        let manager = ReplyManager::new();
        let (tx, _rx) = oneshot::channel();

        let id = manager.register(tx);
        assert_eq!(manager.pending_count(), 1);

        manager.cancel(id);
        assert_eq!(manager.pending_count(), 0);
    }

    #[test]
    fn test_reply_manager_reply_unknown() {
        let manager = ReplyManager::new();
        let reply = FuseReply::Ok;
        let result = manager.reply(999, reply);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No pending request"));
    }

    #[test]
    fn test_channels_creation() {
        let channels = Channels::new();
        assert_eq!(channels.reply_manager.pending_count(), 0);
    }
}
