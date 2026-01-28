/// FUSE server managing the communication with Elixir
///
/// This module implements the FuseServer which coordinates operations between
/// a FUSE thread and Elixir processes via channels.
use crate::channel::{Channels, OperationRequest, ReplyManager};
use crate::operation::FuseOperation;
use rustler::LocalPid;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Server managing FUSE session and channels
///
/// This is wrapped as a Rustler Resource so Elixir can hold a reference.
/// The server coordinates message passing between FUSE operations and Elixir.
pub struct FuseServer {
    /// PID of the Elixir process to send operations to
    callback_pid: LocalPid,
    /// Channel for sending operations
    operation_tx: mpsc::UnboundedSender<OperationRequest>,
    /// Manager for tracking pending replies
    reply_manager: ReplyManager,
    /// Shutdown signal
    shutdown: Arc<Mutex<bool>>,
}

impl FuseServer {
    /// Create a new FUSE server
    ///
    /// Returns the server and the receiver end of the operation channel.
    /// The receiver should be passed to a background task that forwards
    /// operations to the callback_pid.
    pub fn new(callback_pid: LocalPid) -> (Self, mpsc::UnboundedReceiver<OperationRequest>) {
        let channels = Channels::new();

        let server = Self {
            callback_pid,
            operation_tx: channels.operation_tx,
            reply_manager: channels.reply_manager,
            shutdown: Arc::new(Mutex::new(false)),
        };

        (server, channels.operation_rx)
    }

    /// Get the callback PID
    pub fn callback_pid(&self) -> LocalPid {
        self.callback_pid
    }

    /// Get a clone of the reply manager for use in background tasks
    pub fn reply_manager(&self) -> ReplyManager {
        self.reply_manager.clone()
    }

    /// Submit an operation to be handled by Elixir
    ///
    /// This is called from the mock FUSE thread (or real FUSE operations later).
    /// Returns a receiver that will be signaled with the reply.
    pub fn submit_operation(
        &self,
        operation: FuseOperation,
    ) -> Result<tokio::sync::oneshot::Receiver<crate::operation::FuseReply>, String> {
        if self.is_shutdown() {
            return Err("Server is shut down".to_string());
        }

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let id = self.reply_manager.register(reply_tx);

        // Create another channel for the OperationRequest struct
        // (this is a bit redundant but maintains the struct design)
        let (placeholder_tx, _placeholder_rx) = tokio::sync::oneshot::channel();

        let request = OperationRequest {
            id,
            operation,
            reply_tx: placeholder_tx,
        };

        self.operation_tx
            .send(request)
            .map_err(|_| "Failed to send operation to channel".to_string())?;

        Ok(reply_rx)
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        let mut shutdown = self.shutdown.lock().unwrap();
        *shutdown = true;
    }

    /// Check if shutdown has been signaled
    pub fn is_shutdown(&self) -> bool {
        let shutdown = self.shutdown.lock().unwrap();
        *shutdown
    }

    /// Get count of pending operations
    pub fn pending_count(&self) -> usize {
        self.reply_manager.pending_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_pid() -> LocalPid {
        // Create a dummy PID for testing
        // In real usage, this would be a valid Elixir PID
        unsafe { std::mem::zeroed() }
    }

    #[test]
    fn test_server_creation() {
        let pid = make_test_pid();
        let (server, _rx) = FuseServer::new(pid);
        assert_eq!(server.pending_count(), 0);
        assert!(!server.is_shutdown());
    }

    #[test]
    fn test_server_shutdown() {
        let pid = make_test_pid();
        let (server, _rx) = FuseServer::new(pid);

        assert!(!server.is_shutdown());
        server.shutdown();
        assert!(server.is_shutdown());
    }

    #[test]
    fn test_submit_operation_after_shutdown() {
        let pid = make_test_pid();
        let (server, _rx) = FuseServer::new(pid);

        server.shutdown();

        let operation = FuseOperation::Read {
            ino: 1,
            offset: 0,
            size: 4096,
        };

        let result = server.submit_operation(operation);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("shut down"));
    }
}
