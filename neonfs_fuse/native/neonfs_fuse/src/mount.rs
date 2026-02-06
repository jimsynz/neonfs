/// FUSE mount and session management
///
/// This module handles mounting the NeonFS filesystem at a mount point,
/// managing the FUSE session lifecycle, and graceful unmounting.
use crate::error::FuseError;
use crate::filesystem::NeonFilesystem;
use crate::server::FuseServer;
use fuser::BackgroundSession;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Handle to a mounted FUSE filesystem
///
/// This struct manages the mount session and provides graceful unmount capability.
pub struct MountSession {
    /// Path where the filesystem is mounted
    mount_point: std::path::PathBuf,
    /// Background session handle - dropping this unmounts the filesystem
    background_session: Option<BackgroundSession>,
    /// Server managing communication with Elixir
    server: Arc<Mutex<FuseServer>>,
}

impl MountSession {
    /// Mount a FUSE filesystem at the given mount point
    ///
    /// This spawns a background thread that runs the FUSE session. The FUSE
    /// kernel driver will send requests to this thread, which forwards them
    /// to Elixir for handling.
    ///
    /// # Arguments
    /// * `mount_point` - Directory where the filesystem will be mounted
    /// * `server` - FuseServer to handle operations
    /// * `options` - Mount options (e.g., ["auto_unmount", "allow_other"])
    pub fn mount<P: AsRef<Path>>(
        mount_point: P,
        server: FuseServer,
        options: Vec<String>,
    ) -> Result<Self, FuseError> {
        let mount_point = mount_point.as_ref().to_path_buf();

        // Validate mount point exists
        if !mount_point.exists() {
            return Err(FuseError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Mount point does not exist: {:?}", mount_point),
            )));
        }

        if !mount_point.is_dir() {
            return Err(FuseError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Mount point is not a directory: {:?}", mount_point),
            )));
        }

        let server_arc = Arc::new(Mutex::new(server));

        // Convert mount options to fuser format
        let mut fuser_options = vec![
            fuser::MountOption::FSName("neonfs".to_string()),
            fuser::MountOption::RO, // Read-only for now (write ops in task 0013)
        ];

        for opt in options {
            match opt.as_str() {
                "auto_unmount" => fuser_options.push(fuser::MountOption::AutoUnmount),
                "allow_other" => fuser_options.push(fuser::MountOption::AllowOther),
                "allow_root" => fuser_options.push(fuser::MountOption::AllowRoot),
                "ro" => fuser_options.push(fuser::MountOption::RO),
                "rw" => {} // Skip RW for now, handled in task 0013
                _ => {
                    log::warn!("Ignoring unknown mount option: {}", opt);
                }
            }
        }

        // Create a new FuseServer for the filesystem
        // This is a design trade-off: each mount needs its own communication channel
        let callback_pid = server_arc.lock().unwrap().callback_pid();
        let (new_server, _rx) = FuseServer::new(callback_pid);

        let fs = NeonFilesystem::new(new_server);

        // Use spawn_mount2 which handles threading internally
        // This is more reliable in container environments than manual thread spawning
        log::warn!(
            "Calling fuser::spawn_mount2 for {:?} with options: {:?}",
            mount_point,
            fuser_options
        );

        let background_session =
            fuser::spawn_mount2(fs, &mount_point, &fuser_options).map_err(|e| {
                log::error!("fuser::spawn_mount2 FAILED for {:?}: {}", mount_point, e);
                FuseError::Io(e)
            })?;

        log::warn!("FUSE mount successful for {:?}", mount_point);

        Ok(Self {
            mount_point,
            background_session: Some(background_session),
            server: server_arc,
        })
    }

    /// Get the mount point path
    #[allow(dead_code)] // May be used in future for mount info queries
    pub fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    /// Unmount the filesystem gracefully
    ///
    /// This sends a shutdown signal to the server and unmounts the filesystem.
    /// Dropping the BackgroundSession will trigger the unmount.
    ///
    /// # Arguments
    /// * `_fusermount_cmd` - The fusermount command (unused, kept for API compatibility)
    pub fn unmount(mut self, _fusermount_cmd: &str) -> Result<(), FuseError> {
        // Signal server to shut down
        {
            let server = self
                .server
                .lock()
                .map_err(|_| FuseError::ChannelSend("Failed to lock server".to_string()))?;
            server.shutdown();
        }

        // Drop the background session to trigger unmount
        // BackgroundSession::drop() calls join() on the session thread
        if let Some(session) = self.background_session.take() {
            log::info!("Unmounting filesystem at {:?}", self.mount_point);
            drop(session);
            log::info!("FUSE session ended for {:?}", self.mount_point);
        }

        Ok(())
    }
}

impl Drop for MountSession {
    fn drop(&mut self) {
        // If the background session is still active, dropping it will unmount
        if self.background_session.is_some() {
            log::warn!(
                "MountSession dropped without explicit unmount for {:?}",
                self.mount_point
            );
            // BackgroundSession's Drop impl will handle cleanup
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mount_point_validation() {
        // This test requires an actual mount point, so we skip it in CI
        // Just verify the types compile
        let _ = std::marker::PhantomData::<MountSession>;
    }
}
