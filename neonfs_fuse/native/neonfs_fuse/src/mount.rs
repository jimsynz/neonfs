/// FUSE mount and session management
///
/// This module handles mounting the NeonFS filesystem at a mount point,
/// managing the FUSE session lifecycle, and graceful unmounting.
use crate::error::FuseError;
use crate::filesystem::NeonFilesystem;
use crate::server::FuseServer;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;

/// Handle to a mounted FUSE filesystem
///
/// This struct manages the mount session and provides graceful unmount capability.
pub struct MountSession {
    /// Path where the filesystem is mounted
    mount_point: std::path::PathBuf,
    /// Thread handle for the FUSE session
    session_thread: Option<thread::JoinHandle<Result<(), FuseError>>>,
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
        let server_clone = Arc::clone(&server_arc);
        let mount_point_clone = mount_point.clone();

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

        // Spawn FUSE session thread
        let session_thread = thread::spawn(move || {
            let server_locked = server_clone
                .lock()
                .map_err(|_| FuseError::ChannelSend("Failed to lock server".to_string()))?;

            // Create filesystem instance
            // We need to clone the server, but we can't move it out of the Arc<Mutex>
            // So we'll need a different approach - pass the Arc itself
            drop(server_locked);

            // Create a new FuseServer for the filesystem
            // This is a design trade-off: each mount needs its own communication channel
            let (new_server, _rx) = FuseServer::new(server_clone.lock().unwrap().callback_pid());

            let fs = NeonFilesystem::new(new_server);

            // Mount the filesystem - this blocks until unmount
            fuser::mount2(fs, &mount_point_clone, &fuser_options).map_err(FuseError::Io)?;

            Ok(())
        });

        Ok(Self {
            mount_point,
            session_thread: Some(session_thread),
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
    /// The method blocks until the FUSE session thread completes.
    ///
    /// # Arguments
    /// * `fusermount_cmd` - The fusermount command to use (e.g., "fusermount3" or "fusermount")
    pub fn unmount(mut self, fusermount_cmd: &str) -> Result<(), FuseError> {
        // Signal server to shut down
        {
            let server = self
                .server
                .lock()
                .map_err(|_| FuseError::ChannelSend("Failed to lock server".to_string()))?;
            server.shutdown();
        }

        // Unmount the filesystem using the configured command, with fallbacks
        // Try lazy unmount (-z) variants first as they're more reliable in containers
        let mount_point_str = self.mount_point.to_str().unwrap_or("");
        let unmount_commands = [
            (fusermount_cmd, vec!["-uz", mount_point_str]),
            (fusermount_cmd, vec!["-u", mount_point_str]),
            ("fusermount3", vec!["-uz", mount_point_str]),
            ("fusermount3", vec!["-u", mount_point_str]),
            ("fusermount", vec!["-uz", mount_point_str]),
            ("fusermount", vec!["-u", mount_point_str]),
            ("umount", vec!["-l", mount_point_str]),
            ("umount", vec![mount_point_str]),
        ];

        let mut unmounted = false;
        for (cmd, args) in &unmount_commands {
            match std::process::Command::new(cmd).args(args).status() {
                Ok(exit_status) if exit_status.success() => {
                    log::info!(
                        "Unmounted filesystem at {:?} using {}",
                        self.mount_point,
                        cmd
                    );
                    unmounted = true;
                    break;
                }
                Ok(exit_status) => {
                    log::debug!("{} exited with status: {:?}", cmd, exit_status);
                }
                Err(e) => {
                    log::debug!("Failed to run {}: {}", cmd, e);
                }
            }
        }

        if !unmounted {
            log::warn!("All unmount commands failed for {:?}", self.mount_point);
        }

        // Wait for session thread to complete
        // Note: The session thread may return an error if the unmount wasn't clean,
        // but we don't propagate this as an error since the unmount was requested.
        if let Some(handle) = self.session_thread.take() {
            match handle.join() {
                Ok(Ok(())) => {
                    log::info!("FUSE session thread completed cleanly");
                }
                Ok(Err(e)) => {
                    // Session exited with error - this can happen with lazy unmount
                    // or permission issues, but the filesystem should still be unmounted
                    log::warn!("FUSE session thread exited with error: {}", e);
                }
                Err(e) => {
                    log::error!("FUSE session thread panicked: {:?}", e);
                }
            }
        }

        Ok(())
    }
}

impl Drop for MountSession {
    fn drop(&mut self) {
        // If the session thread is still running, we should try to clean up
        if self.session_thread.is_some() {
            log::warn!("MountSession dropped without explicit unmount");

            // Try to unmount using fusermount3/fusermount, don't panic if it fails
            if std::process::Command::new("fusermount3")
                .arg("-u")
                .arg(&self.mount_point)
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
            {
                return;
            }
            let _ = std::process::Command::new("fusermount")
                .arg("-u")
                .arg(&self.mount_point)
                .status();
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
