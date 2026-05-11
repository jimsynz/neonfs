//! FUSE mount management commands

use crate::daemon::DaemonConnection;
use crate::error::{CliError, Result};
use crate::output::{json, table, OutputFormat};
use crate::term::types::MountInfo;
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, Map, Term};
use std::collections::HashMap;
use std::path::Path;

/// Daemon-owned path used to discover the daemon's effective uid for the
/// pre-flight ownership check. Matches the StateDirectory in the packaged
/// systemd units; overridable via `NEONFS_DATA_DIR` env var so tests
/// (and non-default deployments) can point elsewhere.
const DEFAULT_DAEMON_DATA_DIR: &str = "/var/lib/neonfs";

/// FUSE mount management subcommands
#[derive(Debug, Subcommand)]
pub enum FuseCommand {
    /// Mount a volume
    Mount {
        /// Volume name
        volume: String,

        /// Mount point path
        mountpoint: String,
    },

    /// Unmount a volume
    Unmount {
        /// Mount point path
        mountpoint: String,
    },

    /// List all mounts
    List,
}

impl FuseCommand {
    /// Execute the FUSE command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            FuseCommand::Mount { volume, mountpoint } => self.mount(volume, mountpoint, format),
            FuseCommand::Unmount { mountpoint } => self.unmount(mountpoint, format),
            FuseCommand::List => self.list(format),
        }
    }

    fn mount(&self, volume: &str, mountpoint: &str, format: OutputFormat) -> Result<()> {
        preflight_check_mountpoint(mountpoint)?;

        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });
        let mountpoint_term = Term::Binary(Binary {
            bytes: mountpoint.as_bytes().to_vec(),
        });
        let options_term = Term::Map(Map {
            map: HashMap::new(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "mount",
                vec![volume_term, mountpoint_term, options_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let mount = MountInfo::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&mount)?);
            }
            OutputFormat::Table => {
                println!(
                    "✓ Volume '{}' mounted at '{}'",
                    mount.volume_name, mount.mount_point
                );
                println!("  Mount ID: {}", mount.id);
            }
        }
        Ok(())
    }

    fn unmount(&self, mountpoint: &str, format: OutputFormat) -> Result<()> {
        let mountpoint_term = Term::Binary(Binary {
            bytes: mountpoint.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "unmount",
                vec![mountpoint_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "success",
                    "mountpoint": mountpoint,
                    "message": "Volume unmounted"
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("✓ Volume unmounted from '{}'", mountpoint);
            }
        }
        Ok(())
    }

    fn list(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "list_mounts", vec![])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;

        let mount_terms = term_to_list(&data)?;
        let mounts: Result<Vec<MountInfo>> =
            mount_terms.into_iter().map(MountInfo::from_term).collect();
        let mounts = mounts?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&mounts)?);
            }
            OutputFormat::Table => {
                if mounts.is_empty() {
                    println!("No active mounts");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "NODE".to_string(),
                        "VOLUME".to_string(),
                        "MOUNTPOINT".to_string(),
                        "MOUNTED AT".to_string(),
                    ]);
                    for mount in &mounts {
                        tbl.add_row(vec![
                            mount.node.clone(),
                            mount.volume_name.clone(),
                            mount.mount_point.clone(),
                            mount.started_at.clone(),
                        ]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }
}

/// Pre-flight checks for `fuse mount` (issue #757).
///
/// Catch the most common operator mistakes locally — before the round
/// trip to the daemon — and produce a specific error message rather
/// than the opaque `:fusermount_no_fd` atom. The daemon-side validation
/// remains the authoritative defence; this is a UX layer.
///
/// Checks (in order):
/// 1. Path is absolute.
/// 2. Path exists.
/// 3. Path is a directory (and not a regular file / symlink to a file).
/// 4. Daemon uid (discovered by stat'ing the daemon data dir, default
///    `/var/lib/neonfs`, overridable via `NEONFS_DATA_DIR`) owns the
///    mount point. Skipped silently when the data dir is missing — that
///    case is more useful as the connection error than as a mount-point
///    error.
/// 5. Mount point is empty — warning, not an error: mounting over a
///    non-empty directory hides its contents.
fn preflight_check_mountpoint(mountpoint: &str) -> Result<()> {
    let path = Path::new(mountpoint);

    if !path.is_absolute() {
        return Err(CliError::InvalidArgument(format!(
            "Mount point must be an absolute path: {}",
            mountpoint
        )));
    }

    let metadata = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(CliError::InvalidArgument(format!(
                "Mount point does not exist. Create it with: \
                 sudo mkdir -p {} && sudo chown neonfs:neonfs {}",
                mountpoint, mountpoint
            )));
        }
        Err(e) => {
            return Err(CliError::InvalidArgument(format!(
                "Cannot stat mount point {}: {}",
                mountpoint, e
            )));
        }
    };

    // Resolve symlinks for the directory check (a symlink to a directory
    // is acceptable as a mount point; a symlink to a file is not).
    let target_metadata = if metadata.file_type().is_symlink() {
        match std::fs::metadata(path) {
            Ok(m) => m,
            Err(e) => {
                return Err(CliError::InvalidArgument(format!(
                    "Cannot resolve mount point symlink {}: {}",
                    mountpoint, e
                )));
            }
        }
    } else {
        metadata.clone()
    };

    if !target_metadata.is_dir() {
        return Err(CliError::InvalidArgument(format!(
            "Mount point is not a directory: {}",
            mountpoint
        )));
    }

    check_mountpoint_owner(path, &target_metadata)?;
    warn_if_not_empty(path, mountpoint);

    Ok(())
}

fn check_mountpoint_owner(path: &Path, metadata: &std::fs::Metadata) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let data_dir =
        std::env::var("NEONFS_DATA_DIR").unwrap_or_else(|_| DEFAULT_DAEMON_DATA_DIR.to_string());

    let daemon_uid = match std::fs::metadata(&data_dir) {
        Ok(m) => m.uid(),
        // No daemon data dir — either the daemon isn't installed or
        // it's somewhere unconventional. Skip the check; the daemon
        // connection step will produce a clearer error.
        Err(_) => return Ok(()),
    };

    let mount_uid = metadata.uid();

    // root (uid 0) can mount anywhere; skip the check for root daemons.
    if daemon_uid == 0 || daemon_uid == mount_uid {
        return Ok(());
    }

    Err(CliError::InvalidArgument(format!(
        "Mount point must be owned by the neonfs user \
         (currently owned by uid={}, daemon uid={}). \
         Run: sudo chown neonfs:neonfs {}",
        mount_uid,
        daemon_uid,
        path.display()
    )))
}

fn warn_if_not_empty(path: &Path, mountpoint: &str) {
    let mut entries = match std::fs::read_dir(path) {
        Ok(it) => it,
        Err(_) => return,
    };

    if entries.next().is_some() {
        eprintln!(
            "Warning: {} is not empty; existing files will be hidden while mounted.",
            mountpoint
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tempfile::TempDir;

    fn with_data_dir<F: FnOnce()>(data_dir: &str, body: F) {
        // Tests are not parallel-safe across this env var; the cargo
        // default of one thread per test is fine for the small number
        // of tests here.
        let prev = std::env::var("NEONFS_DATA_DIR").ok();
        // Safety: tests in this module are not run concurrently with
        // other code that reads NEONFS_DATA_DIR.
        unsafe {
            std::env::set_var("NEONFS_DATA_DIR", data_dir);
        }
        body();
        unsafe {
            match prev {
                Some(v) => std::env::set_var("NEONFS_DATA_DIR", v),
                None => std::env::remove_var("NEONFS_DATA_DIR"),
            }
        }
    }

    #[test]
    fn preflight_rejects_relative_path() {
        let err = preflight_check_mountpoint("relative/path").unwrap_err();
        assert!(matches!(err, CliError::InvalidArgument(ref m) if m.contains("absolute")));
    }

    #[test]
    fn preflight_rejects_missing_path() {
        let tmp = TempDir::new().unwrap();
        let missing = tmp.path().join("does-not-exist");
        let missing_str = missing.to_str().unwrap();

        let err = preflight_check_mountpoint(missing_str).unwrap_err();
        let msg = match err {
            CliError::InvalidArgument(m) => m,
            other => panic!("expected InvalidArgument, got {:?}", other),
        };
        assert!(msg.contains("does not exist"));
        assert!(msg.contains("mkdir -p"));
        assert!(msg.contains(missing_str));
    }

    #[test]
    #[serial(NEONFS_DATA_DIR)]
    fn preflight_rejects_regular_file() {
        let tmp = TempDir::new().unwrap();
        let file = tmp.path().join("a-file");
        std::fs::write(&file, b"").unwrap();
        let file_str = file.to_str().unwrap();

        with_data_dir(tmp.path().to_str().unwrap(), || {
            let err = preflight_check_mountpoint(file_str).unwrap_err();
            let msg = match err {
                CliError::InvalidArgument(m) => m,
                other => panic!("expected InvalidArgument, got {:?}", other),
            };
            assert!(msg.contains("is not a directory"));
            assert!(msg.contains(file_str));
        });
    }

    #[test]
    #[serial(NEONFS_DATA_DIR)]
    fn preflight_accepts_empty_directory_with_matching_owner() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("mountpoint");
        std::fs::create_dir(&dir).unwrap();

        // The data dir is owned by the same uid as the mountpoint
        // (both created by this test process), so the ownership check
        // should pass.
        with_data_dir(tmp.path().to_str().unwrap(), || {
            preflight_check_mountpoint(dir.to_str().unwrap()).unwrap();
        });
    }

    #[test]
    #[serial(NEONFS_DATA_DIR)]
    fn preflight_skips_owner_check_when_data_dir_missing() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("mountpoint");
        std::fs::create_dir(&dir).unwrap();

        // Point at a nonexistent data dir — the ownership check is
        // skipped (daemon connection will report the real error first).
        with_data_dir("/this/path/does/not/exist", || {
            preflight_check_mountpoint(dir.to_str().unwrap()).unwrap();
        });
    }
}
