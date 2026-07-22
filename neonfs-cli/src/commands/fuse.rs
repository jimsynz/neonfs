//! FUSE mount management commands

use crate::daemon::DaemonConnection;
use crate::error::{CliError, Result};
use crate::output::{json, table, OutputFormat};
use crate::term::types::MountInfo;
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Atom, Binary, Map, Term};
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

        /// Let any user access the mount (FUSE `allow_other`). The daemon
        /// runs as the `neonfs` user, so without this the mount is
        /// inaccessible even to root. Requires `user_allow_other` in
        /// `/etc/fuse.conf` when the daemon is not root.
        #[arg(long)]
        allow_other: bool,

        /// Let root — and only root — access the mount (FUSE `allow_root`).
        /// Tighter than `--allow-other`. Requires `user_allow_other` in
        /// `/etc/fuse.conf` when the daemon is not root.
        #[arg(long, conflicts_with = "allow_other")]
        allow_root: bool,
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
            FuseCommand::Mount {
                volume,
                mountpoint,
                allow_other,
                allow_root,
            } => self.mount(volume, mountpoint, *allow_other, *allow_root, format),
            FuseCommand::Unmount { mountpoint } => self.unmount(mountpoint, format),
            FuseCommand::List => self.list(format),
        }
    }

    fn mount(
        &self,
        volume: &str,
        mountpoint: &str,
        allow_other: bool,
        allow_root: bool,
        format: OutputFormat,
    ) -> Result<()> {
        preflight_check_mountpoint(mountpoint)?;

        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });
        let mountpoint_term = Term::Binary(Binary {
            bytes: mountpoint.as_bytes().to_vec(),
        });

        let options_term = build_mount_options(local_hostname(), allow_other, allow_root);

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

/// Builds the mount `options` map sent to the daemon.
///
/// * `host` — the operator's hostname (from `local_hostname`), so the daemon
///   routes the mount to a co-located FUSE node; the mount point only exists
///   here, even when the RPC lands on a core node elsewhere (#1359).
/// * `allow_other` / `allow_root` — FUSE access options threaded through to
///   `MountManager.build_mount_options/1` (#1574). Sent as ETF `true` atoms so
///   the core-side `Map.get(opts, "allow_other")` check sees a boolean.
///
/// Absent/`false` keys are omitted rather than encoded as `false`, matching how
/// the daemon defaults them off.
fn build_mount_options(host: Option<String>, allow_other: bool, allow_root: bool) -> Term {
    let mut options = HashMap::new();

    if let Some(host) = host {
        options.insert(binary("host"), binary(&host));
    }
    if allow_other {
        options.insert(binary("allow_other"), Term::Atom(Atom::from("true")));
    }
    if allow_root {
        options.insert(binary("allow_root"), Term::Atom(Atom::from("true")));
    }

    Term::Map(Map { map: options })
}

fn binary(value: &str) -> Term {
    Term::Binary(Binary {
        bytes: value.as_bytes().to_vec(),
    })
}

/// The host's name, for matching against the `@host` of cluster node
/// names so a mount lands on a co-located FUSE node (#1359). `None` when
/// the syscall fails or the name isn't valid UTF-8 — the daemon then
/// falls back to its default FUSE discovery chain.
fn local_hostname() -> Option<String> {
    let mut buf = [0_u8; 256];
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if rc != 0 {
        return None;
    }

    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    std::str::from_utf8(&buf[..end])
        .ok()
        .map(|name| name.to_string())
        .filter(|name| !name.is_empty())
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
    fn local_hostname_is_trimmed_and_non_empty() {
        // gethostname succeeds in every realistic environment; when it
        // does the result must be NUL-trimmed and non-empty so it can
        // match an `@host` node-name suffix.
        if let Some(host) = local_hostname() {
            assert!(!host.is_empty());
            assert!(!host.contains('\0'));
        }
    }

    fn options_map(term: &Term) -> HashMap<String, Term> {
        match term {
            Term::Map(Map { map }) => map
                .iter()
                .map(|(k, v)| match k {
                    Term::Binary(Binary { bytes }) => {
                        (String::from_utf8(bytes.clone()).unwrap(), v.clone())
                    }
                    other => panic!("expected binary key, got {:?}", other),
                })
                .collect(),
            other => panic!("expected map, got {:?}", other),
        }
    }

    #[test]
    fn build_mount_options_omits_flags_when_false() {
        let map = options_map(&build_mount_options(None, false, false));
        assert!(map.is_empty());
    }

    #[test]
    fn build_mount_options_encodes_allow_other_as_true_atom() {
        let map = options_map(&build_mount_options(None, true, false));
        assert_eq!(
            map.get("allow_other"),
            Some(&Term::Atom(Atom::from("true")))
        );
        assert!(!map.contains_key("allow_root"));
    }

    #[test]
    fn build_mount_options_encodes_allow_root_as_true_atom() {
        let map = options_map(&build_mount_options(None, false, true));
        assert_eq!(map.get("allow_root"), Some(&Term::Atom(Atom::from("true"))));
        assert!(!map.contains_key("allow_other"));
    }

    #[test]
    fn build_mount_options_keeps_host_alongside_flags() {
        let map = options_map(&build_mount_options(
            Some("myhost".to_string()),
            true,
            false,
        ));
        assert_eq!(
            map.get("host"),
            Some(&Term::Binary(Binary {
                bytes: b"myhost".to_vec()
            }))
        );
        assert_eq!(
            map.get("allow_other"),
            Some(&Term::Atom(Atom::from("true")))
        );
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
