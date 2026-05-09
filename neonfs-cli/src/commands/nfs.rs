//! NFS export and mount management commands

use crate::daemon::DaemonConnection;
use crate::error::{CliError, Result};
use crate::output::{json, table, OutputFormat};
use crate::term::types::NfsExportInfo;
use crate::term::{extract_error, term_to_list, term_to_map, term_to_string, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, Term};
use std::process::Command;

/// NFS export and mount subcommands
#[derive(Debug, Subcommand)]
pub enum NfsCommand {
    /// Export a volume via NFS
    Export {
        /// Volume name
        volume: String,
    },

    /// Unexport a volume from NFS
    Unexport {
        /// Volume name
        volume: String,
    },

    /// List all NFS exports
    List,

    /// Mount an NFS-exported volume locally as the calling user.
    ///
    /// Runs `mount.nfs` in the CLI process so the kernel checks the
    /// **caller's** permissions on the mountpoint (not the daemon's
    /// service-user identity — see #847). Typically requires
    /// privileges to invoke `mount(2)`; run with `sudo` when the
    /// caller isn't already root.
    Mount {
        /// Volume name (must already be exported via `neonfs nfs export`)
        volume: String,

        /// Local mountpoint (must exist and be writable by the caller)
        mountpoint: String,

        /// Extra mount options appended to the default
        /// `nfsvers=3,proto=tcp,...`. Comma-separated.
        #[arg(long)]
        options: Option<String>,
    },

    /// Unmount a previously-mounted NFS volume.
    ///
    /// Runs `umount` in the CLI process. Requires the same privileges
    /// `mount` did.
    Unmount {
        /// Local mountpoint to unmount
        mountpoint: String,
    },
}

impl NfsCommand {
    /// Execute the NFS command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            NfsCommand::Export { volume } => self.export(volume, format),
            NfsCommand::Unexport { volume } => self.unexport(volume, format),
            NfsCommand::List => self.list(format),
            NfsCommand::Mount {
                volume,
                mountpoint,
                options,
            } => self.mount(volume, mountpoint, options.as_deref(), format),
            NfsCommand::Unmount { mountpoint } => self.unmount(mountpoint, format),
        }
    }

    fn export(&self, volume: &str, format: OutputFormat) -> Result<()> {
        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "nfs_export", vec![volume_term])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let export = NfsExportInfo::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&export)?);
            }
            OutputFormat::Table => {
                println!("✓ Volume '{}' exported via NFS", export.volume_name);
                println!();
                println!("  To mount:");
                let port_opt = if export.port != 2049 {
                    format!(",port={},mountport={}", export.port, export.port)
                } else {
                    String::new()
                };
                println!(
                    "    mount -t nfs -o nfsvers=3,proto=tcp{} {}:/{} /mnt/point",
                    port_opt, export.server_address, export.volume_name
                );
            }
        }
        Ok(())
    }

    fn unexport(&self, volume: &str, format: OutputFormat) -> Result<()> {
        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "nfs_unexport",
                vec![volume_term],
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
                    "volume": volume,
                    "message": "Volume unexported"
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("✓ Volume '{}' unexported from NFS", volume);
            }
        }
        Ok(())
    }

    fn mount(
        &self,
        volume: &str,
        mountpoint: &str,
        extra_options: Option<&str>,
        format: OutputFormat,
    ) -> Result<()> {
        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_nfs_mount_request",
                vec![volume_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let server_address = map
            .get("server_address")
            .and_then(|t| term_to_string(t).ok())
            .ok_or_else(|| {
                CliError::TermConversionError("missing server_address in mount params".into())
            })?;

        let port = map
            .get("port")
            .and_then(|t| crate::term::term_to_i64(t).ok())
            .unwrap_or(2049);

        let export_path = map
            .get("export_path")
            .and_then(|t| term_to_string(t).ok())
            .unwrap_or_else(|| format!("/{}", volume));

        let nfs_target = format!("{}:{}", server_address, export_path);

        let mut options =
            String::from("nfsvers=3,proto=tcp,nolock,hard,intr,rsize=1048576,wsize=1048576");

        if port != 2049 {
            options.push_str(&format!(",port={},mountport={}", port, port));
        }

        if let Some(extra) = extra_options {
            options.push(',');
            options.push_str(extra);
        }

        let status = Command::new("mount.nfs")
            .arg("-o")
            .arg(&options)
            .arg(&nfs_target)
            .arg(mountpoint)
            .status()
            .map_err(|e| {
                CliError::InvalidArgument(format!(
                    "failed to invoke mount.nfs (is it installed?): {}",
                    e
                ))
            })?;

        if !status.success() {
            return Err(CliError::InvalidArgument(format!(
                "mount.nfs exited with {} (target={}, mountpoint={}); \
                 hint: run with sudo if the kernel rejected the mount(2) on permission grounds",
                status, nfs_target, mountpoint,
            )));
        }

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "mounted",
                    "volume": volume,
                    "mountpoint": mountpoint,
                    "target": nfs_target,
                });
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            OutputFormat::Table => {
                println!("✓ Volume '{}' mounted at {}", volume, mountpoint);
                println!("  target  : {}", nfs_target);
                println!("  options : {}", options);
            }
        }

        Ok(())
    }

    fn unmount(&self, mountpoint: &str, format: OutputFormat) -> Result<()> {
        let status = Command::new("umount")
            .arg(mountpoint)
            .status()
            .map_err(|e| CliError::InvalidArgument(format!("failed to invoke umount: {}", e)))?;

        if !status.success() {
            return Err(CliError::InvalidArgument(format!(
                "umount exited with {} (mountpoint={}); hint: run with sudo if the \
                 kernel rejected umount(2) on permission grounds",
                status, mountpoint,
            )));
        }

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "unmounted",
                    "mountpoint": mountpoint,
                });
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            OutputFormat::Table => {
                println!("✓ Unmounted {}", mountpoint);
            }
        }

        Ok(())
    }

    fn list(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "nfs_list_exports", vec![])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;

        let export_terms = term_to_list(&data)?;
        let exports: Result<Vec<NfsExportInfo>> = export_terms
            .into_iter()
            .map(NfsExportInfo::from_term)
            .collect();
        let exports = exports?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&exports)?);
            }
            OutputFormat::Table => {
                if exports.is_empty() {
                    println!("No active NFS exports");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "NODE".to_string(),
                        "VOLUME".to_string(),
                        "EXPORTED AT".to_string(),
                    ]);
                    for export in &exports {
                        tbl.add_row(vec![
                            export.node.clone(),
                            export.volume_name.clone(),
                            export.exported_at.clone(),
                        ]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: NfsCommand,
    }

    #[test]
    fn test_nfs_mount_parses_required_args() {
        let cli = TestCli::try_parse_from(["test", "mount", "myvol", "/mnt/foo"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = &cli {
            assert!(matches!(
                parsed.command,
                NfsCommand::Mount { options: None, .. }
            ));
        }
    }

    #[test]
    fn test_nfs_mount_accepts_extra_options() {
        let cli = TestCli::try_parse_from([
            "test",
            "mount",
            "myvol",
            "/mnt/foo",
            "--options",
            "ro,noatime",
        ]);
        assert!(cli.is_ok());
        if let Ok(parsed) = &cli {
            assert!(matches!(
                parsed.command,
                NfsCommand::Mount {
                    options: Some(_),
                    ..
                }
            ));
        }
    }

    #[test]
    fn test_nfs_mount_requires_volume_and_mountpoint() {
        let cli = TestCli::try_parse_from(["test", "mount"]);
        assert!(cli.is_err());

        let cli = TestCli::try_parse_from(["test", "mount", "myvol"]);
        assert!(cli.is_err());
    }

    #[test]
    fn test_nfs_unmount_parses() {
        let cli = TestCli::try_parse_from(["test", "unmount", "/mnt/foo"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = &cli {
            assert!(matches!(parsed.command, NfsCommand::Unmount { .. }));
        }
    }
}
