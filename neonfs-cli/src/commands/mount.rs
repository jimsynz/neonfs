//! Mount management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::MountInfo;
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, Term};

/// Mount management subcommands
#[derive(Debug, Subcommand)]
pub enum MountCommand {
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

impl MountCommand {
    /// Execute the mount command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            MountCommand::Mount { volume, mountpoint } => self.mount(volume, mountpoint, format),
            MountCommand::Unmount { mountpoint } => self.unmount(mountpoint, format),
            MountCommand::List => self.list(format),
        }
    }

    fn mount(&self, volume: &str, mountpoint: &str, format: OutputFormat) -> Result<()> {
        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });
        let mountpoint_term = Term::Binary(Binary {
            bytes: mountpoint.as_bytes().to_vec(),
        });

        // Connect to daemon and mount volume
        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "mount",
                vec![volume_term, mountpoint_term],
            )
            .await
        })?;

        // Check for error response
        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        // Unwrap {:ok, mount_info} tuple
        let data = unwrap_ok_tuple(result)?;
        let mount = MountInfo::from_term(data)?;

        // Format output
        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&mount)?);
            }
            OutputFormat::Table => {
                println!(
                    "✓ Volume '{}' mounted at '{}'",
                    mount.volume_name, mount.mount_point
                );
                println!("  Mount ID: {}", mount.mount_id);
            }
        }
        Ok(())
    }

    fn unmount(&self, mountpoint: &str, format: OutputFormat) -> Result<()> {
        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        let mountpoint_term = Term::Binary(Binary {
            bytes: mountpoint.as_bytes().to_vec(),
        });

        // Connect to daemon and unmount
        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "unmount",
                vec![mountpoint_term],
            )
            .await
        })?;

        // Check for error response
        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        // Format output
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
        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        // Connect to daemon and list mounts
        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "list_mounts", vec![])
                .await
        })?;

        // Check for error response
        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        // Unwrap {:ok, value} tuple
        let data = unwrap_ok_tuple(result)?;

        // Parse list of mounts
        let mount_terms = term_to_list(&data)?;
        let mounts: Result<Vec<MountInfo>> =
            mount_terms.into_iter().map(MountInfo::from_term).collect();
        let mounts = mounts?;

        // Format output
        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&mounts)?);
            }
            OutputFormat::Table => {
                if mounts.is_empty() {
                    println!("No active mounts");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "VOLUME".to_string(),
                        "MOUNTPOINT".to_string(),
                        "MOUNTED AT".to_string(),
                    ]);
                    for mount in mounts {
                        tbl.add_row(vec![mount.volume_name, mount.mount_point, mount.mounted_at]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }
}

// Tests for mount commands would require a running daemon (integration tests)
// Unit tests for output formatting are in the output module
