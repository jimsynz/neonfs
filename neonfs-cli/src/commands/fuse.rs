//! FUSE mount management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::MountInfo;
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, Map, Term};
use std::collections::HashMap;

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
