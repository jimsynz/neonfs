//! NFS export management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::NfsExportInfo;
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, Term};

/// NFS export management subcommands
#[derive(Debug, Subcommand)]
pub enum NfsCommand {
    /// Export a volume via NFS
    Mount {
        /// Volume name
        volume: String,
    },

    /// Unexport a volume from NFS
    Unmount {
        /// Export ID or volume name
        target: String,
    },

    /// List all NFS exports
    List,
}

impl NfsCommand {
    /// Execute the NFS command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            NfsCommand::Mount { volume } => self.mount(volume, format),
            NfsCommand::Unmount { target } => self.unmount(target, format),
            NfsCommand::List => self.list(format),
        }
    }

    fn mount(&self, volume: &str, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "nfs_mount", vec![volume_term])
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
                println!("  Export ID: {}", export.id);
            }
        }
        Ok(())
    }

    fn unmount(&self, target: &str, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let target_term = Term::Binary(Binary {
            bytes: target.as_bytes().to_vec(),
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "nfs_unmount",
                vec![target_term],
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
                    "target": target,
                    "message": "Volume unexported"
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("✓ Volume unexported from NFS: '{}'", target);
            }
        }
        Ok(())
    }

    fn list(&self, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "nfs_list_mounts", vec![])
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
                        "EXPORT ID".to_string(),
                        "VOLUME".to_string(),
                        "EXPORTED AT".to_string(),
                    ]);
                    for export in &exports {
                        tbl.add_row(vec![
                            export.node.clone(),
                            export.id.clone(),
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
