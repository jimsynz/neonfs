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
}

impl NfsCommand {
    /// Execute the NFS command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            NfsCommand::Export { volume } => self.export(volume, format),
            NfsCommand::Unexport { volume } => self.unexport(volume, format),
            NfsCommand::List => self.list(format),
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
