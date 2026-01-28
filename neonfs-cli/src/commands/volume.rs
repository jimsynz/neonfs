//! Volume management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::VolumeInfo;
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, Term};

/// Volume management subcommands
#[derive(Debug, Subcommand)]
pub enum VolumeCommand {
    /// List all volumes
    List,

    /// Create a new volume
    Create {
        /// Volume name
        name: String,

        /// Replication factor
        #[arg(long, default_value = "3")]
        replicas: u32,

        /// Compression algorithm
        #[arg(long, default_value = "zstd")]
        compression: String,
    },

    /// Delete a volume
    Delete {
        /// Volume name
        name: String,

        /// Skip confirmation
        #[arg(long)]
        force: bool,
    },

    /// Show volume details
    Show {
        /// Volume name
        name: String,
    },
}

impl VolumeCommand {
    /// Execute the volume command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            VolumeCommand::List => self.list(format),
            VolumeCommand::Create {
                name,
                replicas,
                compression,
            } => self.create(name, *replicas, compression, format),
            VolumeCommand::Delete { name, force } => self.delete(name, *force, format),
            VolumeCommand::Show { name } => self.show(name, format),
        }
    }

    fn list(&self, format: OutputFormat) -> Result<()> {
        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        // Connect to daemon and list volumes
        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "list_volumes", vec![])
                .await
        })?;

        // Check for error response
        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        // Unwrap {:ok, value} tuple
        let data = unwrap_ok_tuple(result)?;

        // Parse list of volumes
        let volume_terms = term_to_list(&data)?;
        let volumes: Result<Vec<VolumeInfo>> = volume_terms
            .into_iter()
            .map(VolumeInfo::from_term)
            .collect();
        let volumes = volumes?;

        // Format output
        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&volumes)?);
            }
            OutputFormat::Table => {
                let mut tbl = table::Table::new(vec![
                    "NAME".to_string(),
                    "SIZE".to_string(),
                    "CHUNKS".to_string(),
                    "DURABILITY".to_string(),
                ]);
                for vol in &volumes {
                    tbl.add_row(vec![
                        vol.name.clone(),
                        VolumeInfo::format_size(vol.logical_size),
                        vol.chunk_count.to_string(),
                        vol.durability_string(),
                    ]);
                }
                print!("{}", tbl.render()?);
            }
        }
        Ok(())
    }

    fn create(
        &self,
        name: &str,
        replicas: u32,
        compression: &str,
        format: OutputFormat,
    ) -> Result<()> {
        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        // Build volume configuration
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });
        let config_term = Term::Binary(Binary {
            bytes: serde_json::to_vec(&serde_json::json!({
                "durability": {
                    "type": "replicate",
                    "factor": replicas,
                    "min_copies": std::cmp::min(replicas, 2)
                },
                "compression": {
                    "algorithm": compression,
                    "level": 3,
                    "min_size": 4096
                }
            }))?,
        });

        // Connect to daemon and create volume
        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "create_volume",
                vec![name_term, config_term],
            )
            .await
        })?;

        // Check for error response
        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        // Unwrap {:ok, volume} tuple and parse
        let data = unwrap_ok_tuple(result)?;
        let volume = VolumeInfo::from_term(data)?;

        // Format output
        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&volume)?);
            }
            OutputFormat::Table => {
                println!("✓ Volume '{}' created successfully", volume.name);
                println!("  ID: {}", volume.id);
                println!("  Durability: {}", volume.durability_string());
            }
        }
        Ok(())
    }

    fn delete(&self, name: &str, _force: bool, format: OutputFormat) -> Result<()> {
        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        // Connect to daemon and delete volume
        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "delete_volume",
                vec![name_term],
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
                    "volume": name,
                    "message": "Volume deleted"
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("✓ Volume '{}' deleted successfully", name);
            }
        }
        Ok(())
    }

    fn show(&self, name: &str, format: OutputFormat) -> Result<()> {
        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        // Connect to daemon and get volume info
        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "get_volume", vec![name_term])
                .await
        })?;

        // Check for error response
        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        // Unwrap {:ok, volume} tuple
        let data = unwrap_ok_tuple(result)?;
        let volume = VolumeInfo::from_term(data)?;

        // Format output
        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&volume)?);
            }
            OutputFormat::Table => {
                let mut tbl = table::Table::new(vec!["Property".to_string(), "Value".to_string()]);
                tbl.add_row(vec!["Name".to_string(), volume.name.clone()]);
                tbl.add_row(vec!["ID".to_string(), volume.id.clone()]);
                tbl.add_row(vec![
                    "Logical Size".to_string(),
                    VolumeInfo::format_size(volume.logical_size),
                ]);
                tbl.add_row(vec![
                    "Physical Size".to_string(),
                    VolumeInfo::format_size(volume.physical_size),
                ]);
                tbl.add_row(vec!["Chunks".to_string(), volume.chunk_count.to_string()]);
                tbl.add_row(vec!["Durability".to_string(), volume.durability_string()]);
                print!("{}", tbl.render()?);
            }
        }
        Ok(())
    }
}

// Tests for volume commands would require a running daemon (integration tests)
// Unit tests for output formatting are in the output module
