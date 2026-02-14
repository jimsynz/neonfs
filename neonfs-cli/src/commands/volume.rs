//! Volume management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::{RotationStatus, VolumeInfo};
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Atom, Binary, FixInteger, Map, Term};

/// Volume management subcommands
#[derive(Debug, Subcommand)]
pub enum VolumeCommand {
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

        /// Encryption mode (none or server-side)
        #[arg(long, default_value = "none")]
        encryption: String,
    },

    /// Delete a volume
    Delete {
        /// Volume name
        name: String,

        /// Skip confirmation
        #[arg(long)]
        force: bool,
    },

    /// List all volumes
    List,

    /// Start key rotation for an encrypted volume
    RotateKey {
        /// Volume name
        name: String,
    },

    /// Show key rotation progress for a volume
    RotationStatus {
        /// Volume name
        name: String,
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
            VolumeCommand::Create {
                name,
                replicas,
                compression,
                encryption,
            } => self.create(name, *replicas, compression, encryption, format),
            VolumeCommand::Delete { name, force } => self.delete(name, *force, format),
            VolumeCommand::List => self.list(format),
            VolumeCommand::RotateKey { name } => self.rotate_key(name, format),
            VolumeCommand::RotationStatus { name } => self.rotation_status(name, format),
            VolumeCommand::Show { name } => self.show(name, format),
        }
    }

    fn list(&self, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "list_volumes", vec![])
                .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        let data = unwrap_ok_tuple(result)?;
        let volume_terms = term_to_list(&data)?;
        let volumes: Result<Vec<VolumeInfo>> = volume_terms
            .into_iter()
            .map(VolumeInfo::from_term)
            .collect();
        let volumes = volumes?;

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
                    "ENCRYPTION".to_string(),
                ]);
                for vol in &volumes {
                    tbl.add_row(vec![
                        vol.name.clone(),
                        VolumeInfo::format_size(vol.logical_size),
                        vol.chunk_count.to_string(),
                        vol.durability_string(),
                        vol.encryption_mode.clone(),
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
        encryption: &str,
        format: OutputFormat,
    ) -> Result<()> {
        // Validate encryption mode
        let encryption_mode = match encryption {
            "none" => "none",
            "server-side" => "server_side",
            other => {
                return Err(crate::error::CliError::InvalidArgument(format!(
                    "Invalid encryption mode '{}'. Valid: none, server-side",
                    other
                )));
            }
        };

        let runtime = tokio::runtime::Runtime::new()?;

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let durability_map = Term::Map(Map {
            entries: vec![
                (
                    Term::Atom(Atom::from("type")),
                    Term::Atom(Atom::from("replicate")),
                ),
                (
                    Term::Atom(Atom::from("factor")),
                    Term::FixInteger(FixInteger::from(replicas as i32)),
                ),
                (
                    Term::Atom(Atom::from("min_copies")),
                    Term::FixInteger(FixInteger::from(std::cmp::min(replicas, 2) as i32)),
                ),
            ],
        });

        let compression_map = Term::Map(Map {
            entries: vec![
                (
                    Term::Atom(Atom::from("algorithm")),
                    Term::Atom(Atom::from(compression)),
                ),
                (
                    Term::Atom(Atom::from("level")),
                    Term::FixInteger(FixInteger::from(3)),
                ),
                (
                    Term::Atom(Atom::from("min_size")),
                    Term::FixInteger(FixInteger::from(4096)),
                ),
            ],
        });

        let encryption_map = Term::Map(Map {
            entries: vec![(
                Term::Atom(Atom::from("mode")),
                Term::Atom(Atom::from(encryption_mode)),
            )],
        });

        let config_term = Term::Map(Map {
            entries: vec![
                (Term::Atom(Atom::from("durability")), durability_map),
                (Term::Atom(Atom::from("compression")), compression_map),
                (Term::Atom(Atom::from("encryption")), encryption_map),
            ],
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "create_volume",
                vec![name_term, config_term],
            )
            .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        let data = unwrap_ok_tuple(result)?;
        let volume = VolumeInfo::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&volume)?);
            }
            OutputFormat::Table => {
                println!("Volume '{}' created successfully", volume.name);
                println!("  ID: {}", volume.id);
                println!("  Durability: {}", volume.durability_string());
                println!("  Encryption: {}", volume.encryption_mode);
            }
        }
        Ok(())
    }

    fn delete(&self, name: &str, _force: bool, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "delete_volume",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

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
                println!("Volume '{}' deleted successfully", name);
            }
        }
        Ok(())
    }

    fn show(&self, name: &str, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "get_volume", vec![name_term])
                .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        let data = unwrap_ok_tuple(result)?;
        let volume = VolumeInfo::from_term(data)?;

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
                tbl.add_row(vec![
                    "Encryption".to_string(),
                    volume.encryption_mode.clone(),
                ]);
                if volume.encryption_key_version > 0 {
                    tbl.add_row(vec![
                        "Key Version".to_string(),
                        volume.encryption_key_version.to_string(),
                    ]);
                }
                if let Some(ref rotation) = volume.rotation {
                    tbl.add_row(vec![
                        "Rotation".to_string(),
                        format!(
                            "v{} -> v{} ({}/{})",
                            rotation.from_version,
                            rotation.to_version,
                            rotation.migrated,
                            rotation.total_chunks
                        ),
                    ]);
                }
                print!("{}", tbl.render()?);
            }
        }
        Ok(())
    }

    fn rotate_key(&self, name: &str, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "rotate_volume_key",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        let data = unwrap_ok_tuple(result)?;
        let status = RotationStatus::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&status)?);
            }
            OutputFormat::Table => {
                println!("Key rotation started for volume '{}'", name);
                println!(
                    "  Version: {} -> {}",
                    status.from_version, status.to_version
                );
                println!("  Chunks to re-encrypt: {}", status.total_chunks);
            }
        }
        Ok(())
    }

    fn rotation_status(&self, name: &str, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "rotation_status",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        let data = unwrap_ok_tuple(result)?;
        let status = RotationStatus::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&status)?);
            }
            OutputFormat::Table => {
                let percentage = if status.total_chunks > 0 {
                    (status.migrated as f64 / status.total_chunks as f64) * 100.0
                } else {
                    100.0
                };

                let mut tbl = table::Table::new(vec!["Property".to_string(), "Value".to_string()]);
                tbl.add_row(vec![
                    "From Version".to_string(),
                    status.from_version.to_string(),
                ]);
                tbl.add_row(vec![
                    "To Version".to_string(),
                    status.to_version.to_string(),
                ]);
                tbl.add_row(vec![
                    "Progress".to_string(),
                    format!(
                        "{}/{} ({:.1}%)",
                        status.migrated, status.total_chunks, percentage
                    ),
                ]);
                if !status.started_at.is_empty() {
                    tbl.add_row(vec!["Started At".to_string(), status.started_at.clone()]);
                }
                print!("{}", tbl.render()?);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volume_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: VolumeCommand,
        }

        let cli = TestCli::try_parse_from(["test", "list"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "create", "myvol"]);
        assert!(cli.is_ok());

        let cli =
            TestCli::try_parse_from(["test", "create", "myvol", "--encryption", "server-side"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "create", "myvol", "--encryption", "none"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "show", "myvol"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "delete", "myvol", "--force"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "rotate-key", "myvol"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "rotation-status", "myvol"]);
        assert!(cli.is_ok());
    }
}
