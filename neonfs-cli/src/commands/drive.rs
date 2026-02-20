//! Drive management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::DriveInfo;
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Atom, Binary, Map, Term};

/// Drive management subcommands
#[derive(Debug, Subcommand)]
pub enum DriveCommand {
    /// Add a new drive to this node
    Add {
        /// Absolute path to the storage directory
        #[arg(long)]
        path: String,

        /// Storage tier: hot, warm, or cold
        #[arg(long, default_value = "hot")]
        tier: String,

        /// Capacity limit (e.g. "1T", "500G", "0" for unlimited)
        #[arg(long, default_value = "0")]
        capacity: String,

        /// Unique drive ID (auto-generated from path if not provided)
        #[arg(long)]
        id: Option<String>,
    },

    /// Remove a drive from this node
    Remove {
        /// Drive identifier
        drive_id: String,

        /// Force removal even if drive contains data
        #[arg(long)]
        force: bool,
    },

    /// List all drives on this node
    List,
}

impl DriveCommand {
    /// Execute the drive command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            DriveCommand::Add {
                path,
                tier,
                capacity,
                id,
            } => self.add(path, tier, capacity, id.as_deref(), format),
            DriveCommand::Remove { drive_id, force } => self.remove(drive_id, *force, format),
            DriveCommand::List => self.list(format),
        }
    }

    fn add(
        &self,
        path: &str,
        tier: &str,
        capacity: &str,
        id: Option<&str>,
        format: OutputFormat,
    ) -> Result<()> {
        // Validate tier
        if !["hot", "warm", "cold"].contains(&tier) {
            return Err(crate::error::CliError::InvalidArgument(format!(
                "Invalid tier '{}'. Valid: hot, warm, cold",
                tier
            )));
        }

        let runtime = tokio::runtime::Runtime::new()?;

        let mut entries = vec![
            (
                Term::Binary(Binary {
                    bytes: b"path".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: path.as_bytes().to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"tier".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: tier.as_bytes().to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"capacity".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: capacity.as_bytes().to_vec(),
                }),
            ),
        ];

        if let Some(drive_id) = id {
            entries.push((
                Term::Binary(Binary {
                    bytes: b"id".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: drive_id.as_bytes().to_vec(),
                }),
            ));
        }

        let config_term = Term::Map(Map { entries });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_add_drive",
                vec![config_term],
            )
            .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        let data = unwrap_ok_tuple(result)?;
        let drive = DriveInfo::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&drive)?);
            }
            OutputFormat::Table => {
                println!("Drive '{}' added successfully", drive.id);
                println!("  Path: {}", drive.path);
                println!("  Tier: {}", drive.tier);
                println!(
                    "  Capacity: {}",
                    DriveInfo::format_capacity(drive.capacity_bytes)
                );
            }
        }
        Ok(())
    }

    fn remove(&self, drive_id: &str, force: bool, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let drive_id_term = Term::Binary(Binary {
            bytes: drive_id.as_bytes().to_vec(),
        });

        let force_term = Term::Atom(Atom::from(if force { "true" } else { "false" }));

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_remove_drive",
                vec![drive_id_term, force_term],
            )
            .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            if err_msg == "drive_has_data" {
                eprintln!(
                    "Error: Drive '{}' contains data. Evacuate data first or use --force to remove anyway.",
                    drive_id
                );
                return Err(crate::error::CliError::RpcError(
                    "Drive contains data".to_string(),
                ));
            }
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "success",
                    "drive_id": drive_id,
                    "message": "Drive removed"
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("Drive '{}' removed successfully", drive_id);
            }
        }
        Ok(())
    }

    fn list(&self, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "handle_list_drives", vec![])
                .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        let data = unwrap_ok_tuple(result)?;
        let drive_terms = term_to_list(&data)?;
        let drives: Result<Vec<DriveInfo>> =
            drive_terms.into_iter().map(DriveInfo::from_term).collect();
        let drives = drives?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&drives)?);
            }
            OutputFormat::Table => {
                if drives.is_empty() {
                    println!("No drives configured");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "ID".to_string(),
                        "PATH".to_string(),
                        "TIER".to_string(),
                        "CAPACITY".to_string(),
                        "USED".to_string(),
                        "STATE".to_string(),
                    ]);
                    for drive in &drives {
                        tbl.add_row(vec![
                            drive.id.clone(),
                            drive.path.clone(),
                            drive.tier.clone(),
                            DriveInfo::format_capacity(drive.capacity_bytes),
                            DriveInfo::format_capacity(drive.used_bytes),
                            drive.state.clone(),
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

    #[test]
    fn test_drive_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: DriveCommand,
        }

        let cli = TestCli::try_parse_from(["test", "list"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "add", "--path", "/data/nvme0"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from([
            "test",
            "add",
            "--path",
            "/data/nvme0",
            "--tier",
            "hot",
            "--capacity",
            "1T",
            "--id",
            "nvme0",
        ]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "remove", "nvme0"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "remove", "nvme0", "--force"]);
        assert!(cli.is_ok());
    }
}
