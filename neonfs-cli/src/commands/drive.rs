//! Drive management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::DriveInfo;
use crate::term::{extract_error, term_to_list, term_to_map, term_to_string, unwrap_ok_tuple};
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

    /// List all drives across the cluster
    List {
        /// Filter to drives on a specific node
        #[arg(long)]
        node: Option<String>,
    },

    /// Evacuate all data from a drive (graceful removal)
    Evacuate {
        /// Drive identifier
        drive_id: String,

        /// Node where the drive is located (default: local node)
        #[arg(long)]
        node: Option<String>,

        /// Allow migration to any tier (default: same tier only)
        #[arg(long)]
        any_tier: bool,
    },
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
            DriveCommand::List { node } => self.list(node.as_deref(), format),
            DriveCommand::Evacuate {
                drive_id,
                node,
                any_tier,
            } => self.evacuate(drive_id, node.as_deref(), *any_tier, format),
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

        if let Some(err) = extract_error(&result) {
            return Err(err);
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

        if let Some(err) = extract_error(&result) {
            if err.error_message() == "drive_has_data" {
                eprintln!(
                    "Error: Drive '{}' contains data. Use `drive evacuate {}` first, or --force to remove anyway.",
                    drive_id, drive_id
                );
                return Err(crate::error::CliError::RpcError(
                    "Drive contains data".to_string(),
                ));
            }
            return Err(err);
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

    fn list(&self, node: Option<&str>, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let mut filter_entries = vec![];

        if let Some(n) = node {
            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"node".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: n.as_bytes().to_vec(),
                }),
            ));
        }

        let filters_term = Term::Map(Map {
            entries: filter_entries,
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_list_drives",
                vec![filters_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
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
                        "NODE".to_string(),
                        "ID".to_string(),
                        "PATH".to_string(),
                        "TIER".to_string(),
                        "CAPACITY".to_string(),
                        "USED".to_string(),
                        "STATE".to_string(),
                    ]);
                    for drive in &drives {
                        tbl.add_row(vec![
                            drive.node_short(),
                            drive.id.clone(),
                            drive.path.clone(),
                            drive.tier.clone(),
                            DriveInfo::format_capacity(drive.capacity_bytes),
                            DriveInfo::format_bytes(drive.used_bytes),
                            drive.state.clone(),
                        ]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }

    fn evacuate(
        &self,
        drive_id: &str,
        node: Option<&str>,
        any_tier: bool,
        format: OutputFormat,
    ) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        // Get the local node name from the daemon if no node specified
        let node_name = match node {
            Some(n) => n.to_string(),
            None => {
                let status = runtime.block_on(async {
                    let mut conn = DaemonConnection::connect().await?;
                    conn.call("Elixir.NeonFS.CLI.Handler", "cluster_status", vec![])
                        .await
                })?;
                let status_data = unwrap_ok_tuple(status)?;
                let status_map = term_to_map(&status_data)?;
                match status_map.get("node") {
                    Some(term) => term_to_string(term)?,
                    None => {
                        return Err(crate::error::CliError::RpcError(
                            "Could not determine local node name".to_string(),
                        ))
                    }
                }
            }
        };

        let node_term = Term::Binary(Binary {
            bytes: node_name.as_bytes().to_vec(),
        });
        let drive_id_term = Term::Binary(Binary {
            bytes: drive_id.as_bytes().to_vec(),
        });

        let mut opts_entries = vec![];
        if any_tier {
            opts_entries.push((
                Term::Binary(Binary {
                    bytes: b"any_tier".to_vec(),
                }),
                Term::Atom(Atom::from("true")),
            ));
        }
        let opts_term = Term::Map(Map {
            entries: opts_entries,
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_evacuate_drive",
                vec![node_term, drive_id_term, opts_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            match err.error_message().as_str() {
                "already_draining" => {
                    eprintln!("Error: Drive '{}' is already being evacuated.", drive_id);
                }
                "insufficient_capacity" => {
                    eprintln!(
                        "Error: Not enough capacity on other drives to evacuate '{}'.",
                        drive_id
                    );
                }
                _ => {}
            }
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let job_map = term_to_map(&data)?;

        let job_id = job_map
            .get("id")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();
        let total = job_map
            .get("progress_total")
            .and_then(extract_integer)
            .unwrap_or(0);

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "started",
                    "job_id": job_id,
                    "drive_id": drive_id,
                    "node": node_name,
                    "total_chunks": total,
                    "any_tier": any_tier
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("Evacuation started for drive '{}'", drive_id);
                println!("  Node: {}", node_name);
                println!("  Job ID: {}", job_id);
                println!("  Chunks to evacuate: {}", total);
                if any_tier {
                    println!("  Mode: any tier (cross-tier migration allowed)");
                } else {
                    println!("  Mode: same tier only");
                }
                println!("\nTrack progress with: neonfs job show {}", job_id);
            }
        }
        Ok(())
    }
}

fn extract_integer(term: &Term) -> Option<i64> {
    match term {
        Term::FixInteger(n) => Some(n.value as i64),
        _ => None,
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
        if let Ok(parsed) = cli {
            match parsed.command {
                DriveCommand::List { node } => assert!(node.is_none()),
                _ => panic!("Expected List variant"),
            }
        }

        let cli = TestCli::try_parse_from(["test", "list", "--node", "neonfs_core@host1"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                DriveCommand::List { node } => {
                    assert_eq!(node.as_deref(), Some("neonfs_core@host1"));
                }
                _ => panic!("Expected List variant"),
            }
        }

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

    #[test]
    fn test_evacuate_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: DriveCommand,
        }

        let cli = TestCli::try_parse_from(["test", "evacuate", "nvme0"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                DriveCommand::Evacuate {
                    drive_id,
                    node,
                    any_tier,
                } => {
                    assert_eq!(drive_id, "nvme0");
                    assert!(node.is_none());
                    assert!(!any_tier);
                }
                _ => panic!("Expected Evacuate variant"),
            }
        }

        let cli = TestCli::try_parse_from([
            "test",
            "evacuate",
            "sata0",
            "--node",
            "neonfs-core@host1",
            "--any-tier",
        ]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                DriveCommand::Evacuate {
                    drive_id,
                    node,
                    any_tier,
                } => {
                    assert_eq!(drive_id, "sata0");
                    assert_eq!(node.as_deref(), Some("neonfs-core@host1"));
                    assert!(any_tier);
                }
                _ => panic!("Expected Evacuate variant"),
            }
        }
    }
}
