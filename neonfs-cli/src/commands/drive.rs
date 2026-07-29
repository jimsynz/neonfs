//! Drive management commands

use crate::daemon::DaemonConnection;
use crate::error::{CliError, Result};
use crate::output::{json, table, OutputFormat};
use crate::term::types::DriveInfo;
use crate::term::{
    extract_error, term_to_bool, term_to_list, term_to_map, term_to_string, term_to_u64,
    unwrap_ok_tuple,
};
use clap::Subcommand;
use eetf::{Atom, Binary, Map, Term};
use serde::Serialize;

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

    /// Evacuate all data from a drive (graceful removal).
    ///
    /// Always prefers a same-tier target drive and falls back to any tier
    /// when none is available — evacuation must succeed even if no
    /// same-tier drive remains in the cluster.
    Evacuate {
        /// Drive identifier
        drive_id: String,

        /// Node where the drive is located (default: local node)
        #[arg(long)]
        node: Option<String>,

        /// Block until the evacuation job finishes; exits non-zero on failure.
        #[arg(long)]
        wait: bool,

        /// Start even though this drive holds a volume's last copies and
        /// there is nowhere to relocate them. Cannot override the
        /// `_system` volume being left with none.
        #[arg(long)]
        force: bool,
    },

    /// Show replication health: under-replicated volumes and drives
    /// holding the sole copy of anything
    Replicas,
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
                wait,
                force,
            } => self.evacuate(drive_id, node.as_deref(), *wait, *force, format),
            DriveCommand::Replicas => self.replicas(format),
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

        let config_term = Term::Map(Map {
            map: entries.into_iter().collect(),
        });

        let result = smol::block_on(async {
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
        let drive_id_term = Term::Binary(Binary {
            bytes: drive_id.as_bytes().to_vec(),
        });

        let force_term = Term::Atom(Atom::from(if force { "true" } else { "false" }));

        let result = smol::block_on(async {
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
            map: filter_entries.into_iter().collect(),
        });

        let result = smol::block_on(async {
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
                            drive.node.clone(),
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
        wait: bool,
        force: bool,
        format: OutputFormat,
    ) -> Result<()> {
        let node_name = match node {
            Some(n) => n.to_string(),
            None => {
                let status = smol::block_on(async {
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
        let opts_term = Term::Map(Map {
            map: [(
                Term::Binary(Binary {
                    bytes: b"force".to_vec(),
                }),
                Term::Atom(Atom::from(if force { "true" } else { "false" })),
            )]
            .into_iter()
            .collect(),
        });

        let result = smol::block_on(async {
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

        if wait {
            return crate::commands::job::wait_and_report(&job_id, format);
        }

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
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("Evacuation started for drive '{}'", drive_id);
                println!("  Node: {}", node_name);
                println!("  Job ID: {}", job_id);
                println!("  Chunks to evacuate: {}", total);
                println!("\nTrack progress with: neonfs job show {}", job_id);
            }
        }
        Ok(())
    }

    fn replicas(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "handle_replica_status", vec![])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let report = ReplicaReport::from_term(unwrap_ok_tuple(result)?)?;

        match format {
            OutputFormat::Json => println!("{}", json::format(&report)?),
            OutputFormat::Table => report.print_table()?,
        }

        Ok(())
    }
}

/// Per-volume replication health as reported by `handle_replica_status`.
#[derive(Debug, Serialize)]
struct VolumeReplication {
    volume_name: String,
    system: bool,
    min_copies: u64,
    chunk_count: u64,
    below_min_copies: u64,
    zero_copies: u64,
    least_copies: u64,
}

impl VolumeReplication {
    fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            volume_name: term_to_string(map.get("volume_name").ok_or_else(|| {
                CliError::TermConversionError("Missing 'volume_name' field".to_string())
            })?)?,
            system: map
                .get("system")
                .and_then(|t| term_to_bool(t).ok())
                .unwrap_or(false),
            min_copies: field_u64(&map, "min_copies"),
            chunk_count: field_u64(&map, "chunk_count"),
            below_min_copies: field_u64(&map, "below_min_copies"),
            zero_copies: field_u64(&map, "zero_copies"),
            least_copies: field_u64(&map, "least_copies"),
        })
    }
}

/// A drive that is the only holder of at least one chunk.
#[derive(Debug, Serialize)]
struct SoleCopyDrive {
    node: String,
    drive_id: String,
    chunk_count: u64,
}

impl SoleCopyDrive {
    fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            node: term_to_string(map.get("node").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node' field".to_string())
            })?)?,
            drive_id: term_to_string(map.get("drive_id").ok_or_else(|| {
                CliError::TermConversionError("Missing 'drive_id' field".to_string())
            })?)?,
            chunk_count: field_u64(&map, "chunk_count"),
        })
    }
}

#[derive(Debug, Serialize)]
struct ReplicaReport {
    volumes: Vec<VolumeReplication>,
    sole_copy_drives: Vec<SoleCopyDrive>,
}

impl ReplicaReport {
    fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        let volumes = match map.get("volumes") {
            Some(term) => term_to_list(term)?
                .into_iter()
                .map(VolumeReplication::from_term)
                .collect::<Result<Vec<_>>>()?,
            None => vec![],
        };

        let sole_copy_drives = match map.get("sole_copy_drives") {
            Some(term) => term_to_list(term)?
                .into_iter()
                .map(SoleCopyDrive::from_term)
                .collect::<Result<Vec<_>>>()?,
            None => vec![],
        };

        Ok(Self {
            volumes,
            sole_copy_drives,
        })
    }

    fn print_table(&self) -> Result<()> {
        let mut tbl = table::Table::new(vec![
            "VOLUME".to_string(),
            "MIN COPIES".to_string(),
            "CHUNKS".to_string(),
            "BELOW MIN".to_string(),
            "ZERO COPIES".to_string(),
            "FEWEST".to_string(),
        ]);

        for volume in &self.volumes {
            tbl.add_row(vec![
                if volume.system {
                    format!("{} (system)", volume.volume_name)
                } else {
                    volume.volume_name.clone()
                },
                volume.min_copies.to_string(),
                volume.chunk_count.to_string(),
                volume.below_min_copies.to_string(),
                volume.zero_copies.to_string(),
                volume.least_copies.to_string(),
            ]);
        }

        print!("{}", tbl.render()?);

        let under: Vec<&VolumeReplication> = self
            .volumes
            .iter()
            .filter(|v| v.below_min_copies > 0)
            .collect();

        if under.is_empty() {
            println!("\nAll volumes meet their min_copies floor.");
        } else {
            println!("\n{} volume(s) under-replicated:", under.len());
            for volume in under {
                println!(
                    "  {} — {} chunk(s) below min_copies {} (fewest {})",
                    volume.volume_name,
                    volume.below_min_copies,
                    volume.min_copies,
                    volume.least_copies
                );
            }
        }

        if self.sole_copy_drives.is_empty() {
            println!("No drive holds the sole copy of any chunk.");
        } else {
            println!("\nDrives holding sole copies (loss here loses data):");
            for drive in &self.sole_copy_drives {
                println!(
                    "  {} on {} — {} chunk(s)",
                    drive.drive_id, drive.node, drive.chunk_count
                );
            }
        }

        Ok(())
    }
}

fn field_u64(map: &std::collections::HashMap<String, Term>, key: &str) -> u64 {
    map.get(key).and_then(|t| term_to_u64(t).ok()).unwrap_or(0)
}

fn extract_integer(term: &Term) -> Option<i64> {
    match term {
        Term::FixInteger(n) => Some(n.value as i64),
        Term::BigInteger(big) => {
            use num_traits::ToPrimitive;
            big.to_i64()
        }
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
                    wait,
                    force,
                } => {
                    assert_eq!(drive_id, "nvme0");
                    assert!(node.is_none());
                    assert!(!wait);
                    assert!(!force);
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
            "--wait",
            "--force",
        ]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                DriveCommand::Evacuate {
                    drive_id,
                    node,
                    wait,
                    force,
                } => {
                    assert_eq!(drive_id, "sata0");
                    assert_eq!(node.as_deref(), Some("neonfs-core@host1"));
                    assert!(wait);
                    assert!(force);
                }
                _ => panic!("Expected Evacuate variant"),
            }
        }
    }

    #[test]
    fn test_replicas_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: DriveCommand,
        }

        let cli = TestCli::try_parse_from(["test", "replicas"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            assert!(matches!(parsed.command, DriveCommand::Replicas));
        }
    }

    #[test]
    fn test_replica_report_from_term() {
        use eetf::FixInteger;

        fn binary(value: &str) -> Term {
            Term::Binary(Binary {
                bytes: value.as_bytes().to_vec(),
            })
        }

        fn integer(value: i32) -> Term {
            Term::FixInteger(FixInteger { value })
        }

        let volume = Term::Map(Map {
            map: [
                (binary("volume_name"), binary("_system")),
                (binary("system"), Term::Atom(Atom::from("true"))),
                (binary("min_copies"), integer(2)),
                (binary("chunk_count"), integer(9)),
                (binary("below_min_copies"), integer(4)),
                (binary("zero_copies"), integer(0)),
                (binary("least_copies"), integer(1)),
            ]
            .into_iter()
            .collect(),
        });

        let sole_copy = Term::Map(Map {
            map: [
                (binary("node"), binary("neonfs_core@host1")),
                (binary("drive_id"), binary("nvme0")),
                (binary("chunk_count"), integer(4)),
            ]
            .into_iter()
            .collect(),
        });

        let report = Term::Map(Map {
            map: [
                (binary("volumes"), Term::List(vec![volume].into())),
                (
                    binary("sole_copy_drives"),
                    Term::List(vec![sole_copy].into()),
                ),
            ]
            .into_iter()
            .collect(),
        });

        let parsed = ReplicaReport::from_term(report).expect("report parses");

        assert_eq!(parsed.volumes.len(), 1);
        assert_eq!(parsed.volumes[0].volume_name, "_system");
        assert!(parsed.volumes[0].system);
        assert_eq!(parsed.volumes[0].below_min_copies, 4);
        assert_eq!(parsed.volumes[0].least_copies, 1);

        assert_eq!(parsed.sole_copy_drives.len(), 1);
        assert_eq!(parsed.sole_copy_drives[0].drive_id, "nvme0");
        assert_eq!(parsed.sole_copy_drives[0].chunk_count, 4);
    }

    #[test]
    fn test_replica_report_defaults_missing_sections() {
        let report = Term::Map(Map {
            map: std::collections::HashMap::new(),
        });

        let parsed = ReplicaReport::from_term(report).expect("empty report parses");

        assert!(parsed.volumes.is_empty());
        assert!(parsed.sole_copy_drives.is_empty());
    }
}
