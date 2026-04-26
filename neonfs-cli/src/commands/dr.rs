//! DR snapshot commands
//!
//! Operator-facing surface for the cluster's disaster-recovery snapshot
//! pipeline. Each snapshot captures `MetadataStateMachine` indexes plus
//! cluster CA material at one Ra log point — see
//! `NeonFS.Core.DRSnapshot` for the on-disk format.
//!
//! The handlers on the daemon side live in
//! `NeonFS.CLI.Handler.handle_dr_snapshot_*`.

use crate::daemon::DaemonConnection;
use crate::error::{CliError, Result};
use crate::output::{json, table, OutputFormat};
use crate::term::{
    extract_error, term_to_list, term_to_map, term_to_string, term_to_u64, unwrap_ok_tuple,
};
use clap::Subcommand;
use eetf::{Binary, Map, Term};
use serde::Serialize;

/// DR snapshot subcommands
#[derive(Debug, Subcommand)]
pub enum DrCommand {
    /// Snapshot management
    Snapshot {
        #[command(subcommand)]
        command: DrSnapshotCommand,
    },
}

/// Snapshot subcommand variants
#[derive(Debug, Subcommand)]
pub enum DrSnapshotCommand {
    /// Create an immediate snapshot of the cluster's metadata + CA state
    Create {
        /// Optional human-readable label (recorded in the audit log only)
        #[arg(long)]
        label: Option<String>,
    },

    /// List every snapshot in the `_system` volume's `/dr` directory
    List,

    /// Show a single snapshot's manifest
    Show {
        /// Snapshot ID (the timestamp directory under `/dr`)
        id: String,
    },
}

#[derive(Debug, Serialize)]
struct DrSnapshotEntry {
    id: String,
    path: String,
    created_at: String,
    state_version: Option<u64>,
    file_count: u64,
    total_bytes: u64,
}

impl DrSnapshotEntry {
    fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        let id = term_to_string(map.get("id").ok_or_else(|| {
            CliError::TermConversionError("Missing 'id' field".to_string())
        })?)?;

        let path = term_to_string(map.get("path").ok_or_else(|| {
            CliError::TermConversionError("Missing 'path' field".to_string())
        })?)?;

        let created_at = term_to_string(map.get("created_at").ok_or_else(|| {
            CliError::TermConversionError("Missing 'created_at' field".to_string())
        })?)?;

        let state_version = map.get("state_version").and_then(|t| term_to_u64(t).ok());
        let file_count = map.get("file_count").and_then(|t| term_to_u64(t).ok()).unwrap_or(0);
        let total_bytes = map.get("total_bytes").and_then(|t| term_to_u64(t).ok()).unwrap_or(0);

        Ok(Self {
            id,
            path,
            created_at,
            state_version,
            file_count,
            total_bytes,
        })
    }

    fn id_short(&self) -> String {
        if self.id.len() > 17 {
            format!("{}…", &self.id[..16])
        } else {
            self.id.clone()
        }
    }
}

impl DrCommand {
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            DrCommand::Snapshot { command } => command.execute(format),
        }
    }
}

impl DrSnapshotCommand {
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            DrSnapshotCommand::Create { label } => Self::create(label.as_deref(), format),
            DrSnapshotCommand::List => Self::list(format),
            DrSnapshotCommand::Show { id } => Self::show(id, format),
        }
    }

    fn create(label: Option<&str>, format: OutputFormat) -> Result<()> {
        let mut opts = vec![];

        if let Some(label) = label {
            opts.push((
                Term::Binary(Binary {
                    bytes: b"label".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: label.as_bytes().to_vec(),
                }),
            ));
        }

        let opts_term = Term::Map(Map {
            map: opts.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_dr_snapshot_create",
                vec![opts_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let entry = DrSnapshotEntry::from_term(unwrap_ok_tuple(result)?)?;

        match format {
            OutputFormat::Json => println!("{}", json::format(&entry)?),
            OutputFormat::Table => {
                println!("Created snapshot {}", entry.id);
                println!("  Path:          {}", entry.path);
                println!("  Created:       {}", entry.created_at);
                println!("  State version: {:?}", entry.state_version);
                println!("  Files:         {}", entry.file_count);
                println!("  Bytes:         {}", entry.total_bytes);
            }
        }

        Ok(())
    }

    fn list(format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_dr_snapshot_list",
                vec![],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let entry_terms = term_to_list(&unwrap_ok_tuple(result)?)?;
        let entries: Result<Vec<DrSnapshotEntry>> = entry_terms
            .into_iter()
            .map(DrSnapshotEntry::from_term)
            .collect();
        let entries = entries?;

        match format {
            OutputFormat::Json => println!("{}", json::format(&entries)?),
            OutputFormat::Table => {
                if entries.is_empty() {
                    println!("No DR snapshots found.");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "ID".to_string(),
                        "CREATED".to_string(),
                        "STATE_VERSION".to_string(),
                        "FILES".to_string(),
                        "BYTES".to_string(),
                    ]);
                    for entry in &entries {
                        tbl.add_row(vec![
                            entry.id_short(),
                            entry.created_at.clone(),
                            entry
                                .state_version
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "-".to_string()),
                            entry.file_count.to_string(),
                            entry.total_bytes.to_string(),
                        ]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }

        Ok(())
    }

    fn show(id: &str, format: OutputFormat) -> Result<()> {
        let id_term = Term::Binary(Binary {
            bytes: id.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_dr_snapshot_show",
                vec![id_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let entry = DrSnapshotEntry::from_term(unwrap_ok_tuple(result)?)?;

        match format {
            OutputFormat::Json => println!("{}", json::format(&entry)?),
            OutputFormat::Table => {
                println!("Snapshot {}", entry.id);
                println!("  Path:          {}", entry.path);
                println!("  Created:       {}", entry.created_at);
                println!("  State version: {:?}", entry.state_version);
                println!("  Files:         {}", entry.file_count);
                println!("  Bytes:         {}", entry.total_bytes);
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
        command: DrCommand,
    }

    #[test]
    fn parses_snapshot_create_with_label() {
        let cli = TestCli::try_parse_from(["test", "snapshot", "create", "--label", "manual"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn parses_snapshot_create_without_label() {
        let cli = TestCli::try_parse_from(["test", "snapshot", "create"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn parses_snapshot_list() {
        let cli = TestCli::try_parse_from(["test", "snapshot", "list"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn parses_snapshot_show() {
        let cli = TestCli::try_parse_from(["test", "snapshot", "show", "20260425T120000Z"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn show_requires_id() {
        let cli = TestCli::try_parse_from(["test", "snapshot", "show"]);
        assert!(cli.is_err());
    }
}
