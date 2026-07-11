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
use std::collections::BTreeMap;

/// DR snapshot subcommands
#[derive(Debug, Subcommand)]
pub enum DrCommand {
    /// Snapshot management
    Snapshot {
        #[command(subcommand)]
        command: DrSnapshotCommand,
    },

    /// Full-cluster restore (#1005): stage + apply an exported DR
    /// snapshot, then restore each volume's content from its backup
    /// archive. Run this on a freshly-bootstrapped single node
    /// (`neonfs cluster init`); reattach the remaining nodes with
    /// `neonfs cluster join` afterwards.
    Restore {
        /// Source directory produced by `dr snapshot export`, holding the
        /// snapshot plus (by default) `volumes/<name>.backup` archives.
        #[arg(long)]
        source: String,

        /// Optional JSON catalogue `{"<volume>": "<archive-path>"}`
        /// pinning where each volume's backup archive lives. Relative
        /// paths resolve against `--source`; the catalogue is
        /// authoritative, so omitted volumes are left as empty shells.
        #[arg(long)]
        catalogue: Option<String>,

        /// Passphrase for encrypted backup archives (#1004).
        #[arg(long)]
        passphrase: Option<String>,
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

    /// Apply a snapshot's cluster-wide metadata back into live Ra
    /// state (#1005). Overlays the eight cluster-wide keyspaces
    /// (volumes, services, encryption keys, ACLs, segment
    /// assignments, credentials, escalations, KV); per-volume content
    /// is restored separately via `backup restore`.
    Apply {
        /// Snapshot ID (the timestamp directory under `/dr`)
        id: String,
    },

    /// Export a snapshot off-cluster to a directory on the daemon's
    /// filesystem (#1367) so it survives a bare-metal disaster — the
    /// in-cluster copy is destroyed with `_system`.
    Export {
        /// Snapshot ID (the timestamp directory under `/dr`)
        id: String,

        /// Destination directory on the daemon's filesystem
        #[arg(long)]
        out: String,
    },

    /// Stage an exported snapshot back into a freshly-bootstrapped
    /// cluster's `_system` volume (#1367), ready for `apply`.
    Import {
        /// Source directory produced by `dr snapshot export`
        source: String,
    },
}

#[derive(Debug, Serialize)]
struct DrRestoreResult {
    id: String,
    total: u64,
    generation: u64,
    restored: BTreeMap<String, u64>,
}

impl DrRestoreResult {
    fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        let id = term_to_string(
            map.get("id")
                .ok_or_else(|| CliError::TermConversionError("Missing 'id' field".to_string()))?,
        )?;

        let total = map
            .get("total")
            .and_then(|t| term_to_u64(t).ok())
            .unwrap_or(0);
        let generation = map
            .get("generation")
            .and_then(|t| term_to_u64(t).ok())
            .unwrap_or(0);

        let restored = map
            .get("restored")
            .and_then(|t| term_to_map(t).ok())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| term_to_u64(v).ok().map(|n| (k.clone(), n)))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            id,
            total,
            generation,
            restored,
        })
    }
}

#[derive(Debug, Serialize)]
struct DrVolumeRestore {
    name: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    archive: Option<String>,
    files: u64,
    bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl DrVolumeRestore {
    fn from_term(term: &Term) -> Result<Self> {
        let map = term_to_map(term)?;
        Ok(Self {
            name: map
                .get("name")
                .map(term_to_string)
                .transpose()?
                .unwrap_or_default(),
            status: map
                .get("status")
                .map(term_to_string)
                .transpose()?
                .unwrap_or_default(),
            archive: map.get("archive").map(term_to_string).transpose()?,
            files: map
                .get("files")
                .and_then(|t| term_to_u64(t).ok())
                .unwrap_or(0),
            bytes: map
                .get("bytes")
                .and_then(|t| term_to_u64(t).ok())
                .unwrap_or(0),
            reason: map.get("reason").map(term_to_string).transpose()?,
        })
    }
}

#[derive(Debug, Serialize)]
struct DrFullRestoreResult {
    snapshot_id: String,
    generation: u64,
    restored: BTreeMap<String, u64>,
    volumes: Vec<DrVolumeRestore>,
    volumes_restored: u64,
    volumes_failed: u64,
}

impl DrFullRestoreResult {
    fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        let snapshot_id = term_to_string(map.get("snapshot_id").ok_or_else(|| {
            CliError::TermConversionError("Missing 'snapshot_id' field".to_string())
        })?)?;

        let generation = map
            .get("generation")
            .and_then(|t| term_to_u64(t).ok())
            .unwrap_or(0);

        let restored = map
            .get("restored")
            .and_then(|t| term_to_map(t).ok())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| term_to_u64(v).ok().map(|n| (k.clone(), n)))
                    .collect()
            })
            .unwrap_or_default();

        let volumes = map
            .get("volumes")
            .map(term_to_list)
            .transpose()?
            .unwrap_or_default()
            .iter()
            .map(DrVolumeRestore::from_term)
            .collect::<Result<Vec<_>>>()?;

        let volumes_restored = map
            .get("volumes_restored")
            .and_then(|t| term_to_u64(t).ok())
            .unwrap_or(0);
        let volumes_failed = map
            .get("volumes_failed")
            .and_then(|t| term_to_u64(t).ok())
            .unwrap_or(0);

        Ok(Self {
            snapshot_id,
            generation,
            restored,
            volumes,
            volumes_restored,
            volumes_failed,
        })
    }
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

        let id = term_to_string(
            map.get("id")
                .ok_or_else(|| CliError::TermConversionError("Missing 'id' field".to_string()))?,
        )?;

        let path =
            term_to_string(map.get("path").ok_or_else(|| {
                CliError::TermConversionError("Missing 'path' field".to_string())
            })?)?;

        let created_at = term_to_string(map.get("created_at").ok_or_else(|| {
            CliError::TermConversionError("Missing 'created_at' field".to_string())
        })?)?;

        let state_version = map.get("state_version").and_then(|t| term_to_u64(t).ok());
        let file_count = map
            .get("file_count")
            .and_then(|t| term_to_u64(t).ok())
            .unwrap_or(0);
        let total_bytes = map
            .get("total_bytes")
            .and_then(|t| term_to_u64(t).ok())
            .unwrap_or(0);

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
            DrCommand::Restore {
                source,
                catalogue,
                passphrase,
            } => Self::restore(source, catalogue.as_deref(), passphrase.as_deref(), format),
        }
    }

    fn restore(
        source: &str,
        catalogue: Option<&str>,
        passphrase: Option<&str>,
        format: OutputFormat,
    ) -> Result<()> {
        let mut opts = vec![];
        if let Some(catalogue) = catalogue {
            opts.push((binary("catalogue"), binary(catalogue)));
        }
        if let Some(passphrase) = passphrase {
            opts.push((binary("passphrase"), binary(passphrase)));
        }
        let opts_term = Term::Map(Map {
            map: opts.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_dr_restore",
                vec![binary(source), opts_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let restore = DrFullRestoreResult::from_term(unwrap_ok_tuple(result)?)?;

        match format {
            OutputFormat::Json => println!("{}", json::format(&restore)?),
            OutputFormat::Table => {
                println!("Restored cluster from snapshot {}", restore.snapshot_id);
                println!("  Cluster generation: {}", restore.generation);
                println!(
                    "  Volumes restored:   {} ({} failed)",
                    restore.volumes_restored, restore.volumes_failed
                );
                for volume in &restore.volumes {
                    match volume.status.as_str() {
                        "restored" => println!(
                            "    ✓ {:<24} {} file(s), {} byte(s)",
                            volume.name, volume.files, volume.bytes
                        ),
                        "skipped" => println!(
                            "    - {:<24} skipped ({})",
                            volume.name,
                            volume.reason.as_deref().unwrap_or("no archive")
                        ),
                        _ => println!(
                            "    ✗ {:<24} failed ({})",
                            volume.name,
                            volume.reason.as_deref().unwrap_or("unknown")
                        ),
                    }
                }
            }
        }

        Ok(())
    }
}

impl DrSnapshotCommand {
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            DrSnapshotCommand::Create { label } => Self::create(label.as_deref(), format),
            DrSnapshotCommand::List => Self::list(format),
            DrSnapshotCommand::Show { id } => Self::show(id, format),
            DrSnapshotCommand::Apply { id } => Self::apply(id, format),
            DrSnapshotCommand::Export { id, out } => Self::export(id, out, format),
            DrSnapshotCommand::Import { source } => Self::import(source, format),
        }
    }

    fn export(id: &str, out: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_dr_snapshot_export",
                vec![binary(id), binary(out)],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let map = term_to_map(&unwrap_ok_tuple(result)?)?;
        let dest = map
            .get("dest")
            .map(term_to_string)
            .transpose()?
            .unwrap_or_default();
        let files = map
            .get("file_count")
            .map(term_to_u64)
            .transpose()?
            .unwrap_or(0);

        match format {
            OutputFormat::Json => {
                println!(
                    "{{\"id\":\"{}\",\"dest\":\"{}\",\"file_count\":{}}}",
                    id, dest, files
                )
            }
            OutputFormat::Table => {
                println!("Exported snapshot {}", id);
                println!("  Destination: {}", dest);
                println!("  Files:       {}", files);
            }
        }

        Ok(())
    }

    fn import(source: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_dr_snapshot_import",
                vec![binary(source)],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let map = term_to_map(&unwrap_ok_tuple(result)?)?;
        let id = map
            .get("id")
            .map(term_to_string)
            .transpose()?
            .unwrap_or_default();
        let files = map
            .get("file_count")
            .map(term_to_u64)
            .transpose()?
            .unwrap_or(0);

        match format {
            OutputFormat::Json => {
                println!("{{\"id\":\"{}\",\"file_count\":{}}}", id, files)
            }
            OutputFormat::Table => {
                println!("Imported snapshot {}", id);
                println!("  Files: {}", files);
                println!();
                println!("Apply it with: neonfs dr snapshot apply {}", id);
            }
        }

        Ok(())
    }

    fn apply(id: &str, format: OutputFormat) -> Result<()> {
        let id_term = Term::Binary(Binary {
            bytes: id.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_dr_snapshot_apply",
                vec![id_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let restore = DrRestoreResult::from_term(unwrap_ok_tuple(result)?)?;

        match format {
            OutputFormat::Json => println!("{}", json::format(&restore)?),
            OutputFormat::Table => {
                println!("Applied snapshot {}", restore.id);
                println!("  Entries restored: {}", restore.total);
                println!("  Cluster generation: {}", restore.generation);
                for (keyspace, count) in &restore.restored {
                    println!("    {:<20} {}", keyspace, count);
                }
            }
        }

        Ok(())
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

fn binary(s: &str) -> Term {
    Term::Binary(Binary {
        bytes: s.as_bytes().to_vec(),
    })
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

    #[test]
    fn parses_snapshot_apply() {
        let cli = TestCli::try_parse_from(["test", "snapshot", "apply", "20260425T120000Z"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn apply_requires_id() {
        let cli = TestCli::try_parse_from(["test", "snapshot", "apply"]);
        assert!(cli.is_err());
    }

    #[test]
    fn parses_snapshot_export() {
        let cli = TestCli::try_parse_from([
            "test",
            "snapshot",
            "export",
            "20260425T120000Z",
            "--out",
            "/tmp/dr",
        ]);
        assert!(cli.is_ok());
    }

    #[test]
    fn export_requires_out() {
        let cli = TestCli::try_parse_from(["test", "snapshot", "export", "20260425T120000Z"]);
        assert!(cli.is_err());
    }

    #[test]
    fn parses_snapshot_import() {
        let cli = TestCli::try_parse_from(["test", "snapshot", "import", "/tmp/dr"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn parses_restore_with_source_only() {
        let cli = TestCli::try_parse_from(["test", "restore", "--source", "/tmp/dr"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn parses_restore_with_catalogue_and_passphrase() {
        let cli = TestCli::try_parse_from([
            "test",
            "restore",
            "--source",
            "/tmp/dr",
            "--catalogue",
            "/tmp/dr/catalogue.json",
            "--passphrase",
            "hunter2",
        ]);
        assert!(cli.is_ok());
    }

    #[test]
    fn restore_requires_source() {
        let cli = TestCli::try_parse_from(["test", "restore"]);
        assert!(cli.is_err());
    }
}
