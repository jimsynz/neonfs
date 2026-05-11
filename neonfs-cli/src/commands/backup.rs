//! `neonfs backup create | list | restore` — operator-facing
//! orchestration over the snapshot + export + import primitives
//! (#968).

use clap::Subcommand;
use eetf::{Atom, Map, Term};
use std::collections::HashMap;

use crate::daemon::DaemonConnection;
use crate::error::{CliError, Result};
use crate::output::OutputFormat;
use crate::term::{
    extract_error, term_to_list, term_to_map, term_to_string, term_to_u64, unwrap_ok_tuple,
};

/// `neonfs backup …` — orchestration over snapshot + export +
/// import.
///
/// `create` snapshots the volume, exports the snapshot's tree to
/// the destination, then drops the snapshot. `restore` is the
/// inverse: read an export back into a brand-new volume. `list`
/// peeks at the manifest of an existing export without unpacking
/// the body.
///
/// All paths are on the daemon's filesystem. Cross-cluster restore
/// and incremental backups remain tracked in #248.
#[derive(Debug, Subcommand)]
pub enum BackupCommand {
    /// Take a snapshot, export it to the destination, then drop
    /// the snapshot. On export failure the snapshot is *left* in
    /// place so the operator can retry without re-snapshotting.
    Create {
        /// Volume to back up
        #[arg(long)]
        volume: String,

        /// Destination tarball path on the daemon's filesystem
        #[arg(long)]
        to: String,

        /// Optional snapshot name (default: generated)
        #[arg(long)]
        name: Option<String>,
    },

    /// Inspect an existing backup's manifest without unpacking the
    /// body.
    List {
        /// Backup tarball path on the daemon's filesystem
        #[arg(long)]
        from: String,
    },

    /// Restore a backup into a new volume. Same wiring as
    /// `volume import` — exposed here because that's where
    /// operators look.
    Restore {
        /// Backup tarball path on the daemon's filesystem
        #[arg(long)]
        from: String,

        /// Name of the new volume to create
        #[arg(long = "as")]
        new_name: String,
    },
}

impl BackupCommand {
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            BackupCommand::Create { volume, to, name } => {
                self.create(volume, to, name.as_deref(), format)
            }
            BackupCommand::List { from } => self.list(from, format),
            BackupCommand::Restore { from, new_name } => self.restore(from, new_name, format),
        }
    }

    fn create(
        &self,
        volume: &str,
        to: &str,
        name: Option<&str>,
        format: OutputFormat,
    ) -> Result<()> {
        let opts_term = match name {
            None => Term::Map(Map {
                map: vec![].into_iter().collect(),
            }),
            Some(n) => Term::Map(Map {
                map: vec![(Term::Atom(Atom::from("name")), binary_val(n))]
                    .into_iter()
                    .collect(),
            }),
        };

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_backup_create",
                vec![binary_val(volume), binary_val(to), opts_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let path = required_string(&map, "path")?;
        let snapshot_id = required_string(&map, "snapshot_id")?;
        let file_count = required_u64(&map, "file_count")?;
        let byte_count = required_u64(&map, "byte_count")?;

        match format {
            OutputFormat::Json => {
                println!(
                    "{{\"volume\":\"{}\",\"path\":\"{}\",\"snapshot_id\":\"{}\",\"file_count\":{},\"byte_count\":{}}}",
                    volume, path, snapshot_id, file_count, byte_count
                );
            }
            OutputFormat::Table => {
                println!("✓ Backed up {} to {}", volume, path);
                println!();
                println!("  Snapshot:  {}", snapshot_id);
                println!("  Files:     {}", file_count);
                println!("  Bytes:     {}", byte_count);
            }
        }
        Ok(())
    }

    fn list(&self, from: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_backup_describe",
                vec![binary_val(from)],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        // The handler returns the manifest map verbatim. Reach in
        // for the human-readable summary fields.
        let map = term_to_map(&data)?;

        let schema = required_string(&map, "schema")?;
        let volume = map
            .get("volume")
            .and_then(|t| term_to_map(t).ok())
            .and_then(|m| m.get("name").and_then(|v| term_to_string(v).ok()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let file_count = required_u64(&map, "file_count").unwrap_or(0);
        let total_bytes = required_u64(&map, "total_bytes").unwrap_or(0);
        let snapshot_id = map
            .get("snapshot_id")
            .and_then(|v| term_to_string(v).ok());
        let exported_at = map
            .get("exported_at")
            .and_then(|v| term_to_string(v).ok())
            .unwrap_or_else(|| "<unknown>".to_string());

        match format {
            OutputFormat::Json => {
                let snap = snapshot_id
                    .as_deref()
                    .map(|s| format!("\"{}\"", s))
                    .unwrap_or_else(|| "null".to_string());
                println!(
                    "{{\"from\":\"{}\",\"schema\":\"{}\",\"volume\":\"{}\",\"exported_at\":\"{}\",\"file_count\":{},\"total_bytes\":{},\"snapshot_id\":{}}}",
                    from, schema, volume, exported_at, file_count, total_bytes, snap
                );
            }
            OutputFormat::Table => {
                println!("Backup at {}", from);
                println!();
                println!("  Schema:       {}", schema);
                println!("  Volume:       {}", volume);
                println!("  Exported at:  {}", exported_at);
                println!("  Files:        {}", file_count);
                println!("  Total bytes:  {}", total_bytes);
                if let Some(snap) = snapshot_id {
                    println!("  From snap:    {}", snap);
                }
            }
        }
        Ok(())
    }

    fn restore(&self, from: &str, new_name: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_backup_restore",
                vec![binary_val(from), binary_val(new_name)],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let volume_id = required_string(&map, "volume_id")?;
        let volume_name = required_string(&map, "volume_name")?;
        let file_count = required_u64(&map, "file_count")?;
        let byte_count = required_u64(&map, "byte_count")?;

        match format {
            OutputFormat::Json => {
                println!(
                    "{{\"from\":\"{}\",\"volume_id\":\"{}\",\"volume_name\":\"{}\",\"file_count\":{},\"byte_count\":{}}}",
                    from, volume_id, volume_name, file_count, byte_count
                );
            }
            OutputFormat::Table => {
                println!("✓ Restored {} from {}", volume_name, from);
                println!();
                println!("  Volume ID:  {}", volume_id);
                println!("  Files:      {}", file_count);
                println!("  Bytes:      {}", byte_count);
            }
        }
        Ok(())
    }
}

fn binary_val(s: &str) -> Term {
    Term::Binary(eetf::Binary {
        bytes: s.as_bytes().to_vec(),
    })
}

fn required_string(map: &HashMap<String, Term>, key: &str) -> Result<String> {
    map.get(key)
        .ok_or_else(|| CliError::TermConversionError(format!("Missing '{}'", key)))
        .and_then(term_to_string)
}

fn required_u64(map: &HashMap<String, Term>, key: &str) -> Result<u64> {
    map.get(key)
        .ok_or_else(|| CliError::TermConversionError(format!("Missing '{}'", key)))
        .and_then(term_to_u64)
}

// Pull in `term_to_list` so unused-import doesn't fire if the
// future schema additions need list decoding; remove once the
// dependency materialises.
#[allow(dead_code)]
fn _silence_unused() {
    let _ = term_to_list as fn(&Term) -> _;
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: BackupCommand,
    }

    #[test]
    fn create_parses_minimal_args() {
        let cli =
            TestCli::try_parse_from(["test", "create", "--volume", "v", "--to", "/tmp/b.tar"])
                .unwrap();

        match cli.command {
            BackupCommand::Create { volume, to, name } => {
                assert_eq!(volume, "v");
                assert_eq!(to, "/tmp/b.tar");
                assert!(name.is_none());
            }
            _ => panic!("expected Create"),
        }
    }

    #[test]
    fn create_accepts_name() {
        let cli = TestCli::try_parse_from([
            "test", "create", "--volume", "v", "--to", "/tmp/b.tar", "--name", "weekly",
        ])
        .unwrap();

        match cli.command {
            BackupCommand::Create { name, .. } => assert_eq!(name.as_deref(), Some("weekly")),
            _ => panic!("expected Create"),
        }
    }

    #[test]
    fn list_parses() {
        let cli = TestCli::try_parse_from(["test", "list", "--from", "/tmp/b.tar"]).unwrap();
        match cli.command {
            BackupCommand::List { from } => assert_eq!(from, "/tmp/b.tar"),
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn restore_parses() {
        let cli = TestCli::try_parse_from([
            "test", "restore", "--from", "/tmp/b.tar", "--as", "restored-vol",
        ])
        .unwrap();
        match cli.command {
            BackupCommand::Restore { from, new_name } => {
                assert_eq!(from, "/tmp/b.tar");
                assert_eq!(new_name, "restored-vol");
            }
            _ => panic!("expected Restore"),
        }
    }

    #[test]
    fn restore_requires_from_and_as() {
        assert!(TestCli::try_parse_from(["test", "restore"]).is_err());
        assert!(TestCli::try_parse_from(["test", "restore", "--from", "/x"]).is_err());
        assert!(TestCli::try_parse_from(["test", "restore", "--as", "v"]).is_err());
    }
}
