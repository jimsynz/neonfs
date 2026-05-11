//! Volume management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::{RotationStatus, VolumeInfo};
use crate::term::{
    extract_error, term_to_list, term_to_map, term_to_string, term_to_u64, unwrap_ok_tuple,
};
use clap::Subcommand;
use eetf::{Atom, Binary, FixInteger, Map, Term};
use std::collections::HashMap;

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

        /// Scrub interval in seconds (time between full integrity scans)
        #[arg(long)]
        scrub_interval: Option<u64>,

        /// Access time update mode (noatime or relatime)
        #[arg(long)]
        atime_mode: Option<String>,
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
    List {
        /// Include system volumes (e.g. _system)
        #[arg(long)]
        all: bool,
    },

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

    /// Update volume configuration
    Update {
        /// Volume name
        name: String,

        // -- General --
        /// Compression algorithm (none/zstd)
        #[arg(long, help_heading = "General")]
        compression: Option<String>,

        /// Write acknowledgement level (local/quorum/all)
        #[arg(long, help_heading = "General")]
        write_ack: Option<String>,

        /// I/O scheduling weight (positive integer)
        #[arg(long, help_heading = "General")]
        io_weight: Option<u32>,

        /// Access time update mode (noatime/relatime)
        #[arg(long, help_heading = "General")]
        atime_mode: Option<String>,

        // -- Tiering --
        /// Initial storage tier (hot/warm/cold)
        #[arg(long, help_heading = "Tiering")]
        initial_tier: Option<String>,

        /// Promotion threshold (accesses per hour)
        #[arg(long, help_heading = "Tiering")]
        promotion_threshold: Option<u32>,

        /// Demotion delay (hours)
        #[arg(long, help_heading = "Tiering")]
        demotion_delay: Option<u32>,

        // -- Caching --
        /// Cache transformed chunks (true/false)
        #[arg(long, help_heading = "Caching")]
        cache_transformed: Option<bool>,

        /// Cache reconstructed stripes (true/false)
        #[arg(long, help_heading = "Caching")]
        cache_reconstructed: Option<bool>,

        /// Cache remote chunks (true/false)
        #[arg(long, help_heading = "Caching")]
        cache_remote: Option<bool>,

        // -- Verification --
        /// Verify chunks on read (always/never/sampling)
        #[arg(long, help_heading = "Verification")]
        verify_on_read: Option<String>,

        /// Sampling rate for read verification (0.0-1.0)
        #[arg(long, help_heading = "Verification")]
        verify_sampling_rate: Option<f64>,

        /// Scrub interval (hours)
        #[arg(long, help_heading = "Verification")]
        scrub_interval: Option<u64>,

        // -- Metadata Consistency --
        /// Number of metadata replicas
        #[arg(long, help_heading = "Metadata Consistency")]
        metadata_replicas: Option<u32>,

        /// Read quorum size
        #[arg(long, help_heading = "Metadata Consistency")]
        read_quorum: Option<u32>,

        /// Write quorum size
        #[arg(long, help_heading = "Metadata Consistency")]
        write_quorum: Option<u32>,
    },

    /// Inspect or trigger garbage collection for a single volume.
    ///
    /// With no flags: prints the current GC schedule (interval,
    /// last_run, next_run_due_at) plus the latest GC job for the
    /// volume.
    ///
    /// With --now: triggers an immediate GC job for the volume.
    ///
    /// With --interval: updates the per-volume GC cadence in the
    /// volume's root segment. Accepts `s`, `m`, `h`, `d` suffixes
    /// (e.g. `24h`, `30m`); minimum 1 minute.
    Gc {
        /// Volume name
        name: String,

        /// Trigger an immediate GC job for the volume.
        #[arg(long, conflicts_with = "interval")]
        now: bool,

        /// New GC cadence (e.g. `24h`, `30m`). Stored in the
        /// volume's `RootSegment.schedules.gc.interval_ms`.
        #[arg(long)]
        interval: Option<String>,
    },

    /// Inspect or trigger integrity scrub for a single volume.
    ///
    /// With no flags: prints the current scrub schedule plus the
    /// latest scrub job for the volume.
    ///
    /// With --now: triggers an immediate scrub job.
    ///
    /// With --interval: updates the per-volume scrub cadence in the
    /// volume's `RootSegment.schedules.scrub.interval_ms`. Accepts
    /// `s`, `m`, `h`, `d` suffixes; minimum 1 minute.
    Scrub {
        /// Volume name
        name: String,

        /// Trigger an immediate scrub job for the volume.
        #[arg(long, conflicts_with = "interval")]
        now: bool,

        /// New scrub cadence (e.g. `7d`, `24h`).
        #[arg(long)]
        interval: Option<String>,
    },

    /// Inspect or trigger per-volume anti-entropy reconciliation.
    ///
    /// With no flags: prints the current schedule plus the latest
    /// anti-entropy job for the volume.
    ///
    /// With --now: triggers an immediate anti-entropy job.
    ///
    /// With --interval: updates the per-volume cadence in the
    /// volume's `RootSegment.schedules.anti_entropy.interval_ms`.
    /// Accepts `s`, `m`, `h`, `d` suffixes; minimum 1 minute.
    AntiEntropy {
        /// Volume name
        name: String,

        /// Trigger an immediate anti-entropy job for the volume.
        #[arg(long, conflicts_with = "interval")]
        now: bool,

        /// New anti-entropy cadence (e.g. `1h`, `30m`).
        #[arg(long)]
        interval: Option<String>,
    },

    /// Manage per-volume snapshots (#962 / epic #959).
    ///
    /// A snapshot is a frozen pointer to the volume's current root
    /// chunk. Create is O(1); chunks shared with the live head share
    /// storage transparently.
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },

    /// Promote a snapshot to a brand-new top-level volume (#964).
    ///
    /// The new volume points at the snapshot's root chunk — no bytes
    /// are copied. Both volumes pin the same content-addressed chunk
    /// graph; per-volume GC keeps chunks alive as long as either root
    /// references them. The new volume inherits the source volume's
    /// storage policy.
    Promote {
        /// Source volume name
        source: String,

        /// Snapshot id or name on the source volume
        snapshot: String,

        /// Name of the new volume to create
        #[arg(long = "as")]
        new_name: String,
    },

    /// Export a volume's live root as a portable tarball (#965).
    ///
    /// V1: live root only, local output path only. Snapshot export,
    /// ACL/xattr capture, and S3/file:// URL outputs are tracked as
    /// follow-ups. The output path is on the daemon's filesystem.
    Export {
        /// Volume name
        volume: String,

        /// Output tarball path on the daemon's filesystem
        #[arg(long = "to")]
        to: String,
    },

    /// Rollback a volume's live root to a snapshot (#963).
    ///
    /// Destructive in the general case: chunks reachable from the
    /// current live root but not from any remaining snapshot become
    /// unreferenced and are reclaimed by the next GC pass.
    ///
    /// By default the rollback is refused unless the current live
    /// root is already covered by another snapshot — pass `--safe`
    /// (auto-snapshot the current root first; always recoverable)
    /// or `--force --yes` to acknowledge the discard.
    Restore {
        /// Volume name
        volume: String,

        /// Snapshot id or name to restore to
        #[arg(long = "to")]
        snapshot: String,

        /// Auto-create a `pre-restore-<id>` snapshot of the current
        /// live root before swapping. The discarded state is always
        /// recoverable via the new snapshot.
        #[arg(long, conflicts_with = "force")]
        safe: bool,

        /// Proceed even when the current live root is not covered by
        /// any snapshot and `--safe` is not set. Requires `--yes`.
        #[arg(long, requires = "yes")]
        force: bool,

        /// Skip the interactive confirmation prompt. Required with
        /// `--force`.
        #[arg(long)]
        r#yes: bool,
    },
}

/// Per-volume snapshot subcommands.
#[derive(Debug, Subcommand)]
pub enum SnapshotCommand {
    /// Create a snapshot of the named volume's current root.
    Create {
        /// Volume name
        volume: String,

        /// Optional human-readable label for the snapshot.
        #[arg(long)]
        name: Option<String>,
    },

    /// List every snapshot for the named volume, newest first.
    List {
        /// Volume name
        volume: String,
    },

    /// Show a single snapshot by id, scoped to the named volume.
    Show {
        /// Volume name
        volume: String,

        /// Snapshot id
        id: String,
    },

    /// Delete the snapshot's pin. Idempotent — missing snapshot is a
    /// no-op. Chunk reclamation is the GC scheduler's job.
    Delete {
        /// Volume name
        volume: String,

        /// Snapshot id or name
        id: String,

        /// Skip the interactive confirmation prompt.
        #[arg(long)]
        r#yes: bool,
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
                scrub_interval,
                atime_mode,
            } => self.create(
                name,
                *replicas,
                compression,
                encryption,
                *scrub_interval,
                atime_mode.as_deref(),
                format,
            ),
            VolumeCommand::Delete { name, force } => self.delete(name, *force, format),
            VolumeCommand::List { all } => self.list(*all, format),
            VolumeCommand::RotateKey { name } => self.rotate_key(name, format),
            VolumeCommand::RotationStatus { name } => self.rotation_status(name, format),
            VolumeCommand::Show { name } => self.show(name, format),
            VolumeCommand::Update {
                name,
                compression,
                write_ack,
                io_weight,
                atime_mode,
                initial_tier,
                promotion_threshold,
                demotion_delay,
                cache_transformed,
                cache_reconstructed,
                cache_remote,
                verify_on_read,
                verify_sampling_rate,
                scrub_interval,
                metadata_replicas,
                read_quorum,
                write_quorum,
            } => self.update(
                name,
                compression.as_deref(),
                write_ack.as_deref(),
                *io_weight,
                atime_mode.as_deref(),
                initial_tier.as_deref(),
                *promotion_threshold,
                *demotion_delay,
                *cache_transformed,
                *cache_reconstructed,
                *cache_remote,
                verify_on_read.as_deref(),
                *verify_sampling_rate,
                *scrub_interval,
                *metadata_replicas,
                *read_quorum,
                *write_quorum,
                format,
            ),
            VolumeCommand::Gc {
                name,
                now,
                interval,
            } => self.gc(name, *now, interval.as_deref(), format),
            VolumeCommand::Scrub {
                name,
                now,
                interval,
            } => self.scrub(name, *now, interval.as_deref(), format),
            VolumeCommand::AntiEntropy {
                name,
                now,
                interval,
            } => self.anti_entropy(name, *now, interval.as_deref(), format),
            VolumeCommand::Snapshot { command } => command.execute(format),
            VolumeCommand::Promote {
                source,
                snapshot,
                new_name,
            } => self.promote(source, snapshot, new_name, format),
            VolumeCommand::Restore {
                volume,
                snapshot,
                safe,
                force,
                r#yes,
            } => self.restore(volume, snapshot, *safe, *force, *r#yes, format),
            VolumeCommand::Export { volume, to } => self.export(volume, to, format),
        }
    }

    fn export(&self, volume: &str, to: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_export",
                vec![binary_val(volume), binary_val(to)],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let path = term_to_string(map.get("path").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'path'".to_string())
        })?)?;
        let file_count = term_to_u64(map.get("file_count").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'file_count'".to_string())
        })?)?;
        let byte_count = term_to_u64(map.get("byte_count").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'byte_count'".to_string())
        })?)?;

        match format {
            OutputFormat::Json => {
                println!(
                    "{{\"volume\":\"{}\",\"path\":\"{}\",\"file_count\":{},\"byte_count\":{}}}",
                    volume, path, file_count, byte_count
                );
            }
            OutputFormat::Table => {
                println!("✓ Exported {} to {}", volume, path);
                println!();
                println!("  Files:  {}", file_count);
                println!("  Bytes:  {}", byte_count);
            }
        }
        Ok(())
    }

    fn restore(
        &self,
        volume: &str,
        snapshot: &str,
        safe: bool,
        force: bool,
        r#yes: bool,
        format: OutputFormat,
    ) -> Result<()> {
        let mut opt_pairs: Vec<(Term, Term)> = vec![];
        if safe {
            opt_pairs.push((
                Term::Atom(Atom::from("safe")),
                Term::Atom(Atom::from("true")),
            ));
        }
        if force {
            opt_pairs.push((
                Term::Atom(Atom::from("force")),
                Term::Atom(Atom::from("true")),
            ));
        }

        let opts_term = Term::Map(Map {
            map: opt_pairs.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_restore",
                vec![binary_val(volume), binary_val(snapshot), opts_term],
            )
            .await
        })?;

        // Distinguish the "uncovered + not forced" precondition error
        // from any other failure so we can give the operator a clear
        // next-step hint.
        if let Some(err) = extract_error(&result) {
            if matches!(&err, crate::error::CliError::InvalidArgument(msg) if msg.contains("unreferenced_chunks"))
                && !r#yes
            {
                eprintln!(
                    "✗ Refusing to restore: the current live root of `{}` is not covered by any \
                     snapshot. Restoring would permanently discard the chunks reachable only \
                     from the current root.\n\n\
                     Re-run with `--safe` (recommended; auto-snapshots the current root first) \
                     or `--force --yes` to discard.",
                    volume
                );
                return Err(err);
            }
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let previous_root = term_to_string(map.get("previous_root_hex").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'previous_root_hex'".to_string())
        })?)?;
        let new_root = term_to_string(map.get("new_root_hex").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'new_root_hex'".to_string())
        })?)?;
        let pre_restore = match map.get("pre_restore_snapshot_id") {
            Some(Term::Atom(a)) if a.name == "nil" => None,
            Some(term) => Some(term_to_string(term)?),
            None => None,
        };

        match format {
            OutputFormat::Json => {
                let pre = pre_restore
                    .as_deref()
                    .map(|s| format!("\"{}\"", s))
                    .unwrap_or_else(|| "null".to_string());
                println!(
                    "{{\"volume\":\"{}\",\"snapshot\":\"{}\",\"previous_root_hex\":\"{}\",\"new_root_hex\":\"{}\",\"pre_restore_snapshot_id\":{}}}",
                    volume, snapshot, previous_root, new_root, pre
                );
            }
            OutputFormat::Table => {
                if previous_root == new_root {
                    println!(
                        "✓ No-op: {} already at root {} (matching snapshot {})",
                        volume, new_root, snapshot
                    );
                } else {
                    println!("✓ Restored {} to snapshot {}", volume, snapshot);
                    println!();
                    println!("  Previous root: {}", previous_root);
                    println!("  New root:      {}", new_root);
                    if let Some(pre) = pre_restore {
                        println!("  Safety snapshot: {} (pinned the previous root)", pre);
                    }
                }
            }
        }
        Ok(())
    }

    fn promote(
        &self,
        source: &str,
        snapshot: &str,
        new_name: &str,
        format: OutputFormat,
    ) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_promote",
                vec![
                    binary_val(source),
                    binary_val(snapshot),
                    binary_val(new_name),
                    Term::Map(Map {
                        map: vec![].into_iter().collect(),
                    }),
                ],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let volume_id = term_to_string(map.get("volume_id").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'volume_id'".to_string())
        })?)?;
        let volume_name = term_to_string(map.get("volume_name").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'volume_name'".to_string())
        })?)?;
        let snapshot_id = term_to_string(map.get("snapshot_id").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'snapshot_id'".to_string())
        })?)?;
        let root_chunk_hash_hex =
            term_to_string(map.get("root_chunk_hash_hex").ok_or_else(|| {
                crate::error::CliError::TermConversionError(
                    "Missing 'root_chunk_hash_hex'".to_string(),
                )
            })?)?;

        match format {
            OutputFormat::Json => {
                println!(
                    "{{\"volume_id\":\"{}\",\"volume_name\":\"{}\",\"source\":\"{}\",\"snapshot_id\":\"{}\",\"root_chunk_hash_hex\":\"{}\"}}",
                    volume_id, volume_name, source, snapshot_id, root_chunk_hash_hex
                );
            }
            OutputFormat::Table => {
                println!("✓ Promoted snapshot {} from {} to volume {}", snapshot_id, source, volume_name);
                println!();
                println!("  Volume ID:    {}", volume_id);
                println!("  Volume Name:  {}", volume_name);
                println!("  Source:       {}", source);
                println!("  Snapshot:     {}", snapshot_id);
                println!("  Root:         {}", root_chunk_hash_hex);
            }
        }
        Ok(())
    }

    fn list(&self, all: bool, format: OutputFormat) -> Result<()> {
        let mut filter_entries = vec![];

        if all {
            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"all".to_vec(),
                }),
                Term::Atom(Atom::from("true")),
            ));
        }

        let filters_term = Term::Map(Map {
            map: filter_entries.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "list_volumes",
                vec![filters_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
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

    #[allow(clippy::too_many_arguments)]
    fn create(
        &self,
        name: &str,
        replicas: u32,
        compression: &str,
        encryption: &str,
        scrub_interval: Option<u64>,
        atime_mode: Option<&str>,
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

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let durability_map = Term::Map(Map {
            map: HashMap::from([
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
            ]),
        });

        let compression_map = Term::Map(Map {
            map: HashMap::from([
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
            ]),
        });

        let encryption_map = Term::Map(Map {
            map: HashMap::from([(
                Term::Atom(Atom::from("mode")),
                Term::Atom(Atom::from(encryption_mode)),
            )]),
        });

        let mut config_entries = vec![
            (Term::Atom(Atom::from("durability")), durability_map),
            (Term::Atom(Atom::from("compression")), compression_map),
            (Term::Atom(Atom::from("encryption")), encryption_map),
        ];

        if let Some(interval) = scrub_interval {
            let verification_map = Term::Map(Map {
                map: HashMap::from([(
                    Term::Atom(Atom::from("scrub_interval")),
                    Term::FixInteger(FixInteger::from(interval as i32)),
                )]),
            });
            config_entries.push((Term::Atom(Atom::from("verification")), verification_map));
        }

        if let Some(atime) = atime_mode {
            let atime_atom = match atime {
                "noatime" => "noatime",
                "relatime" => "relatime",
                other => {
                    return Err(crate::error::CliError::InvalidArgument(format!(
                        "Invalid atime mode '{}'. Valid: noatime, relatime",
                        other
                    )));
                }
            };
            config_entries.push((
                Term::Atom(Atom::from("atime_mode")),
                Term::Atom(Atom::from(atime_atom)),
            ));
        }

        let config_term = Term::Map(Map {
            map: config_entries.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "create_volume",
                vec![name_term, config_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
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
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "delete_volume",
                vec![name_term],
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

    fn gc(
        &self,
        name: &str,
        now: bool,
        interval: Option<&str>,
        format: OutputFormat,
    ) -> Result<()> {
        match (now, interval) {
            (true, _) => self.gc_now(name, format),
            (false, Some(d)) => self.gc_set_interval(name, d, format),
            (false, None) => self.gc_status(name, format),
        }
    }

    fn gc_now(&self, name: &str, format: OutputFormat) -> Result<()> {
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_gc_now",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let job_map = term_to_map(&data)?;

        let job_id = job_map
            .get("id")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "started",
                    "job_id": job_id,
                    "volume": name,
                });
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            OutputFormat::Table => {
                println!("Garbage collection started for volume '{}'", name);
                println!("  Job ID: {}", job_id);
            }
        }

        Ok(())
    }

    fn gc_set_interval(&self, name: &str, interval: &str, format: OutputFormat) -> Result<()> {
        let interval_ms = parse_duration_ms(interval)?;

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });
        let interval_term = Term::FixInteger(FixInteger {
            value: interval_ms as i32,
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_gc_set_interval",
                vec![name_term, interval_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let stored_interval = map
            .get("schedule")
            .and_then(|t| term_to_map(t).ok())
            .and_then(|m| m.get("interval_ms").cloned())
            .and_then(|t| crate::term::term_to_i64(&t).ok())
            .unwrap_or(interval_ms);

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "volume": name,
                    "interval_ms": stored_interval,
                });
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            OutputFormat::Table => {
                println!("Updated GC interval for volume '{}'", name);
                println!("  interval_ms: {}", stored_interval);
            }
        }

        Ok(())
    }

    fn gc_status(&self, name: &str, format: OutputFormat) -> Result<()> {
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_gc_status",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let schedule_map = map
            .get("schedule")
            .and_then(|t| term_to_map(t).ok())
            .unwrap_or_default();

        let interval_ms = schedule_map
            .get("interval_ms")
            .and_then(|t| crate::term::term_to_i64(t).ok())
            .map(|n| n.to_string())
            .unwrap_or_else(|| "<unset>".to_string());

        let last_run = schedule_map
            .get("last_run")
            .map(|t| term_to_string(t).unwrap_or_else(|_| "never".to_string()))
            .unwrap_or_else(|| "never".to_string());

        let next_run = map
            .get("next_run_due_at")
            .map(|t| term_to_string(t).unwrap_or_else(|_| "<unknown>".to_string()))
            .unwrap_or_else(|| "<unknown>".to_string());

        let latest_job_id = map
            .get("latest_job")
            .and_then(|t| term_to_map(t).ok())
            .and_then(|m| m.get("id").cloned())
            .map(|t| term_to_string(&t).unwrap_or_default())
            .unwrap_or_default();

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "volume": name,
                    "interval_ms": interval_ms,
                    "last_run": last_run,
                    "next_run_due_at": next_run,
                    "latest_job_id": latest_job_id,
                });
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            OutputFormat::Table => {
                println!("GC schedule for volume '{}'", name);
                println!("  interval_ms     : {}", interval_ms);
                println!("  last_run        : {}", last_run);
                println!("  next_run_due_at : {}", next_run);
                println!("  latest_job_id   : {}", latest_job_id);
            }
        }

        Ok(())
    }

    fn scrub(
        &self,
        name: &str,
        now: bool,
        interval: Option<&str>,
        format: OutputFormat,
    ) -> Result<()> {
        match (now, interval) {
            (true, _) => self.scrub_now(name, format),
            (false, Some(d)) => self.scrub_set_interval(name, d, format),
            (false, None) => self.scrub_status(name, format),
        }
    }

    fn scrub_now(&self, name: &str, format: OutputFormat) -> Result<()> {
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_scrub_now",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let job_map = term_to_map(&data)?;

        let job_id = job_map
            .get("id")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "started",
                    "job_id": job_id,
                    "volume": name,
                });
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            OutputFormat::Table => {
                println!("Scrub started for volume '{}'", name);
                println!("  Job ID: {}", job_id);
            }
        }

        Ok(())
    }

    fn scrub_set_interval(&self, name: &str, interval: &str, format: OutputFormat) -> Result<()> {
        let interval_ms = parse_duration_ms(interval)?;

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });
        let interval_term = Term::FixInteger(FixInteger {
            value: interval_ms as i32,
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_scrub_set_interval",
                vec![name_term, interval_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let stored_interval = map
            .get("schedule")
            .and_then(|t| term_to_map(t).ok())
            .and_then(|m| m.get("interval_ms").cloned())
            .and_then(|t| crate::term::term_to_i64(&t).ok())
            .unwrap_or(interval_ms);

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "volume": name,
                    "interval_ms": stored_interval,
                });
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            OutputFormat::Table => {
                println!("Updated scrub interval for volume '{}'", name);
                println!("  interval_ms: {}", stored_interval);
            }
        }

        Ok(())
    }

    fn scrub_status(&self, name: &str, format: OutputFormat) -> Result<()> {
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_scrub_status",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let schedule_map = map
            .get("schedule")
            .and_then(|t| term_to_map(t).ok())
            .unwrap_or_default();

        let interval_ms = schedule_map
            .get("interval_ms")
            .and_then(|t| crate::term::term_to_i64(t).ok())
            .map(|n| n.to_string())
            .unwrap_or_else(|| "<unset>".to_string());

        let last_run = schedule_map
            .get("last_run")
            .map(|t| term_to_string(t).unwrap_or_else(|_| "never".to_string()))
            .unwrap_or_else(|| "never".to_string());

        let next_run = map
            .get("next_run_due_at")
            .map(|t| term_to_string(t).unwrap_or_else(|_| "<unknown>".to_string()))
            .unwrap_or_else(|| "<unknown>".to_string());

        let latest_job_id = map
            .get("latest_job")
            .and_then(|t| term_to_map(t).ok())
            .and_then(|m| m.get("id").cloned())
            .map(|t| term_to_string(&t).unwrap_or_default())
            .unwrap_or_default();

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "volume": name,
                    "interval_ms": interval_ms,
                    "last_run": last_run,
                    "next_run_due_at": next_run,
                    "latest_job_id": latest_job_id,
                });
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            OutputFormat::Table => {
                println!("Scrub schedule for volume '{}'", name);
                println!("  interval_ms     : {}", interval_ms);
                println!("  last_run        : {}", last_run);
                println!("  next_run_due_at : {}", next_run);
                println!("  latest_job_id   : {}", latest_job_id);
            }
        }

        Ok(())
    }

    fn anti_entropy(
        &self,
        name: &str,
        now: bool,
        interval: Option<&str>,
        format: OutputFormat,
    ) -> Result<()> {
        match (now, interval) {
            (true, _) => self.anti_entropy_now(name, format),
            (false, Some(d)) => self.anti_entropy_set_interval(name, d, format),
            (false, None) => self.anti_entropy_status(name, format),
        }
    }

    fn anti_entropy_now(&self, name: &str, format: OutputFormat) -> Result<()> {
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_anti_entropy_now",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let job_map = term_to_map(&data)?;

        let job_id = job_map
            .get("id")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "started",
                    "job_id": job_id,
                    "volume": name,
                });
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            OutputFormat::Table => {
                println!("Anti-entropy started for volume '{}'", name);
                println!("  Job ID: {}", job_id);
            }
        }

        Ok(())
    }

    fn anti_entropy_set_interval(
        &self,
        name: &str,
        interval: &str,
        format: OutputFormat,
    ) -> Result<()> {
        let interval_ms = parse_duration_ms(interval)?;

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });
        let interval_term = Term::FixInteger(FixInteger {
            value: interval_ms as i32,
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_anti_entropy_set_interval",
                vec![name_term, interval_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let stored_interval = map
            .get("schedule")
            .and_then(|t| term_to_map(t).ok())
            .and_then(|m| m.get("interval_ms").cloned())
            .and_then(|t| crate::term::term_to_i64(&t).ok())
            .unwrap_or(interval_ms);

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "volume": name,
                    "interval_ms": stored_interval,
                });
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            OutputFormat::Table => {
                println!("Updated anti-entropy interval for volume '{}'", name);
                println!("  interval_ms: {}", stored_interval);
            }
        }

        Ok(())
    }

    fn anti_entropy_status(&self, name: &str, format: OutputFormat) -> Result<()> {
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_anti_entropy_status",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let map = term_to_map(&data)?;

        let schedule_map = map
            .get("schedule")
            .and_then(|t| term_to_map(t).ok())
            .unwrap_or_default();

        let interval_ms = schedule_map
            .get("interval_ms")
            .and_then(|t| crate::term::term_to_i64(t).ok())
            .map(|n| n.to_string())
            .unwrap_or_else(|| "<unset>".to_string());

        let last_run = schedule_map
            .get("last_run")
            .map(|t| term_to_string(t).unwrap_or_else(|_| "never".to_string()))
            .unwrap_or_else(|| "never".to_string());

        let next_run = map
            .get("next_run_due_at")
            .map(|t| term_to_string(t).unwrap_or_else(|_| "<unknown>".to_string()))
            .unwrap_or_else(|| "<unknown>".to_string());

        let latest_job_id = map
            .get("latest_job")
            .and_then(|t| term_to_map(t).ok())
            .and_then(|m| m.get("id").cloned())
            .map(|t| term_to_string(&t).unwrap_or_default())
            .unwrap_or_default();

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "volume": name,
                    "interval_ms": interval_ms,
                    "last_run": last_run,
                    "next_run_due_at": next_run,
                    "latest_job_id": latest_job_id,
                });
                println!("{}", serde_json::to_string_pretty(&response)?);
            }
            OutputFormat::Table => {
                println!("Anti-entropy schedule for volume '{}'", name);
                println!("  interval_ms     : {}", interval_ms);
                println!("  last_run        : {}", last_run);
                println!("  next_run_due_at : {}", next_run);
                println!("  latest_job_id   : {}", latest_job_id);
            }
        }

        Ok(())
    }

    fn show(&self, name: &str, format: OutputFormat) -> Result<()> {
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "get_volume", vec![name_term])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
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

    #[allow(clippy::too_many_arguments)]
    fn update(
        &self,
        name: &str,
        compression: Option<&str>,
        write_ack: Option<&str>,
        io_weight: Option<u32>,
        atime_mode: Option<&str>,
        initial_tier: Option<&str>,
        promotion_threshold: Option<u32>,
        demotion_delay: Option<u32>,
        cache_transformed: Option<bool>,
        cache_reconstructed: Option<bool>,
        cache_remote: Option<bool>,
        verify_on_read: Option<&str>,
        verify_sampling_rate: Option<f64>,
        scrub_interval: Option<u64>,
        metadata_replicas: Option<u32>,
        read_quorum: Option<u32>,
        write_quorum: Option<u32>,
        format: OutputFormat,
    ) -> Result<()> {
        let mut config_entries: Vec<(Term, Term)> = Vec::new();

        if let Some(v) = compression {
            config_entries.push((binary_key("compression"), binary_val(v)));
        }
        if let Some(v) = write_ack {
            config_entries.push((binary_key("write_ack"), binary_val(v)));
        }
        if let Some(v) = io_weight {
            config_entries.push((binary_key("io_weight"), int_val(v)));
        }
        if let Some(v) = atime_mode {
            config_entries.push((binary_key("atime_mode"), binary_val(v)));
        }
        if let Some(v) = initial_tier {
            config_entries.push((binary_key("initial_tier"), binary_val(v)));
        }
        if let Some(v) = promotion_threshold {
            config_entries.push((binary_key("promotion_threshold"), int_val(v)));
        }
        if let Some(v) = demotion_delay {
            config_entries.push((binary_key("demotion_delay"), int_val(v)));
        }
        if let Some(v) = cache_transformed {
            config_entries.push((binary_key("transformed_chunks"), bool_val(v)));
        }
        if let Some(v) = cache_reconstructed {
            config_entries.push((binary_key("reconstructed_stripes"), bool_val(v)));
        }
        if let Some(v) = cache_remote {
            config_entries.push((binary_key("remote_chunks"), bool_val(v)));
        }
        if let Some(v) = verify_on_read {
            config_entries.push((binary_key("on_read"), binary_val(v)));
        }
        if let Some(v) = verify_sampling_rate {
            config_entries.push((
                binary_key("sampling_rate"),
                Term::Float(eetf::Float { value: v }),
            ));
        }
        if let Some(v) = scrub_interval {
            config_entries.push((binary_key("scrub_interval"), int_val(v as u32)));
        }
        if let Some(v) = metadata_replicas {
            config_entries.push((binary_key("metadata_replicas"), int_val(v)));
        }
        if let Some(v) = read_quorum {
            config_entries.push((binary_key("read_quorum"), int_val(v)));
        }
        if let Some(v) = write_quorum {
            config_entries.push((binary_key("write_quorum"), int_val(v)));
        }

        if config_entries.is_empty() {
            return Err(crate::error::CliError::InvalidArgument(
                "At least one update flag must be specified".to_string(),
            ));
        }

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });
        let config_term = Term::Map(Map {
            map: config_entries.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "update_volume",
                vec![name_term, config_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let volume = VolumeInfo::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&volume)?);
            }
            OutputFormat::Table => {
                println!("Volume '{}' updated successfully", volume.name);
                println!("  ID: {}", volume.id);
                println!("  Durability: {}", volume.durability_string());
            }
        }
        Ok(())
    }

    fn rotate_key(&self, name: &str, format: OutputFormat) -> Result<()> {
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "rotate_volume_key",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
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
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "rotation_status",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
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

impl SnapshotCommand {
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            SnapshotCommand::Create { volume, name } => self.create(volume, name.as_deref(), format),
            SnapshotCommand::List { volume } => self.list(volume, format),
            SnapshotCommand::Show { volume, id } => self.show(volume, id, format),
            SnapshotCommand::Delete { volume, id, yes } => self.delete(volume, id, *yes, format),
        }
    }

    fn create(&self, volume: &str, name: Option<&str>, format: OutputFormat) -> Result<()> {
        let opts = match name {
            Some(name) => Term::Map(Map {
                map: vec![(binary_val("name"), binary_val(name))]
                    .into_iter()
                    .collect(),
            }),
            None => Term::Map(Map {
                map: vec![].into_iter().collect(),
            }),
        };

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_snapshot_create",
                vec![binary_val(volume), opts],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let snap = SnapshotEntry::from_term(data)?;
        print_snapshot(&snap, "Snapshot created", format)
    }

    fn list(&self, volume: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_snapshot_list",
                vec![binary_val(volume)],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let entries = term_to_list(&data)?;
        let snaps: Result<Vec<SnapshotEntry>> =
            entries.into_iter().map(SnapshotEntry::from_term).collect();
        let snaps = snaps?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&snaps)?);
            }
            OutputFormat::Table => {
                let mut tbl = table::Table::new(vec![
                    "ID".to_string(),
                    "NAME".to_string(),
                    "ROOT".to_string(),
                    "CREATED".to_string(),
                ]);
                for snap in &snaps {
                    tbl.add_row(vec![
                        snap.id.clone(),
                        snap.name.clone().unwrap_or_default(),
                        snap.root_chunk_hash_hex
                            .chars()
                            .take(12)
                            .collect::<String>(),
                        snap.created_at.clone(),
                    ]);
                }
                print!("{}", tbl.render()?);
            }
        }
        Ok(())
    }

    fn show(&self, volume: &str, id: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_snapshot_show",
                vec![binary_val(volume), binary_val(id)],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let snap = SnapshotEntry::from_term(data)?;
        print_snapshot(&snap, "Snapshot", format)
    }

    fn delete(&self, volume: &str, id: &str, yes: bool, format: OutputFormat) -> Result<()> {
        if !yes && matches!(format, OutputFormat::Table) {
            eprintln!(
                "Refusing to delete snapshot {} from {} without --yes. Pass --yes to skip this prompt.",
                id, volume
            );
            return Err(crate::error::CliError::InvalidArgument(
                "snapshot delete requires --yes".to_string(),
            ));
        }

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_volume_snapshot_delete",
                vec![binary_val(volume), binary_val(id)],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        match format {
            OutputFormat::Json => {
                println!("{{\"status\":\"deleted\",\"snapshot_id\":\"{}\"}}", id);
            }
            OutputFormat::Table => {
                println!("✓ Snapshot {} deleted from {}", id, volume);
            }
        }
        Ok(())
    }
}

#[derive(Debug, serde::Serialize)]
struct SnapshotEntry {
    id: String,
    volume_id: String,
    volume_name: String,
    name: Option<String>,
    root_chunk_hash_hex: String,
    created_at: String,
}

impl SnapshotEntry {
    fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        let id = term_to_string(map.get("id").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'id' field".to_string())
        })?)?;

        let volume_id = term_to_string(map.get("volume_id").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'volume_id' field".to_string())
        })?)?;

        let volume_name = term_to_string(map.get("volume_name").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'volume_name' field".to_string())
        })?)?;

        let name = match map.get("name") {
            Some(Term::Atom(atom)) if atom.name == "nil" => None,
            Some(t) => Some(term_to_string(t)?),
            None => None,
        };

        let root_chunk_hash_hex =
            term_to_string(map.get("root_chunk_hash_hex").ok_or_else(|| {
                crate::error::CliError::TermConversionError(
                    "Missing 'root_chunk_hash_hex' field".to_string(),
                )
            })?)?;

        let created_at = term_to_string(map.get("created_at").ok_or_else(|| {
            crate::error::CliError::TermConversionError("Missing 'created_at' field".to_string())
        })?)?;

        Ok(SnapshotEntry {
            id,
            volume_id,
            volume_name,
            name,
            root_chunk_hash_hex,
            created_at,
        })
    }
}

fn print_snapshot(snap: &SnapshotEntry, heading: &str, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", json::format(snap)?);
        }
        OutputFormat::Table => {
            println!("✓ {}", heading);
            println!();
            println!("  ID:      {}", snap.id);
            if let Some(name) = &snap.name {
                println!("  Name:    {}", name);
            }
            println!("  Volume:  {}", snap.volume_name);
            println!("  Root:    {}", snap.root_chunk_hash_hex);
            println!("  Created: {}", snap.created_at);
        }
    }
    Ok(())
}

fn binary_key(key: &str) -> Term {
    Term::Binary(Binary {
        bytes: key.as_bytes().to_vec(),
    })
}

fn binary_val(val: &str) -> Term {
    Term::Binary(Binary {
        bytes: val.as_bytes().to_vec(),
    })
}

fn int_val(val: u32) -> Term {
    Term::FixInteger(FixInteger::from(val as i32))
}

fn bool_val(val: bool) -> Term {
    Term::Atom(Atom::from(if val { "true" } else { "false" }))
}

/// Parse a duration string ("24h", "30m", "1d", "60s", or a raw
/// integer of milliseconds) into milliseconds. Used by
/// `volume gc <name> --interval`.
fn parse_duration_ms(input: &str) -> Result<i64> {
    if let Ok(ms) = input.parse::<i64>() {
        return Ok(ms);
    }

    if input.len() < 2 {
        return Err(crate::error::CliError::InvalidArgument(format!(
            "invalid duration '{}'",
            input
        )));
    }

    let (value_str, unit) = input.split_at(input.len() - 1);
    let value = value_str.parse::<i64>().map_err(|_| {
        crate::error::CliError::InvalidArgument(format!("invalid duration value '{}'", input))
    })?;

    let multiplier_ms: i64 = match unit {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => {
            return Err(crate::error::CliError::InvalidArgument(format!(
                "invalid duration unit '{}' (use s, m, h, d)",
                unit
            )))
        }
    };

    Ok(value * multiplier_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_ms_units() {
        assert_eq!(parse_duration_ms("60s").unwrap(), 60_000);
        assert_eq!(parse_duration_ms("30m").unwrap(), 1_800_000);
        assert_eq!(parse_duration_ms("24h").unwrap(), 86_400_000);
        assert_eq!(parse_duration_ms("7d").unwrap(), 604_800_000);
    }

    #[test]
    fn test_parse_duration_ms_raw_integer() {
        assert_eq!(parse_duration_ms("60000").unwrap(), 60_000);
    }

    #[test]
    fn test_parse_duration_ms_rejects_garbage() {
        assert!(parse_duration_ms("nope").is_err());
        assert!(parse_duration_ms("12y").is_err());
        assert!(parse_duration_ms("h").is_err());
        assert!(parse_duration_ms("xh").is_err());
    }

    #[test]
    fn test_volume_scrub_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: VolumeCommand,
        }

        let cli = TestCli::try_parse_from(["test", "scrub", "myvol"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = &cli {
            assert!(matches!(
                parsed.command,
                VolumeCommand::Scrub {
                    now: false,
                    interval: None,
                    ..
                }
            ));
        }

        let cli = TestCli::try_parse_from(["test", "scrub", "myvol", "--now"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = &cli {
            assert!(matches!(
                parsed.command,
                VolumeCommand::Scrub {
                    now: true,
                    interval: None,
                    ..
                }
            ));
        }

        let cli = TestCli::try_parse_from(["test", "scrub", "myvol", "--interval", "7d"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = &cli {
            assert!(matches!(
                parsed.command,
                VolumeCommand::Scrub {
                    now: false,
                    interval: Some(_),
                    ..
                }
            ));
        }

        // --now and --interval are mutually exclusive.
        let cli = TestCli::try_parse_from(["test", "scrub", "myvol", "--now", "--interval", "7d"]);
        assert!(cli.is_err());
    }

    #[test]
    fn test_volume_gc_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: VolumeCommand,
        }

        // Bare form (status).
        let cli = TestCli::try_parse_from(["test", "gc", "myvol"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = &cli {
            assert!(matches!(
                parsed.command,
                VolumeCommand::Gc {
                    now: false,
                    interval: None,
                    ..
                }
            ));
        }

        // --now triggers immediate.
        let cli = TestCli::try_parse_from(["test", "gc", "myvol", "--now"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = &cli {
            assert!(matches!(
                parsed.command,
                VolumeCommand::Gc {
                    now: true,
                    interval: None,
                    ..
                }
            ));
        }

        // --interval updates cadence.
        let cli = TestCli::try_parse_from(["test", "gc", "myvol", "--interval", "24h"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = &cli {
            assert!(matches!(
                parsed.command,
                VolumeCommand::Gc {
                    now: false,
                    interval: Some(_),
                    ..
                }
            ));
        }

        // --now and --interval are mutually exclusive.
        let cli = TestCli::try_parse_from(["test", "gc", "myvol", "--now", "--interval", "24h"]);
        assert!(cli.is_err());
    }

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
        if let Ok(parsed) = &cli {
            assert!(matches!(parsed.command, VolumeCommand::List { all: false }));
        }

        let cli = TestCli::try_parse_from(["test", "list", "--all"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = &cli {
            assert!(matches!(parsed.command, VolumeCommand::List { all: true }));
        }

        let cli = TestCli::try_parse_from(["test", "create", "myvol"]);
        assert!(cli.is_ok());

        let cli =
            TestCli::try_parse_from(["test", "create", "myvol", "--encryption", "server-side"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "create", "myvol", "--encryption", "none"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "create", "myvol", "--scrub-interval", "86400"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "create", "myvol", "--atime-mode", "relatime"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "create", "myvol", "--atime-mode", "noatime"]);
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

    #[test]
    fn test_volume_restore_parses_minimal_args() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: VolumeCommand,
        }

        let cli =
            TestCli::try_parse_from(["test", "restore", "vol-1", "--to", "snap-1"]).unwrap();
        match cli.command {
            VolumeCommand::Restore {
                volume,
                snapshot,
                safe,
                force,
                r#yes,
            } => {
                assert_eq!(volume, "vol-1");
                assert_eq!(snapshot, "snap-1");
                assert!(!safe);
                assert!(!force);
                assert!(!r#yes);
            }
            _ => panic!("expected Restore variant"),
        }
    }

    #[test]
    fn test_volume_restore_accepts_safe_flag() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: VolumeCommand,
        }

        let cli =
            TestCli::try_parse_from(["test", "restore", "v", "--to", "s", "--safe"]).unwrap();
        match cli.command {
            VolumeCommand::Restore { safe, force, .. } => {
                assert!(safe);
                assert!(!force);
            }
            _ => panic!("expected Restore variant"),
        }
    }

    #[test]
    fn test_volume_restore_force_requires_yes() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: VolumeCommand,
        }

        // `--force` without `--yes` is rejected by clap (requires).
        let result =
            TestCli::try_parse_from(["test", "restore", "v", "--to", "s", "--force"]);
        assert!(result.is_err());

        // `--force --yes` parses.
        let cli =
            TestCli::try_parse_from(["test", "restore", "v", "--to", "s", "--force", "--yes"])
                .unwrap();
        match cli.command {
            VolumeCommand::Restore {
                force, r#yes, safe, ..
            } => {
                assert!(force);
                assert!(r#yes);
                assert!(!safe);
            }
            _ => panic!("expected Restore variant"),
        }
    }

    #[test]
    fn test_volume_export_parses() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: VolumeCommand,
        }

        let cli =
            TestCli::try_parse_from(["test", "export", "vol-1", "--to", "/tmp/out.tar"]).unwrap();
        match cli.command {
            VolumeCommand::Export { volume, to } => {
                assert_eq!(volume, "vol-1");
                assert_eq!(to, "/tmp/out.tar");
            }
            _ => panic!("expected Export variant"),
        }
    }

    #[test]
    fn test_volume_export_requires_to_flag() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: VolumeCommand,
        }

        // Without `--to`, clap rejects.
        let result = TestCli::try_parse_from(["test", "export", "vol-1"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_volume_restore_safe_and_force_are_exclusive() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: VolumeCommand,
        }

        // Clap's `conflicts_with` rejects this combination.
        let result = TestCli::try_parse_from([
            "test", "restore", "v", "--to", "s", "--safe", "--force", "--yes",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: VolumeCommand,
        }

        // Basic update with single flag
        let cli = TestCli::try_parse_from(["test", "update", "myvol", "--compression", "zstd"]);
        assert!(cli.is_ok());

        // Multiple general flags
        let cli = TestCli::try_parse_from([
            "test",
            "update",
            "myvol",
            "--write-ack",
            "all",
            "--io-weight",
            "5",
            "--atime-mode",
            "noatime",
        ]);
        assert!(cli.is_ok());

        // Tiering flags
        let cli = TestCli::try_parse_from([
            "test",
            "update",
            "myvol",
            "--initial-tier",
            "warm",
            "--promotion-threshold",
            "10",
            "--demotion-delay",
            "24",
        ]);
        assert!(cli.is_ok());

        // Caching flags
        let cli = TestCli::try_parse_from([
            "test",
            "update",
            "myvol",
            "--cache-transformed",
            "false",
            "--cache-reconstructed",
            "true",
            "--cache-remote",
            "true",
        ]);
        assert!(cli.is_ok());

        // Verification flags
        let cli = TestCli::try_parse_from([
            "test",
            "update",
            "myvol",
            "--verify-on-read",
            "always",
            "--verify-sampling-rate",
            "0.5",
            "--scrub-interval",
            "86400",
        ]);
        assert!(cli.is_ok());

        // Metadata consistency flags
        let cli = TestCli::try_parse_from([
            "test",
            "update",
            "myvol",
            "--metadata-replicas",
            "5",
            "--read-quorum",
            "3",
            "--write-quorum",
            "3",
        ]);
        assert!(cli.is_ok());

        // No flags (should parse successfully — validation happens at execute time)
        let cli = TestCli::try_parse_from(["test", "update", "myvol"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_update_term_helpers() {
        // binary_key
        if let Term::Binary(b) = binary_key("compression") {
            assert_eq!(b.bytes, b"compression");
        } else {
            panic!("binary_key should produce Term::Binary");
        }

        // binary_val
        if let Term::Binary(b) = binary_val("zstd") {
            assert_eq!(b.bytes, b"zstd");
        } else {
            panic!("binary_val should produce Term::Binary");
        }

        // int_val
        if let Term::FixInteger(i) = int_val(42) {
            assert_eq!(i.value, 42);
        } else {
            panic!("int_val should produce Term::FixInteger");
        }

        // bool_val
        if let Term::Atom(a) = bool_val(true) {
            assert_eq!(a.name, "true");
        } else {
            panic!("bool_val(true) should produce Atom(true)");
        }
        if let Term::Atom(a) = bool_val(false) {
            assert_eq!(a.name, "false");
        } else {
            panic!("bool_val(false) should produce Atom(false)");
        }
    }
}
