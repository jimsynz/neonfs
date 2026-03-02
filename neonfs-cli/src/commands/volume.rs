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
        }
    }

    fn list(&self, all: bool, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

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
            entries: filter_entries,
        });

        let result = runtime.block_on(async {
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

        let mut config_entries = vec![
            (Term::Atom(Atom::from("durability")), durability_map),
            (Term::Atom(Atom::from("compression")), compression_map),
            (Term::Atom(Atom::from("encryption")), encryption_map),
        ];

        if let Some(interval) = scrub_interval {
            let verification_map = Term::Map(Map {
                entries: vec![(
                    Term::Atom(Atom::from("scrub_interval")),
                    Term::FixInteger(FixInteger::from(interval as i32)),
                )],
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
            entries: config_entries,
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

        let runtime = tokio::runtime::Runtime::new()?;

        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });
        let config_term = Term::Map(Map {
            entries: config_entries,
        });

        let result = runtime.block_on(async {
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
