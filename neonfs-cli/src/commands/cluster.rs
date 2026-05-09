//! Cluster management commands

use crate::commands::repair::RepairCommand;
use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::{
    CaInfo, CaRevokeResult, CertificateEntry, ClusterInitResult, ClusterStatus, RemoveNodeResult,
};
use crate::term::{
    extract_error, term_to_i64, term_to_list, term_to_map, term_to_string, unwrap_ok_tuple,
};
use clap::Subcommand;
use eetf::{Atom, Binary, FixInteger, List, Map, Term};
use std::path::PathBuf;

/// Cluster management subcommands
#[derive(Debug, Subcommand)]
pub enum ClusterCommand {
    /// Certificate authority management
    Ca {
        #[command(subcommand)]
        command: CaCommand,
    },

    /// Create an invite token for joining nodes
    CreateInvite {
        /// Token expiration duration (e.g., "1h", "30m", "3600")
        #[arg(long, default_value = "1h")]
        expires: String,
    },

    /// Initialize a new cluster
    Init {
        /// Cluster name
        #[arg(long)]
        name: String,
    },

    /// Join an existing cluster
    Join {
        /// Invite token from existing cluster
        #[arg(long)]
        token: String,

        /// Address of existing cluster member (host:port, e.g., node1:9568)
        #[arg(long)]
        via: String,
    },

    /// Rebalance storage across drives within each tier
    Rebalance {
        /// Only rebalance a specific tier (hot, warm, cold)
        #[arg(long)]
        tier: Option<String>,

        /// Balance tolerance (0.0-1.0, default: 0.10)
        #[arg(long, default_value = "0.10")]
        threshold: String,

        /// Chunks per migration batch
        #[arg(long, default_value = "50")]
        batch_size: String,
    },

    /// Show status of an active rebalance operation
    RebalanceStatus,

    /// Replica-count repair (#687)
    Repair {
        #[command(subcommand)]
        command: RepairCommand,
    },

    /// Show cluster status
    Status,

    /// Permanently decommission a node from the cluster
    ///
    /// Revokes the node's certificate and removes it from the Ra quorum
    /// membership. Refuses if the node is the current Ra leader or still
    /// owns drives (unless --force is passed).
    RemoveNode {
        /// Target node name (e.g. `neonfs_core@host2` or `host2`)
        node: String,

        /// Skip the drive-presence check. Force-removing a node with
        /// resident chunks risks losing any chunk whose only replica
        /// was on that node.
        #[arg(long)]
        force: bool,
    },

    /// Rebuild the Ra quorum from a surviving minority after catastrophic
    /// membership loss.
    ///
    /// This is a dangerous, last-resort operation. Every safety gate is
    /// evaluated before any mutation is attempted; a full audit entry is
    /// written once all gates pass. The Ra state mutation itself lands
    /// separately (see #473) — for now the command exits with a
    /// "not yet implemented" error after the audit entry is recorded.
    ForceReset {
        /// Surviving node to keep in the rebuilt quorum. Repeatable and
        /// comma-separated (`--keep a,b` or `--keep a --keep b`). At
        /// least one value is required. Must name a node currently in
        /// the Ra membership (e.g. `neonfs_core@host1` or `host1`).
        #[arg(long, required = true, value_delimiter = ',')]
        keep: Vec<String>,

        /// Minimum time a departed member must have been unreachable
        /// before force-reset will accept it as gone (default 1800 = 30m).
        /// Lower values are intended for tests only — in production the
        /// grace window is the safety wall against a healing partition.
        #[arg(long, default_value = "1800")]
        min_unreachable_seconds: u64,

        /// Required acknowledgement that force-reset can drop committed
        /// writes on the surviving minority. Refuses locally if absent.
        #[arg(long = "yes-i-accept-data-loss")]
        yes_i_accept_data_loss: bool,
    },

    /// Rebuild the bootstrap-layer Ra state from on-disk volume data
    /// (per-volume metadata epic, #788).
    ///
    /// Use this when Ra logs are unrecoverable but the underlying
    /// volume data (drive identity files + root segment chunks) is
    /// intact. Walks every configured drive's `blobs/` tree, decodes
    /// candidate chunks as root segments, and submits the matching
    /// `:register_drive` / `:register_volume_root` Ra commands.
    ///
    /// Last-resort operation. Refuses without `--yes` and refuses if
    /// the bootstrap layer already has volumes registered (use
    /// `--overwrite-ra-state` to force, or `--dry-run` to preview).
    ReconstructFromDisk {
        /// Required acknowledgement that this is the right call.
        /// Refuses locally if absent. Bypassed by `--dry-run`.
        #[arg(long)]
        yes: bool,

        /// Allow reconstruction when the bootstrap layer's
        /// `volume_roots` table is non-empty. Without this flag, a
        /// reconstruction misfire on a healthy cluster is bounded.
        #[arg(long = "overwrite-ra-state")]
        overwrite_ra_state: bool,

        /// Preview the discovered drives + commands without
        /// submitting anything to Ra. Doesn't require `--yes`.
        #[arg(long = "dry-run")]
        dry_run: bool,
    },
}

/// Certificate authority subcommands
#[derive(Debug, Subcommand)]
pub enum CaCommand {
    /// Display CA information (subject, algorithm, validity, serial counter)
    Info,

    /// List all issued node certificates
    List,

    /// Revoke a node's certificate
    Revoke {
        /// Node name whose certificate to revoke
        node: String,
    },

    /// Drive the cluster CA rotation lifecycle.
    ///
    /// With no flags: runs the full orchestrator (#926) — stage a new
    /// incoming CA, walk the cluster reissuing every node's cert, and
    /// distribute the dual-CA bundle. Without `--no-wait`, the
    /// rotation **stops** before finalizing so the dual-CA grace
    /// window can elapse. Operators run `--finalize` after the window.
    ///
    /// Mode flags (mutually exclusive):
    ///
    /// - `--status`   inspect rotation state (read-only).
    /// - `--stage`    init a fresh incoming CA into `_system/tls/incoming/`.
    /// - `--abort`    discard the staged incoming CA.
    /// - `--finalize` promote staged → active, drop the previous active CA.
    /// - `--node <n>` retry the rolling reissue for a single node
    ///   after a per-node failure (see `--abort` to bail).
    Rotate {
        /// Inspect the current rotation state.
        #[arg(long, conflicts_with_all = ["stage", "abort", "finalize", "node"])]
        status: bool,

        /// Stage a new incoming CA without touching the active CA.
        #[arg(long, conflicts_with_all = ["status", "abort", "finalize", "node"])]
        stage: bool,

        /// Discard a staged incoming CA.
        #[arg(long, conflicts_with_all = ["status", "stage", "finalize", "node"])]
        abort: bool,

        /// Promote the staged incoming CA to active. Drops the previous
        /// active CA and invalidates every cert it signed.
        #[arg(long, conflicts_with_all = ["status", "stage", "abort", "node"])]
        finalize: bool,

        /// Retry the rolling reissue for a single node — used after a
        /// per-node failure in the default-mode rotation. Operates
        /// against the currently-staged incoming CA; no-op if no
        /// rotation is staged.
        #[arg(long, conflicts_with_all = ["status", "stage", "abort", "finalize"])]
        node: Option<String>,

        /// Skip the dual-CA grace window before finalizing. Default
        /// mode normally stops after bundle distribution so operators
        /// can wait `--grace-window-seconds` before finalizing; this
        /// flag finalizes immediately. For tests / automation.
        #[arg(long, conflicts_with_all = ["status", "stage", "abort", "finalize", "node"])]
        no_wait: bool,

        /// Recommended grace window between bundle distribution and
        /// finalize, in seconds. Default 86400 (24h). Echoed back to
        /// the operator in the post-distribution message; the daemon
        /// doesn't actually sleep — operators run `--finalize` after
        /// waiting.
        #[arg(long, default_value = "86400", conflicts_with_all = ["status", "stage", "abort", "finalize", "node"])]
        grace_window_seconds: u64,
    },

    /// Restore CA material on a single node after the cluster CA has expired.
    ///
    /// Runs locally — the `neonfs-core` service on this host must be
    /// stopped. Either restore from an off-cluster CA backup tarball
    /// (`--from-backup`) or generate a fresh CA (`--new-key`, which
    /// invalidates every cached cert signed by the outgoing CA).
    ///
    /// This slice (see issue #502) establishes the CLI surface plus the
    /// flag-level safety gates — mutually exclusive source selection and
    /// the `--yes-i-accept-data-loss` acknowledgement. The live-service
    /// check, tarball structural validation, foreign-CA refusal, audit-log
    /// entry, and actual CA install + node cert regeneration ship in
    /// follow-ups (#503, #504).
    EmergencyBootstrap {
        /// Path to an off-cluster CA backup tarball. Mutually exclusive
        /// with `--new-key`.
        #[arg(long = "from-backup", value_name = "PATH")]
        from_backup: Option<PathBuf>,

        /// Generate a fresh cluster CA. Every existing node and client
        /// cert becomes invalid — holders must re-trust the new CA.
        /// Requires `--yes-i-accept-data-loss`. Mutually exclusive with
        /// `--from-backup`.
        #[arg(long = "new-key")]
        new_key: bool,

        /// Operator acknowledgement of the data-loss risk. Required when
        /// `--new-key` is used. Accepted (but not required) when
        /// `--from-backup` is used.
        #[arg(long = "yes-i-accept-data-loss")]
        yes_i_accept_data_loss: bool,
    },
}

impl ClusterCommand {
    /// Execute the cluster command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            ClusterCommand::Ca { command } => command.execute(format),
            ClusterCommand::CreateInvite { expires } => self.create_invite(expires, format),
            ClusterCommand::Init { name } => self.init(name, format),
            ClusterCommand::Join { token, via } => self.join(token, via, format),
            ClusterCommand::Rebalance {
                tier,
                threshold,
                batch_size,
            } => self.rebalance(tier.as_deref(), threshold, batch_size, format),
            ClusterCommand::RebalanceStatus => self.rebalance_status(format),
            ClusterCommand::Repair { command } => command.execute(format),
            ClusterCommand::Status => self.status(format),
            ClusterCommand::RemoveNode { node, force } => self.remove_node(node, *force, format),
            ClusterCommand::ReconstructFromDisk {
                yes,
                overwrite_ra_state,
                dry_run,
            } => self.reconstruct_from_disk(*yes, *overwrite_ra_state, *dry_run, format),
            ClusterCommand::ForceReset {
                keep,
                min_unreachable_seconds,
                yes_i_accept_data_loss,
            } => self.force_reset(
                keep,
                *min_unreachable_seconds,
                *yes_i_accept_data_loss,
                format,
            ),
        }
    }

    fn init(&self, name: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            let name_binary = Binary::from(name.as_bytes().to_vec());
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "cluster_init",
                vec![Term::Binary(name_binary)],
            )
            .await
        })?;

        // Check for error response
        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        // Unwrap {:ok, value} tuple
        let data = unwrap_ok_tuple(result)?;

        // Parse response
        let init_result = ClusterInitResult::from_term(data)?;

        // Format output
        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&init_result)?);
            }
            OutputFormat::Table => {
                println!("✓ Initialized cluster '{}'", init_result.cluster_name);
                println!();
                println!("  Cluster ID:  {}", init_result.cluster_id);
                println!("  Node ID:     {}", init_result.node_id);
                println!("  Node Name:   {}", init_result.node_name);
                println!("  Created:     {}", init_result.created_at);
                println!();
                println!("Create invite tokens with: neonfs cluster create-invite");
            }
        }
        Ok(())
    }

    fn status(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "cluster_status", vec![])
                .await
        })?;

        // Check for error response
        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        // Unwrap {:ok, value} tuple
        let data = unwrap_ok_tuple(result)?;

        // Parse response
        let status = ClusterStatus::from_term(data)?;

        // Format output
        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&status)?);
            }
            OutputFormat::Table => {
                let mut tbl = table::Table::new(vec!["Property".to_string(), "Value".to_string()]);
                tbl.add_row(vec!["Cluster".to_string(), status.name.clone()]);
                tbl.add_row(vec!["Node".to_string(), status.node.clone()]);
                tbl.add_row(vec!["Status".to_string(), status.status.clone()]);
                tbl.add_row(vec!["Volumes".to_string(), status.volumes.to_string()]);
                tbl.add_row(vec!["Uptime".to_string(), status.uptime_string()]);
                print!("{}", tbl.render()?);
            }
        }
        Ok(())
    }

    fn create_invite(&self, expires: &str, format: OutputFormat) -> Result<()> {
        // Parse expiration duration to seconds
        let expires_in = parse_duration(expires)?;

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "create_invite",
                vec![Term::FixInteger(FixInteger {
                    value: expires_in as i32,
                })],
            )
            .await
        })?;

        // Check for error response
        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        // Unwrap {:ok, value} tuple
        let data = unwrap_ok_tuple(result)?;

        // Extract token from map
        let token = extract_token(&data)?;

        // Format output
        match format {
            OutputFormat::Json => {
                let result = serde_json::json!({
                    "token": token,
                    "expires_in": expires_in,
                });
                println!("{}", json::format(&result)?);
            }
            OutputFormat::Table => {
                println!("✓ Created invite token (expires in {})", expires);
                println!();
                println!("  Token: {}", token);
                println!();
                println!("Use this token on a new node with:");
                println!("  neonfs cluster join --token <TOKEN> --via <NODE_NAME>");
            }
        }
        Ok(())
    }

    fn join(&self, token: &str, via: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            let token_binary = Binary::from(token.as_bytes().to_vec());
            let via_binary = Binary::from(via.as_bytes().to_vec());
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "join_cluster",
                vec![Term::Binary(token_binary), Term::Binary(via_binary)],
            )
            .await
        })?;

        // Check for error response
        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        // Unwrap {:ok, value} tuple
        let data = unwrap_ok_tuple(result)?;

        // Parse response
        let join_result = ClusterInitResult::from_term(data)?;

        // Format output
        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&join_result)?);
            }
            OutputFormat::Table => {
                println!(
                    "✓ Successfully joined cluster '{}'",
                    join_result.cluster_name
                );
                println!();
                println!("  Cluster ID:  {}", join_result.cluster_id);
                println!("  Node ID:     {}", join_result.node_id);
                println!("  Node Name:   {}", join_result.node_name);
                println!();
            }
        }
        Ok(())
    }

    fn rebalance(
        &self,
        tier: Option<&str>,
        threshold: &str,
        batch_size: &str,
        format: OutputFormat,
    ) -> Result<()> {
        let mut opts_entries = vec![
            (
                Term::Binary(Binary {
                    bytes: b"threshold".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: threshold.as_bytes().to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"batch_size".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: batch_size.as_bytes().to_vec(),
                }),
            ),
        ];

        if let Some(tier_str) = tier {
            if !["hot", "warm", "cold"].contains(&tier_str) {
                return Err(crate::error::CliError::InvalidArgument(format!(
                    "Invalid tier '{}'. Valid: hot, warm, cold",
                    tier_str
                )));
            }
            opts_entries.push((
                Term::Binary(Binary {
                    bytes: b"tier".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: tier_str.as_bytes().to_vec(),
                }),
            ));
        }

        let opts_term = Term::Map(Map {
            map: opts_entries.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_rebalance",
                vec![opts_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            match err.error_message().as_str() {
                "already_balanced" => {
                    eprintln!("Cluster is already balanced within the configured threshold.");
                }
                "rebalance_already_running" => {
                    eprintln!("A rebalance operation is already in progress.");
                    eprintln!("Use 'neonfs cluster rebalance-status' to check progress.");
                }
                "insufficient_drives" => {
                    eprintln!(
                        "Not enough eligible drives to rebalance (need at least 2 per tier)."
                    );
                }
                "no_drives" => {
                    eprintln!("No drives configured in the cluster.");
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
        let description = job_map
            .get("progress_description")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "started",
                    "job_id": job_id,
                    "threshold": threshold,
                    "batch_size": batch_size,
                    "estimated_chunks": total
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("Rebalance started");
                println!("  Job ID: {}", job_id);
                println!("  {}", description);
                println!("  Threshold: {}", threshold);
                println!("  Batch size: {}", batch_size);
                println!("  Estimated chunks: {}", total);
                println!("\nTrack progress with: neonfs job show {}", job_id);
            }
        }
        Ok(())
    }

    fn rebalance_status(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_rebalance_status",
                vec![],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            if err.error_message() == "no_rebalance" {
                eprintln!("No rebalance operation found.");
            }
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let status_map = term_to_map(&data)?;

        let job_id = status_map
            .get("job_id")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();
        let status = status_map
            .get("status")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();
        let total = status_map
            .get("progress_total")
            .and_then(extract_integer)
            .unwrap_or(0);
        let completed = status_map
            .get("progress_completed")
            .and_then(extract_integer)
            .unwrap_or(0);
        let description = status_map
            .get("progress_description")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "job_id": job_id,
                    "status": status,
                    "progress_total": total,
                    "progress_completed": completed,
                    "progress_description": description
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                let mut tbl = table::Table::new(vec!["Property".to_string(), "Value".to_string()]);
                tbl.add_row(vec!["Job ID".to_string(), job_id]);
                tbl.add_row(vec!["Status".to_string(), status]);
                tbl.add_row(vec![
                    "Progress".to_string(),
                    format!("{}/{}", completed, total),
                ]);
                tbl.add_row(vec!["Description".to_string(), description]);
                print!("{}", tbl.render()?);
            }
        }
        Ok(())
    }

    fn remove_node(&self, node: &str, force: bool, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            let node_binary = Binary::from(node.as_bytes().to_vec());
            let force_key = Binary::from(b"force".to_vec());
            let force_value = Term::Atom(eetf::Atom::from(if force { "true" } else { "false" }));
            let opts = Term::Map(Map::from([(Term::Binary(force_key), force_value)]));
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_remove_node",
                vec![Term::Binary(node_binary), opts],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let remove_result = RemoveNodeResult::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&remove_result)?);
            }
            OutputFormat::Table => {
                println!("✓ Removed node '{}'", remove_result.node);
                println!();
                println!("  Status:              {}", remove_result.status);
                println!("  Remaining members:   {}", remove_result.remaining_members);
                println!(
                    "  Certificate revoked: {}",
                    if remove_result.certificate_revoked {
                        "yes"
                    } else {
                        "no"
                    }
                );
            }
        }
        Ok(())
    }

    fn force_reset(
        &self,
        keep: &[String],
        min_unreachable_seconds: u64,
        yes_i_accept_data_loss: bool,
        format: OutputFormat,
    ) -> Result<()> {
        if keep.is_empty() {
            return Err(crate::error::CliError::InvalidArgument(
                "--keep must name at least one surviving node".to_string(),
            ));
        }

        if !yes_i_accept_data_loss {
            return Err(crate::error::CliError::InvalidArgument(
                "--yes-i-accept-data-loss is required. Force-reset can drop committed writes on the surviving minority.".to_string(),
            ));
        }

        let keep_list = Term::List(List {
            elements: keep
                .iter()
                .map(|name| {
                    Term::Binary(Binary {
                        bytes: name.as_bytes().to_vec(),
                    })
                })
                .collect(),
        });

        let min_unreachable_term = Term::FixInteger(FixInteger {
            value: min_unreachable_seconds as i32,
        });

        let opts = Term::Map(Map::from([
            (
                Term::Binary(Binary {
                    bytes: b"keep".to_vec(),
                }),
                keep_list,
            ),
            (
                Term::Binary(Binary {
                    bytes: b"min_unreachable_seconds".to_vec(),
                }),
                min_unreachable_term,
            ),
            (
                Term::Binary(Binary {
                    bytes: b"yes_i_accept_data_loss".to_vec(),
                }),
                Term::Atom(Atom::from("true")),
            ),
        ]));

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_force_reset",
                vec![opts],
            )
            .await
        })?;

        // `handle_force_reset/1` always returns `{:error, _}` in this
        // slice — either a safety-gate refusal or the "Ra mutation not
        // yet implemented" placeholder. Surface both via the standard
        // error plumbing; the error message distinguishes them.
        if let Some(err) = extract_error(&result) {
            match format {
                OutputFormat::Json => {
                    let payload = serde_json::json!({
                        "status": "refused",
                        "message": err.error_message(),
                    });
                    eprintln!("{}", json::format(&payload)?);
                }
                OutputFormat::Table => {
                    eprintln!(
                        "neonfs cluster force-reset refused: {}",
                        err.error_message()
                    );
                }
            }
            return Err(err);
        }

        // Should not happen in this slice, but be defensive — the Elixir
        // handler shape may change when #473 lands the actual mutation.
        match format {
            OutputFormat::Json => {
                let payload = serde_json::json!({
                    "status": "accepted",
                });
                println!("{}", json::format(&payload)?);
            }
            OutputFormat::Table => {
                println!("neonfs cluster force-reset: accepted");
            }
        }

        Ok(())
    }

    fn reconstruct_from_disk(
        &self,
        yes: bool,
        overwrite_ra_state: bool,
        dry_run: bool,
        format: OutputFormat,
    ) -> Result<()> {
        if !yes && !dry_run {
            return Err(crate::error::CliError::InvalidArgument(
                "--yes is required (or --dry-run to preview). \
                 Reconstruction overwrites the bootstrap layer's Ra state \
                 from on-disk volume data."
                    .to_string(),
            ));
        }

        let opts = Term::Map(Map::from([
            (
                Term::Binary(Binary {
                    bytes: b"yes".to_vec(),
                }),
                Term::Atom(Atom::from(if yes { "true" } else { "false" })),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"overwrite_ra_state".to_vec(),
                }),
                Term::Atom(Atom::from(if overwrite_ra_state {
                    "true"
                } else {
                    "false"
                })),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"dry_run".to_vec(),
                }),
                Term::Atom(Atom::from(if dry_run { "true" } else { "false" })),
            ),
        ]));

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_cluster_reconstruct_from_disk",
                vec![opts],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            match format {
                OutputFormat::Json => {
                    let payload = serde_json::json!({
                        "status": "refused",
                        "message": err.error_message(),
                    });
                    eprintln!("{}", json::format(&payload)?);
                }
                OutputFormat::Table => {
                    eprintln!(
                        "neonfs cluster reconstruct-from-disk refused: {}",
                        err.error_message()
                    );
                }
            }
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let summary = term_to_map(&data)?;

        let drives = summary
            .get("drives")
            .and_then(|t| term_to_i64(t).ok())
            .unwrap_or(0);
        let volumes = summary
            .get("volumes")
            .and_then(|t| term_to_i64(t).ok())
            .unwrap_or(0);
        let commands = summary
            .get("commands")
            .and_then(|t| term_to_i64(t).ok())
            .unwrap_or(0);
        let submitted = summary
            .get("commands_submitted")
            .and_then(|t| term_to_i64(t).ok())
            .unwrap_or(0);
        let failed = summary
            .get("commands_failed")
            .and_then(|t| term_to_list(t).ok())
            .map(|v| v.len() as i64)
            .unwrap_or(0);
        let warnings = summary
            .get("warnings")
            .and_then(|t| term_to_list(t).ok())
            .map(|v| v.len() as i64)
            .unwrap_or(0);

        match format {
            OutputFormat::Json => {
                let payload = serde_json::json!({
                    "status": if dry_run { "preview" } else { "applied" },
                    "drives": drives,
                    "volumes": volumes,
                    "commands": commands,
                    "commands_submitted": submitted,
                    "commands_failed": failed,
                    "warnings": warnings,
                    "dry_run": dry_run,
                });
                println!("{}", json::format(&payload)?);
            }
            OutputFormat::Table => {
                let status_label = if dry_run { "preview" } else { "applied" };
                println!("neonfs cluster reconstruct-from-disk: {}", status_label);
                println!();
                println!("  Drives discovered:     {}", drives);
                println!("  Volumes recovered:     {}", volumes);
                println!("  Ra commands generated: {}", commands);

                if !dry_run {
                    println!("  Commands submitted:    {}", submitted);
                    println!("  Commands failed:       {}", failed);
                }

                if warnings > 0 {
                    println!("  Warnings:              {}", warnings);
                }
            }
        }

        Ok(())
    }
}

impl CaCommand {
    /// Execute the CA subcommand
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            CaCommand::Info => self.info(format),
            CaCommand::List => self.list(format),
            CaCommand::Revoke { node } => self.revoke(node, format),
            CaCommand::Rotate {
                status,
                stage,
                abort,
                finalize,
                node,
                no_wait,
                grace_window_seconds,
            } => self.rotate(
                *status,
                *stage,
                *abort,
                *finalize,
                node.as_deref(),
                *no_wait,
                *grace_window_seconds,
                format,
            ),
            CaCommand::EmergencyBootstrap {
                from_backup,
                new_key,
                yes_i_accept_data_loss,
            } => self.emergency_bootstrap(
                from_backup.as_deref(),
                *new_key,
                *yes_i_accept_data_loss,
                format,
            ),
        }
    }

    fn info(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "handle_ca_info", vec![])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(format_ca_err(err));
        }

        let data = unwrap_ok_tuple(result)?;
        let info = CaInfo::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&info)?);
            }
            OutputFormat::Table => {
                let mut tbl = table::Table::new(vec!["Property".to_string(), "Value".to_string()]);
                tbl.add_row(vec!["Subject".to_string(), info.subject.clone()]);
                tbl.add_row(vec!["Algorithm".to_string(), info.algorithm.clone()]);
                tbl.add_row(vec!["Valid From".to_string(), info.valid_from.clone()]);
                tbl.add_row(vec!["Valid To".to_string(), info.valid_to.clone()]);
                tbl.add_row(vec![
                    "Current Serial".to_string(),
                    info.current_serial.to_string(),
                ]);
                tbl.add_row(vec![
                    "Nodes Issued".to_string(),
                    info.nodes_issued.to_string(),
                ]);
                print!("{}", tbl.render()?);
            }
        }
        Ok(())
    }

    fn list(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "handle_ca_list", vec![])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(format_ca_err(err));
        }

        let data = unwrap_ok_tuple(result)?;
        let entries = term_to_list(&data)?;

        let certs: Vec<CertificateEntry> = entries
            .into_iter()
            .map(CertificateEntry::from_term)
            .collect::<Result<Vec<_>>>()?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&certs)?);
            }
            OutputFormat::Table => {
                if certs.is_empty() {
                    println!("No certificates issued yet.");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "Node".to_string(),
                        "Hostname".to_string(),
                        "Serial".to_string(),
                        "Expires".to_string(),
                        "Status".to_string(),
                    ]);
                    for cert in &certs {
                        tbl.add_row(vec![
                            cert.node_name.clone(),
                            cert.hostname.clone(),
                            cert.serial.to_string(),
                            cert.expires.clone(),
                            cert.status.clone(),
                        ]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }

    fn revoke(&self, node: &str, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            let node_binary = Binary::from(node.as_bytes().to_vec());
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_ca_revoke",
                vec![Term::Binary(node_binary)],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(format_ca_err(err));
        }

        let data = unwrap_ok_tuple(result)?;
        let revoke_result = CaRevokeResult::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&revoke_result)?);
            }
            OutputFormat::Table => {
                println!(
                    "✓ Revoked certificate for '{}' (serial {})",
                    revoke_result.node_name, revoke_result.serial
                );
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn rotate(
        &self,
        status: bool,
        stage: bool,
        abort: bool,
        finalize: bool,
        node: Option<&str>,
        no_wait: bool,
        grace_window_seconds: u64,
        format: OutputFormat,
    ) -> Result<()> {
        let mode_key = match (status, stage, abort, finalize, node) {
            (true, false, false, false, None) => "status",
            (false, true, false, false, None) => "stage",
            (false, false, true, false, None) => "abort",
            (false, false, false, true, None) => "finalize",
            (false, false, false, false, Some(_)) => "node",
            (false, false, false, false, None) => "default",
            // clap's `conflicts_with_all` rejects combinations at parse time.
            _ => unreachable!("clap should have rejected combined mode flags"),
        };

        let mut entries: std::collections::HashMap<Term, Term> = std::collections::HashMap::new();

        entries.insert(
            Term::Binary(Binary {
                bytes: mode_key.as_bytes().to_vec(),
            }),
            Term::Atom(Atom::from("true")),
        );

        if let Some(node_name) = node {
            entries.insert(
                Term::Binary(Binary {
                    bytes: b"node".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: node_name.as_bytes().to_vec(),
                }),
            );
        }

        if no_wait {
            entries.insert(
                Term::Binary(Binary {
                    bytes: b"no-wait".to_vec(),
                }),
                Term::Atom(Atom::from("true")),
            );
        }

        entries.insert(
            Term::Binary(Binary {
                bytes: b"grace-window-seconds".to_vec(),
            }),
            Term::FixInteger(FixInteger {
                value: grace_window_seconds as i32,
            }),
        );

        let opts = Term::Map(Map::from(entries));

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "handle_ca_rotate", vec![opts])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            match format {
                OutputFormat::Json => {
                    let payload = serde_json::json!({
                        "status": "error",
                        "mode": mode_key,
                        "message": err.error_message(),
                    });
                    eprintln!("{}", json::format(&payload)?);
                }
                OutputFormat::Table => {
                    eprintln!(
                        "neonfs cluster ca rotate --{}: {}",
                        mode_key,
                        err.error_message()
                    );
                }
            }
            return Err(err);
        }

        format_rotate_success(mode_key, &result, format)
    }

    fn emergency_bootstrap(
        &self,
        from_backup: Option<&std::path::Path>,
        new_key: bool,
        yes_i_accept_data_loss: bool,
        format: OutputFormat,
    ) -> Result<()> {
        let source =
            validate_emergency_bootstrap_flags(from_backup, new_key, yes_i_accept_data_loss)?;

        // Live-service refusal: emergency-bootstrap must run against a
        // stopped service. If the local `neonfs-core` daemon is running,
        // the install would race its in-memory CA material and the
        // daemon would continue serving with the stale chain. Detects
        // the daemon via a TCP probe on its distribution port.
        crate::commands::ca_bootstrap::refuse_if_daemon_live()?;

        // Safety-gate + install + node-cert-regen layer. Both paths
        // produce a `BackupValidation` (real tarball for `--from-backup`,
        // freshly-minted in-memory CA for `--new-key`) and feed it
        // through the same `install_ca_material` + `regenerate_node_cert`
        // pipeline.
        let tls_dir = crate::tls::tls_dir();

        // Capture the OUTGOING CA fingerprint *before* the install
        // overwrites `$tls_dir/ca.crt`. `None` when the operator is
        // bootstrapping from a tls_dir that has no prior CA on disk
        // — that is a legitimate first-recovery state.
        let old_ca_fingerprint = crate::commands::ca_bootstrap::read_ca_fingerprint(&tls_dir);

        let (installed_cluster, audit_source) = match &source {
            EmergencyBootstrapSource::Backup(path) => {
                let validation = crate::commands::ca_bootstrap::validate_backup_tarball(path)?;
                let local_name = crate::commands::ca_bootstrap::local_cluster_name()?;
                let validation =
                    crate::commands::ca_bootstrap::refuse_foreign_backup(validation, &local_name)?;
                crate::commands::ca_bootstrap::install_ca_material(&validation, &tls_dir)?;
                crate::commands::ca_bootstrap::regenerate_node_cert(&tls_dir)?;
                let audit_source =
                    crate::commands::ca_bootstrap::AuditSource::Backup(path.display().to_string());
                (validation.cluster_name, audit_source)
            }
            EmergencyBootstrapSource::NewKey => {
                let local_name = crate::commands::ca_bootstrap::local_cluster_name()?;
                let existing_serial =
                    crate::commands::ca_bootstrap::read_installed_serial(&tls_dir).unwrap_or(0);
                let validation = crate::commands::ca_bootstrap::generate_new_ca_material(
                    &local_name,
                    existing_serial,
                )?;
                crate::commands::ca_bootstrap::install_ca_material(&validation, &tls_dir)?;
                crate::commands::ca_bootstrap::regenerate_node_cert(&tls_dir)?;
                (
                    validation.cluster_name,
                    crate::commands::ca_bootstrap::AuditSource::NewKey,
                )
            }
        };

        // Emit the audit-log entry. Best-effort — install already
        // succeeded; logging failure is reported but does not roll the
        // install back. `cluster_id` and the new CA fingerprint are
        // read from the freshly-installed material on disk.
        if let Err(e) = emit_audit_log_entry(&tls_dir, &audit_source, old_ca_fingerprint.as_deref())
        {
            eprintln!(
                "warning: cluster ca emergency-bootstrap install succeeded but audit-log write failed: {e}. \
                 The CA + node cert are on disk; restart `neonfs-core` to pick up the new chain."
            );
        }

        let description = source.description();
        let message = format!(
            "cluster ca emergency-bootstrap: CA material installed and node cert regenerated ({description} — \
             cluster `{installed_cluster}`). Next step: restart `neonfs-core` to pick up the new chain."
        );

        match format {
            OutputFormat::Json => {
                let payload = serde_json::json!({
                    "status": "installed",
                    "source": description,
                    "installed_cluster": installed_cluster,
                    "message": message,
                });
                eprintln!("{}", json::format(&payload)?);
            }
            OutputFormat::Table => {
                eprintln!("neonfs cluster ca emergency-bootstrap: CA + node cert installed");
                eprintln!();
                eprintln!("  cluster: {installed_cluster}");
                eprintln!("  source:  {description}");
                eprintln!();
                eprintln!("Next step: restart `neonfs-core` to pick up the new chain.");
            }
        }

        Ok(())
    }
}

/// Glue between the install pipeline and the audit-log writer:
/// reads the freshly-installed `ca.crt` to compute the new fingerprint,
/// reads `cluster_id` from `cluster.json`, captures operator UID and
/// timestamp, and appends a JSONL line via
/// [`crate::commands::ca_bootstrap::write_audit_log_entry`].
fn emit_audit_log_entry(
    tls_dir: &std::path::Path,
    audit_source: &crate::commands::ca_bootstrap::AuditSource,
    old_ca_fingerprint: Option<&str>,
) -> Result<()> {
    let new_ca_fingerprint = crate::commands::ca_bootstrap::read_ca_fingerprint(tls_dir)
        .ok_or_else(|| {
            crate::error::CliError::InvalidArgument(format!(
                "cannot compute fingerprint of newly-installed CA at {}",
                tls_dir.join("ca.crt").display()
            ))
        })?;

    let cluster_id = crate::commands::ca_bootstrap::local_cluster_id()?;
    let data_dir = crate::commands::ca_bootstrap::data_dir();
    let operator_uid = crate::commands::ca_bootstrap::operator_uid();
    let timestamp = crate::commands::ca_bootstrap::audit_timestamp();

    crate::commands::ca_bootstrap::write_audit_log_entry(
        &data_dir,
        &cluster_id,
        audit_source.clone(),
        old_ca_fingerprint,
        &new_ca_fingerprint,
        operator_uid,
        &timestamp,
    )
}

/// Source of CA material for emergency-bootstrap, resolved from the CLI flags.
#[derive(Debug, PartialEq, Eq)]
enum EmergencyBootstrapSource<'a> {
    /// Restore from an off-cluster CA backup tarball at this path.
    Backup(&'a std::path::Path),
    /// Generate a fresh CA in place. Destroys the existing trust chain.
    NewKey,
}

impl<'a> EmergencyBootstrapSource<'a> {
    fn description(&self) -> String {
        match self {
            Self::Backup(path) => format!("backup tarball at {}", path.display()),
            Self::NewKey => "fresh CA (--new-key)".to_string(),
        }
    }
}

/// Flag-shape safety gates for `cluster ca emergency-bootstrap`.
///
/// Enforces:
///   1. Exactly one of `--from-backup` / `--new-key` is set.
///   2. `--yes-i-accept-data-loss` is present when `--new-key` is used.
///
/// Returns the resolved source on success. Downstream checks (live
/// daemon refusal, tarball validation, foreign-CA match) are intentionally
/// out of scope for this slice — see #503.
fn validate_emergency_bootstrap_flags(
    from_backup: Option<&std::path::Path>,
    new_key: bool,
    yes_i_accept_data_loss: bool,
) -> Result<EmergencyBootstrapSource<'_>> {
    use crate::error::CliError;

    match (from_backup, new_key) {
        (None, false) => Err(CliError::InvalidArgument(
            "one of --from-backup <path> or --new-key must be given. \
             --from-backup restores the CA from an off-cluster backup tarball; \
             --new-key generates a fresh CA (destroys the existing trust chain)."
                .to_string(),
        )),
        (Some(_), true) => Err(CliError::InvalidArgument(
            "--from-backup and --new-key are mutually exclusive. \
             Pick one: restore from backup, or generate fresh CA material."
                .to_string(),
        )),
        (None, true) if !yes_i_accept_data_loss => Err(CliError::InvalidArgument(
            "--new-key requires --yes-i-accept-data-loss. Generating a fresh CA \
             invalidates every cert signed by the outgoing CA — every peer and \
             client holding a cached cert must re-trust the new CA. Re-run with \
             the flag once you have accepted the trust-chain reset."
                .to_string(),
        )),
        (None, true) => Ok(EmergencyBootstrapSource::NewKey),
        (Some(path), false) => Ok(EmergencyBootstrapSource::Backup(path)),
    }
}

#[cfg(test)]
mod emergency_bootstrap_tests {
    use super::{validate_emergency_bootstrap_flags, EmergencyBootstrapSource};
    use crate::error::CliError;
    use std::path::Path;

    #[test]
    fn rejects_no_source() {
        let err = validate_emergency_bootstrap_flags(None, false, false).unwrap_err();
        match err {
            CliError::InvalidArgument(msg) => {
                assert!(
                    msg.contains("--from-backup") && msg.contains("--new-key"),
                    "error should name both source options, got: {msg}"
                );
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn rejects_both_sources() {
        let err = validate_emergency_bootstrap_flags(Some(Path::new("/tmp/ca.tar.gz")), true, true)
            .unwrap_err();
        match err {
            CliError::InvalidArgument(msg) => {
                assert!(msg.contains("mutually exclusive"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn rejects_new_key_without_data_loss_ack() {
        let err = validate_emergency_bootstrap_flags(None, true, false).unwrap_err();
        match err {
            CliError::InvalidArgument(msg) => {
                assert!(msg.contains("--yes-i-accept-data-loss"));
                assert!(msg.contains("--new-key"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn accepts_new_key_with_data_loss_ack() {
        let source = validate_emergency_bootstrap_flags(None, true, true).unwrap();
        assert_eq!(source, EmergencyBootstrapSource::NewKey);
    }

    #[test]
    fn accepts_from_backup_without_ack() {
        let path = Path::new("/tmp/ca-backup.tar.gz");
        let source = validate_emergency_bootstrap_flags(Some(path), false, false).unwrap();
        assert_eq!(source, EmergencyBootstrapSource::Backup(path));
    }

    #[test]
    fn accepts_from_backup_with_ack() {
        // The acknowledgement is allowed with --from-backup (belt-and-braces)
        // even though it is not required — an operator can choose to always
        // pass it to make the audit trail explicit.
        let path = Path::new("/tmp/ca-backup.tar.gz");
        let source = validate_emergency_bootstrap_flags(Some(path), false, true).unwrap();
        assert_eq!(source, EmergencyBootstrapSource::Backup(path));
    }

    #[test]
    fn description_identifies_backup_path() {
        let path = Path::new("/var/lib/neonfs/ca-backup-2026.tar.gz");
        let source = EmergencyBootstrapSource::Backup(path);
        let desc = source.description();
        assert!(desc.contains("backup tarball"));
        assert!(desc.contains("ca-backup-2026.tar.gz"));
    }

    #[test]
    fn description_identifies_new_key() {
        let desc = EmergencyBootstrapSource::NewKey.description();
        assert!(desc.contains("fresh CA"));
        assert!(desc.contains("--new-key"));
    }
}

/// Enrich CA-specific errors with more helpful messages.
///
/// Structured errors (NeonfsError) pass through unchanged — the handler
/// already provides descriptive messages. Legacy atom-based errors get
/// rewritten to user-friendly text.
fn format_ca_err(err: crate::error::CliError) -> crate::error::CliError {
    use crate::error::CliError;
    match err {
        CliError::NeonfsError { .. } => err,
        CliError::RpcError(ref msg) => {
            let enriched = match msg.as_str() {
                "ca_not_initialized" => "CA not initialised. Run 'neonfs cluster init' to create the cluster CA.".to_string(),
                "node_not_found" => "No certificate found for the specified node. Use 'neonfs cluster ca list' to see issued certificates.".to_string(),
                _ => return err,
            };
            CliError::RpcError(enriched)
        }
        other => other,
    }
}

/// Parse duration string to seconds
fn parse_duration(duration: &str) -> Result<i64> {
    // Try parsing as raw number first
    if let Ok(seconds) = duration.parse::<i64>() {
        return Ok(seconds);
    }

    // Parse duration with unit suffix (e.g., "1h", "30m")
    let len = duration.len();
    if len < 2 {
        return Err(crate::error::CliError::InvalidArgument(
            "Invalid duration format".to_string(),
        ));
    }

    let (value_str, unit) = duration.split_at(len - 1);
    let value = value_str.parse::<i64>().map_err(|_| {
        crate::error::CliError::InvalidArgument("Invalid duration value".to_string())
    })?;

    let multiplier = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86400,
        _ => {
            return Err(crate::error::CliError::InvalidArgument(
                "Invalid duration unit (use s, m, h, d)".to_string(),
            ))
        }
    };

    Ok(value * multiplier)
}

/// Extract token string from Erlang term map
fn extract_token(term: &Term) -> Result<String> {
    match term {
        Term::Map(map) => {
            for (key, value) in &map.map {
                if let Term::Binary(key_bin) = key {
                    let key_str = String::from_utf8_lossy(&key_bin.bytes);
                    if key_str == "token" {
                        if let Term::Binary(value_bin) = value {
                            return Ok(String::from_utf8_lossy(&value_bin.bytes).to_string());
                        }
                    }
                }
            }
            Err(crate::error::CliError::TermConversionError(
                "token field not found in response".to_string(),
            ))
        }
        _ => Err(crate::error::CliError::TermConversionError(
            "Expected map".to_string(),
        )),
    }
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

fn format_rotate_success(mode_key: &str, result: &Term, format: OutputFormat) -> Result<()> {
    let data = unwrap_ok_tuple(result.clone())?;
    let map = term_to_map(&data)?;

    match format {
        OutputFormat::Json => {
            let json_value = rotate_response_to_json(&map);
            println!("{}", json::format(&json_value)?);
        }
        OutputFormat::Table => {
            print_rotate_table(mode_key, &map);
        }
    }

    Ok(())
}

fn rotate_response_to_json(map: &std::collections::HashMap<String, Term>) -> serde_json::Value {
    let mut obj = serde_json::Map::new();

    for (key, term) in map {
        if let Some(value) = term_to_json_value(term) {
            obj.insert(key.clone(), value);
        }
    }

    serde_json::Value::Object(obj)
}

fn term_to_json_value(term: &Term) -> Option<serde_json::Value> {
    match term {
        Term::Atom(atom) => match atom.name.as_str() {
            "true" => Some(serde_json::Value::Bool(true)),
            "false" => Some(serde_json::Value::Bool(false)),
            "nil" => Some(serde_json::Value::Null),
            other => Some(serde_json::Value::String(other.to_string())),
        },
        Term::Binary(_) => term_to_string(term).ok().map(serde_json::Value::String),
        Term::FixInteger(n) => Some(serde_json::Value::from(n.value)),
        Term::Map(_) => {
            let nested = term_to_map(term).ok()?;
            Some(rotate_response_to_json(&nested))
        }
        _ => None,
    }
}

fn print_rotate_table(mode_key: &str, map: &std::collections::HashMap<String, Term>) {
    match mode_key {
        "stage" => {
            println!("✓ Staged incoming CA");
            print_field(map, "subject", "Subject");
            print_field(map, "fingerprint", "Fingerprint");
            print_field(map, "not_before", "Valid from");
            print_field(map, "not_after", "Valid to");
            println!();
            println!("Inspect with: neonfs cluster ca rotate --status");
            println!("Cancel with:  neonfs cluster ca rotate --abort");
            println!("Commit with:  neonfs cluster ca rotate --finalize");
        }
        "abort" => {
            println!("✓ Aborted CA rotation; staged incoming CA discarded");
        }
        "finalize" => {
            println!("✓ Finalised CA rotation");
            print_field(map, "old_fingerprint", "Previous CA");
            print_field(map, "fingerprint", "New active CA");
        }
        "status" => {
            let in_progress = map
                .get("rotation_in_progress")
                .and_then(|t| match t {
                    Term::Atom(a) => Some(a.name == "true"),
                    _ => None,
                })
                .unwrap_or(false);

            if in_progress {
                println!("✓ Rotation in progress");
            } else {
                println!("◯ No rotation in progress");
            }

            if let Some(active_term) = map.get("active") {
                if let Ok(active) = term_to_map(active_term) {
                    println!();
                    println!("Active CA");
                    print_field(&active, "subject", "  Subject");
                    print_field(&active, "fingerprint", "  Fingerprint");
                    print_field(&active, "valid_from", "  Valid from");
                    print_field(&active, "valid_to", "  Valid to");
                }
            }

            if let Some(incoming_term) = map.get("incoming") {
                if let Ok(incoming) = term_to_map(incoming_term) {
                    println!();
                    println!("Incoming CA");
                    print_field(&incoming, "subject", "  Subject");
                    print_field(&incoming, "fingerprint", "  Fingerprint");
                    print_field(&incoming, "valid_from", "  Valid from");
                    print_field(&incoming, "valid_to", "  Valid to");
                }
            }
        }
        _ => {
            // Fall through; should be unreachable.
        }
    }
}

fn print_field(map: &std::collections::HashMap<String, Term>, key: &str, label: &str) {
    if let Some(term) = map.get(key) {
        if let Some(value) = term_to_json_value(term) {
            let rendered = match value {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            println!("  {}: {}", label, rendered);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebalance_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: ClusterCommand,
        }

        // Default flags
        let cli = TestCli::try_parse_from(["test", "rebalance"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                ClusterCommand::Rebalance {
                    tier,
                    threshold,
                    batch_size,
                } => {
                    assert!(tier.is_none());
                    assert_eq!(threshold, "0.10");
                    assert_eq!(batch_size, "50");
                }
                _ => panic!("Expected Rebalance variant"),
            }
        }

        // With tier
        let cli = TestCli::try_parse_from(["test", "rebalance", "--tier", "hot"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                ClusterCommand::Rebalance { tier, .. } => {
                    assert_eq!(tier.as_deref(), Some("hot"));
                }
                _ => panic!("Expected Rebalance variant"),
            }
        }

        // All flags
        let cli = TestCli::try_parse_from([
            "test",
            "rebalance",
            "--tier",
            "warm",
            "--threshold",
            "0.05",
            "--batch-size",
            "100",
        ]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                ClusterCommand::Rebalance {
                    tier,
                    threshold,
                    batch_size,
                } => {
                    assert_eq!(tier.as_deref(), Some("warm"));
                    assert_eq!(threshold, "0.05");
                    assert_eq!(batch_size, "100");
                }
                _ => panic!("Expected Rebalance variant"),
            }
        }
    }

    #[test]
    fn test_rebalance_status_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: ClusterCommand,
        }

        let cli = TestCli::try_parse_from(["test", "rebalance-status"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            assert!(matches!(parsed.command, ClusterCommand::RebalanceStatus));
        }
    }

    #[test]
    fn test_force_reset_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: ClusterCommand,
        }

        // --keep is required
        let missing_keep =
            TestCli::try_parse_from(["test", "force-reset", "--yes-i-accept-data-loss"]);
        assert!(missing_keep.is_err());

        // Single --keep, default min-unreachable-seconds, with data-loss flag
        let single = TestCli::try_parse_from([
            "test",
            "force-reset",
            "--keep",
            "host1",
            "--yes-i-accept-data-loss",
        ]);
        assert!(single.is_ok());
        if let Ok(parsed) = single {
            match parsed.command {
                ClusterCommand::ForceReset {
                    keep,
                    min_unreachable_seconds,
                    yes_i_accept_data_loss,
                } => {
                    assert_eq!(keep, vec!["host1".to_string()]);
                    assert_eq!(min_unreachable_seconds, 1800);
                    assert!(yes_i_accept_data_loss);
                }
                _ => panic!("Expected ForceReset variant"),
            }
        }

        // Multiple --keep values, custom min-unreachable-seconds
        let multi = TestCli::try_parse_from([
            "test",
            "force-reset",
            "--keep",
            "host1",
            "--keep",
            "host2",
            "--min-unreachable-seconds",
            "600",
            "--yes-i-accept-data-loss",
        ]);
        assert!(multi.is_ok());
        if let Ok(parsed) = multi {
            match parsed.command {
                ClusterCommand::ForceReset {
                    keep,
                    min_unreachable_seconds,
                    yes_i_accept_data_loss,
                } => {
                    assert_eq!(keep, vec!["host1".to_string(), "host2".to_string()]);
                    assert_eq!(min_unreachable_seconds, 600);
                    assert!(yes_i_accept_data_loss);
                }
                _ => panic!("Expected ForceReset variant"),
            }
        }

        // Comma-separated --keep values
        let comma = TestCli::try_parse_from([
            "test",
            "force-reset",
            "--keep",
            "host1,host2",
            "--yes-i-accept-data-loss",
        ]);
        assert!(comma.is_ok());
        if let Ok(parsed) = comma {
            match parsed.command {
                ClusterCommand::ForceReset { keep, .. } => {
                    assert_eq!(keep, vec!["host1".to_string(), "host2".to_string()]);
                }
                _ => panic!("Expected ForceReset variant"),
            }
        }

        // Without --yes-i-accept-data-loss, flag is false but parse succeeds.
        // The local guard in force_reset() produces the refusal at runtime.
        let no_flag = TestCli::try_parse_from(["test", "force-reset", "--keep", "host1"]);
        assert!(no_flag.is_ok());
        if let Ok(parsed) = no_flag {
            match parsed.command {
                ClusterCommand::ForceReset {
                    yes_i_accept_data_loss,
                    ..
                } => assert!(!yes_i_accept_data_loss),
                _ => panic!("Expected ForceReset variant"),
            }
        }
    }

    #[test]
    fn test_reconstruct_from_disk_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: ClusterCommand,
        }

        // No flags → all defaults to false. Parses fine; the
        // safety gate in the handler refuses without --yes (or
        // --dry-run).
        let bare = TestCli::try_parse_from(["test", "reconstruct-from-disk"]).expect("parses");

        match bare.command {
            ClusterCommand::ReconstructFromDisk {
                yes,
                overwrite_ra_state,
                dry_run,
            } => {
                assert!(!yes);
                assert!(!overwrite_ra_state);
                assert!(!dry_run);
            }
            _ => panic!("Expected ReconstructFromDisk variant"),
        }

        // All three flags set.
        let full = TestCli::try_parse_from([
            "test",
            "reconstruct-from-disk",
            "--yes",
            "--overwrite-ra-state",
            "--dry-run",
        ])
        .expect("parses");

        match full.command {
            ClusterCommand::ReconstructFromDisk {
                yes,
                overwrite_ra_state,
                dry_run,
            } => {
                assert!(yes);
                assert!(overwrite_ra_state);
                assert!(dry_run);
            }
            _ => panic!("Expected ReconstructFromDisk variant"),
        }
    }
}
