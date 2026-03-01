//! Cluster management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::{
    CaInfo, CaRevokeResult, CertificateEntry, ClusterInitResult, ClusterStatus,
};
use crate::term::{extract_error, term_to_list, term_to_map, term_to_string, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, FixInteger, Map, Term};

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

        /// Node name of existing cluster member (e.g., neonfs_core@node1)
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

    /// Show cluster status
    Status,
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

    /// Rotate the cluster CA (reissues all node certificates)
    Rotate,
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
            ClusterCommand::Status => self.status(format),
        }
    }

    fn init(&self, name: &str, format: OutputFormat) -> Result<()> {
        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        // Connect to daemon and call cluster_init
        let result = runtime.block_on(async {
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
        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        // Connect to daemon and call cluster_status
        let result = runtime.block_on(async {
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

        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        // Connect to daemon and call create_invite
        let result = runtime.block_on(async {
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
        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        // Connect to daemon and call join_cluster
        let result = runtime.block_on(async {
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
        let runtime = tokio::runtime::Runtime::new()?;

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
            entries: opts_entries,
        });

        let result = runtime.block_on(async {
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
        let runtime = tokio::runtime::Runtime::new()?;

        let result = runtime.block_on(async {
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
}

impl CaCommand {
    /// Execute the CA subcommand
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            CaCommand::Info => self.info(format),
            CaCommand::List => self.list(format),
            CaCommand::Revoke { node } => self.revoke(node, format),
            CaCommand::Rotate => self.rotate(),
        }
    }

    fn info(&self, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let result = runtime.block_on(async {
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
        let runtime = tokio::runtime::Runtime::new()?;

        let result = runtime.block_on(async {
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
        let runtime = tokio::runtime::Runtime::new()?;

        let result = runtime.block_on(async {
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

    fn rotate(&self) -> Result<()> {
        eprintln!("CA rotation is not yet implemented.");
        eprintln!();
        eprintln!("CA rotation is a rare, disruptive operation that reissues all node");
        eprintln!("certificates. It requires a dual-CA transition period and rolling");
        eprintln!("reissuance across the cluster.");
        eprintln!();
        eprintln!("This will be implemented in a future release.");
        Err(crate::error::CliError::RpcError(
            "CA rotation not yet implemented".to_string(),
        ))
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
            for (key, value) in &map.entries {
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
        _ => None,
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
}
