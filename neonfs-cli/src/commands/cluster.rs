//! Cluster management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::{ClusterInitResult, ClusterStatus};
use crate::term::{extract_error, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, FixInteger, Term};

/// Cluster management subcommands
#[derive(Debug, Subcommand)]
pub enum ClusterCommand {
    /// Initialize a new cluster
    Init {
        /// Cluster name
        #[arg(long)]
        name: String,
    },

    /// Show cluster status
    Status,

    /// Create an invite token for joining nodes
    CreateInvite {
        /// Token expiration duration (e.g., "1h", "30m", "3600")
        #[arg(long, default_value = "1h")]
        expires: String,
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
}

impl ClusterCommand {
    /// Execute the cluster command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            ClusterCommand::Init { name } => self.init(name, format),
            ClusterCommand::Status => self.status(format),
            ClusterCommand::CreateInvite { expires } => self.create_invite(expires, format),
            ClusterCommand::Join { token, via } => self.join(token, via, format),
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
        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
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
        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
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
        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
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
        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
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
                println!("✓ Successfully joined cluster '{}'", join_result.cluster_name);
                println!();
                println!("  Cluster ID:  {}", join_result.cluster_id);
                println!("  Node ID:     {}", join_result.node_id);
                println!("  Node Name:   {}", join_result.node_name);
                println!();
            }
        }
        Ok(())
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
    let value = value_str
        .parse::<i64>()
        .map_err(|_| crate::error::CliError::InvalidArgument("Invalid duration value".to_string()))?;

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

// Tests for cluster commands would require a running daemon (integration tests)
// Unit tests for output formatting are in the output module
