//! Cluster management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::{
    CaInfo, CaRevokeResult, CertificateEntry, ClusterInitResult, ClusterStatus,
};
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, FixInteger, Term};

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

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(format_ca_error(&err_msg)));
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

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(format_ca_error(&err_msg)));
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

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(format_ca_error(&err_msg)));
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

/// Format CA-specific error messages for display
fn format_ca_error(err_msg: &str) -> String {
    match err_msg {
        "ca_not_initialized" => {
            "CA not initialised. Run 'neonfs cluster init' to create the cluster CA.".to_string()
        }
        "node_not_found" => {
            "No certificate found for the specified node. Use 'neonfs cluster ca list' to see issued certificates.".to_string()
        }
        other => other.to_string(),
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

// Tests for cluster commands would require a running daemon (integration tests)
// Unit tests for output formatting are in the output module
