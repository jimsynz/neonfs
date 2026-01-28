//! Cluster management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::ClusterStatus;
use crate::term::{extract_error, unwrap_ok_tuple};
use clap::Subcommand;

/// Cluster management subcommands
#[derive(Debug, Subcommand)]
pub enum ClusterCommand {
    /// Initialize a new cluster
    Init {
        /// Cluster name
        #[arg(long)]
        name: String,

        /// Node name for this node
        #[arg(long)]
        node_name: String,
    },

    /// Show cluster status
    Status,

    /// Join an existing cluster
    Join {
        /// Address of existing cluster node
        #[arg(long)]
        node: String,

        /// Node name for this node
        #[arg(long)]
        node_name: String,
    },
}

impl ClusterCommand {
    /// Execute the cluster command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            ClusterCommand::Init { name, node_name } => self.init(name, node_name, format),
            ClusterCommand::Status => self.status(format),
            ClusterCommand::Join { node, node_name } => self.join(node, node_name, format),
        }
    }

    fn init(&self, name: &str, node_name: &str, format: OutputFormat) -> Result<()> {
        // Placeholder implementation
        match format {
            OutputFormat::Json => {
                let result = serde_json::json!({
                    "status": "success",
                    "cluster": name,
                    "node": node_name,
                    "message": "Cluster initialized (placeholder)"
                });
                println!("{}", json::format(&result)?);
            }
            OutputFormat::Table => {
                println!("Initializing cluster '{}' with node '{}'", name, node_name);
                println!("(Placeholder - not yet implemented)");
            }
        }
        Ok(())
    }

    fn status(&self, format: OutputFormat) -> Result<()> {
        // Create tokio runtime for async calls
        let runtime = tokio::runtime::Runtime::new()?;

        // Connect to daemon and call cluster_status
        let result = runtime.block_on(async {
            let conn = DaemonConnection::connect().await?;
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

    fn join(&self, node: &str, node_name: &str, format: OutputFormat) -> Result<()> {
        // Placeholder implementation
        match format {
            OutputFormat::Json => {
                let result = serde_json::json!({
                    "status": "success",
                    "joined_node": node,
                    "node_name": node_name,
                    "message": "Node joined cluster (placeholder)"
                });
                println!("{}", json::format(&result)?);
            }
            OutputFormat::Table => {
                println!("Joining cluster at '{}' as '{}'", node, node_name);
                println!("(Placeholder - not yet implemented)");
            }
        }
        Ok(())
    }
}

// Tests for cluster commands would require a running daemon (integration tests)
// Unit tests for output formatting are in the output module
