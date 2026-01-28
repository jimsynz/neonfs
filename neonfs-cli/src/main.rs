//! NeonFS CLI - Command-line interface for NeonFS distributed filesystem

mod commands;
mod error;
mod output;

use clap::{Parser, Subcommand};
use commands::{
    cluster::ClusterCommand, mount::MountCommand, node::NodeCommand, volume::VolumeCommand,
};
use error::Result;
use output::OutputFormat;

/// NeonFS CLI - Distributed filesystem management
#[derive(Debug, Parser)]
#[command(name = "neonfs-cli")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Command-line interface for NeonFS distributed filesystem")]
#[command(long_about = None)]
struct Cli {
    /// Output format (json or table)
    #[arg(long, global = true, default_value = "table")]
    output: String,

    /// Enable JSON output (shorthand for --output json)
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Cluster management
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },

    /// Volume management
    Volume {
        #[command(subcommand)]
        command: VolumeCommand,
    },

    /// Mount management
    Mount {
        #[command(subcommand)]
        command: MountCommand,
    },

    /// Node management
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
}

impl Cli {
    /// Determine the output format based on flags
    fn output_format(&self) -> OutputFormat {
        if self.json || self.output == "json" {
            OutputFormat::Json
        } else {
            OutputFormat::Table
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let format = cli.output_format();

    match cli.command {
        Commands::Cluster { command } => command.execute(format),
        Commands::Volume { command } => command.execute(format),
        Commands::Mount { command } => command.execute(format),
        Commands::Node { command } => command.execute(format),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        // Test that CLI parsing works
        let cli = Cli::try_parse_from(["neonfs-cli", "node", "list"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_json_flag() {
        let cli = Cli::parse_from(["neonfs-cli", "--json", "node", "list"]);
        assert!(matches!(cli.output_format(), OutputFormat::Json));
    }

    #[test]
    fn test_output_flag() {
        let cli = Cli::parse_from(["neonfs-cli", "--output", "json", "node", "list"]);
        assert!(matches!(cli.output_format(), OutputFormat::Json));
    }

    #[test]
    fn test_default_table_format() {
        let cli = Cli::parse_from(["neonfs-cli", "node", "list"]);
        assert!(matches!(cli.output_format(), OutputFormat::Table));
    }
}
