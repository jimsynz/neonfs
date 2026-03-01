//! NeonFS CLI - Command-line interface for NeonFS distributed filesystem

mod commands;
mod daemon;
mod error;
mod output;
mod term;

use clap::{Parser, Subcommand};
use commands::{
    acl::AclCommand, audit::AuditCommand, cluster::ClusterCommand, drive::DriveCommand,
    gc::GcCommand, job::JobCommand, mount::MountCommand, node::NodeCommand, scrub::ScrubCommand,
    volume::VolumeCommand, worker::WorkerCommand,
};
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
    /// ACL management
    Acl {
        #[command(subcommand)]
        command: AclCommand,
    },

    /// Audit log
    Audit {
        #[command(subcommand)]
        command: AuditCommand,
    },

    /// Cluster management
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },

    /// Drive management
    Drive {
        #[command(subcommand)]
        command: DriveCommand,
    },

    /// Garbage collection
    Gc {
        #[command(subcommand)]
        command: GcCommand,
    },

    /// Background job management
    Job {
        #[command(subcommand)]
        command: JobCommand,
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

    /// Integrity scrubbing
    Scrub {
        #[command(subcommand)]
        command: ScrubCommand,
    },

    /// Volume management
    Volume {
        #[command(subcommand)]
        command: VolumeCommand,
    },

    /// Background worker management
    Worker {
        #[command(subcommand)]
        command: WorkerCommand,
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

fn main() {
    let cli = Cli::parse();
    let format = cli.output_format();

    let result = match cli.command {
        Commands::Acl { command } => command.execute(format),
        Commands::Audit { command } => command.execute(format),
        Commands::Cluster { command } => command.execute(format),
        Commands::Drive { command } => command.execute(format),
        Commands::Gc { command } => command.execute(format),
        Commands::Job { command } => command.execute(format),
        Commands::Mount { command } => command.execute(format),
        Commands::Node { command } => command.execute(format),
        Commands::Scrub { command } => command.execute(format),
        Commands::Volume { command } => command.execute(format),
        Commands::Worker { command } => command.execute(format),
    };

    if let Err(err) = result {
        eprintln!("Error: {err}");
        std::process::exit(err.exit_code());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
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

    #[test]
    fn test_acl_subcommand_parsing() {
        let cli = Cli::try_parse_from(["neonfs-cli", "acl", "show", "myvol"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_audit_subcommand_parsing() {
        let cli = Cli::try_parse_from(["neonfs-cli", "audit", "list"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_volume_encryption_flag() {
        let cli = Cli::try_parse_from([
            "neonfs-cli",
            "volume",
            "create",
            "myvol",
            "--encryption",
            "server-side",
        ]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_volume_rotate_key() {
        let cli = Cli::try_parse_from(["neonfs-cli", "volume", "rotate-key", "myvol"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_volume_rotation_status() {
        let cli = Cli::try_parse_from(["neonfs-cli", "volume", "rotation-status", "myvol"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_job_list() {
        let cli = Cli::try_parse_from(["neonfs-cli", "job", "list"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_job_list_with_status() {
        let cli = Cli::try_parse_from(["neonfs-cli", "job", "list", "--status", "running"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_job_list_node_only() {
        let cli = Cli::try_parse_from(["neonfs-cli", "job", "list", "--node-only"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_job_show() {
        let cli = Cli::try_parse_from(["neonfs-cli", "job", "show", "abc123"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_job_cancel() {
        let cli = Cli::try_parse_from(["neonfs-cli", "job", "cancel", "abc123"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_gc_collect() {
        let cli = Cli::try_parse_from(["neonfs-cli", "gc", "collect"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_gc_collect_with_volume() {
        let cli = Cli::try_parse_from(["neonfs-cli", "gc", "collect", "--volume", "myvol"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_gc_status() {
        let cli = Cli::try_parse_from(["neonfs-cli", "gc", "status"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_scrub_start() {
        let cli = Cli::try_parse_from(["neonfs-cli", "scrub", "start"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_scrub_start_with_volume() {
        let cli = Cli::try_parse_from(["neonfs-cli", "scrub", "start", "--volume", "myvol"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_scrub_status() {
        let cli = Cli::try_parse_from(["neonfs-cli", "scrub", "status"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_drive_list() {
        let cli = Cli::try_parse_from(["neonfs-cli", "drive", "list"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_drive_add() {
        let cli = Cli::try_parse_from(["neonfs-cli", "drive", "add", "--path", "/data/nvme0"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_drive_add_full() {
        let cli = Cli::try_parse_from([
            "neonfs-cli",
            "drive",
            "add",
            "--path",
            "/data/nvme0",
            "--tier",
            "hot",
            "--capacity",
            "1T",
            "--id",
            "nvme0",
        ]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_drive_remove() {
        let cli = Cli::try_parse_from(["neonfs-cli", "drive", "remove", "nvme0"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_drive_remove_force() {
        let cli = Cli::try_parse_from(["neonfs-cli", "drive", "remove", "nvme0", "--force"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_cluster_ca_info() {
        let cli = Cli::try_parse_from(["neonfs-cli", "cluster", "ca", "info"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_cluster_ca_list() {
        let cli = Cli::try_parse_from(["neonfs-cli", "cluster", "ca", "list"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_cluster_ca_revoke() {
        let cli = Cli::try_parse_from(["neonfs-cli", "cluster", "ca", "revoke", "node-1"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_cluster_ca_rotate() {
        let cli = Cli::try_parse_from(["neonfs-cli", "cluster", "ca", "rotate"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_worker_status() {
        let cli = Cli::try_parse_from(["neonfs-cli", "worker", "status"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_worker_configure() {
        let cli =
            Cli::try_parse_from(["neonfs-cli", "worker", "configure", "--max-concurrent", "8"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_worker_configure_all_flags() {
        let cli = Cli::try_parse_from([
            "neonfs-cli",
            "worker",
            "configure",
            "--max-concurrent",
            "8",
            "--max-per-minute",
            "100",
            "--drive-concurrency",
            "4",
        ]);
        assert!(cli.is_ok());
    }
}
