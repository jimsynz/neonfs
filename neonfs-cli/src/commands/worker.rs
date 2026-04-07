//! Background worker management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::{
    extract_error, term_to_list, term_to_map, term_to_string, term_to_u64, unwrap_ok_tuple,
};
use clap::Subcommand;
use eetf::{Binary, FixInteger, Map, Term};

/// Background worker subcommands
#[derive(Debug, Subcommand)]
pub enum WorkerCommand {
    /// Configure background worker settings
    Configure {
        /// Maximum concurrent tasks
        #[arg(long)]
        max_concurrent: Option<u32>,

        /// Maximum task starts per minute
        #[arg(long)]
        max_per_minute: Option<u32>,

        /// Maximum concurrent operations per drive
        #[arg(long)]
        drive_concurrency: Option<u32>,
    },

    /// Show current worker configuration and runtime status
    Status,
}

impl WorkerCommand {
    /// Execute the worker command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            WorkerCommand::Configure {
                max_concurrent,
                max_per_minute,
                drive_concurrency,
            } => self.configure(*max_concurrent, *max_per_minute, *drive_concurrency, format),
            WorkerCommand::Status => self.status(format),
        }
    }

    fn configure(
        &self,
        max_concurrent: Option<u32>,
        max_per_minute: Option<u32>,
        drive_concurrency: Option<u32>,
        format: OutputFormat,
    ) -> Result<()> {
        if max_concurrent.is_none() && max_per_minute.is_none() && drive_concurrency.is_none() {
            return Err(crate::error::CliError::RpcError(
                "At least one setting must be specified. Use --max-concurrent, --max-per-minute, or --drive-concurrency".to_string(),
            ));
        }

        let mut opts_entries = vec![];
        if let Some(val) = max_concurrent {
            opts_entries.push((
                Term::Binary(Binary {
                    bytes: b"max_concurrent".to_vec(),
                }),
                Term::FixInteger(FixInteger::from(val as i32)),
            ));
        }
        if let Some(val) = max_per_minute {
            opts_entries.push((
                Term::Binary(Binary {
                    bytes: b"max_per_minute".to_vec(),
                }),
                Term::FixInteger(FixInteger::from(val as i32)),
            ));
        }
        if let Some(val) = drive_concurrency {
            opts_entries.push((
                Term::Binary(Binary {
                    bytes: b"drive_concurrency".to_vec(),
                }),
                Term::FixInteger(FixInteger::from(val as i32)),
            ));
        }

        let opts_term = Term::Map(Map {
            map: opts_entries.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_worker_configure",
                vec![opts_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let config = term_to_map(&data)?;

        let max_c = extract_u64(&config, "max_concurrent");
        let max_m = extract_u64(&config, "max_per_minute");
        let drive_c = extract_u64(&config, "drive_concurrency");

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "configured",
                    "max_concurrent": max_c,
                    "max_per_minute": max_m,
                    "drive_concurrency": drive_c,
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("Worker configuration updated");
                println!();
                println!("Worker Configuration:");
                println!("  Max concurrent:    {}", max_c);
                println!("  Max per minute:    {}", max_m);
                println!("  Drive concurrency: {}", drive_c);
            }
        }
        Ok(())
    }

    fn status(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "handle_worker_status", vec![])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let status_terms = term_to_list(&data)?;

        let statuses: Vec<_> = status_terms
            .iter()
            .filter_map(|t| term_to_map(t).ok())
            .collect();

        match format {
            OutputFormat::Json => {
                let json_statuses: Vec<_> = statuses
                    .iter()
                    .map(|s| {
                        let (high, normal, low) = extract_priority_counts(s);
                        serde_json::json!({
                            "node": extract_string(s, "node"),
                            "max_concurrent": extract_u64(s, "max_concurrent"),
                            "max_per_minute": extract_u64(s, "max_per_minute"),
                            "drive_concurrency": extract_u64(s, "drive_concurrency"),
                            "queued": extract_u64(s, "queued"),
                            "running": extract_u64(s, "running"),
                            "completed_total": extract_u64(s, "completed_total"),
                            "by_priority": {
                                "high": high,
                                "normal": normal,
                                "low": low,
                            },
                        })
                    })
                    .collect();
                println!("{}", json::format(&json_statuses)?);
            }
            OutputFormat::Table => {
                if statuses.is_empty() {
                    println!("No worker status available");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "NODE".to_string(),
                        "CONCURRENT".to_string(),
                        "PER MIN".to_string(),
                        "DRIVE CONC".to_string(),
                        "QUEUED".to_string(),
                        "RUNNING".to_string(),
                        "COMPLETED".to_string(),
                    ]);
                    for s in &statuses {
                        tbl.add_row(vec![
                            extract_string(s, "node"),
                            extract_u64(s, "max_concurrent").to_string(),
                            extract_u64(s, "max_per_minute").to_string(),
                            extract_u64(s, "drive_concurrency").to_string(),
                            extract_u64(s, "queued").to_string(),
                            extract_u64(s, "running").to_string(),
                            extract_u64(s, "completed_total").to_string(),
                        ]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }
}

fn extract_u64(map: &std::collections::HashMap<String, Term>, key: &str) -> u64 {
    map.get(key).and_then(|t| term_to_u64(t).ok()).unwrap_or(0)
}

fn extract_string(map: &std::collections::HashMap<String, Term>, key: &str) -> String {
    map.get(key)
        .and_then(|t| term_to_string(t).ok())
        .unwrap_or_default()
}

fn extract_priority_counts(status: &std::collections::HashMap<String, Term>) -> (u64, u64, u64) {
    let priority_map = status
        .get("by_priority")
        .and_then(|t| term_to_map(t).ok())
        .unwrap_or_default();

    let high = extract_u64(&priority_map, "high");
    let normal = extract_u64(&priority_map, "normal");
    let low = extract_u64(&priority_map, "low");
    (high, normal, low)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_worker_status_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: WorkerCommand,
        }

        let cli = TestCli::try_parse_from(["test", "status"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            assert!(matches!(parsed.command, WorkerCommand::Status));
        }
    }

    #[test]
    fn test_worker_configure_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: WorkerCommand,
        }

        let cli = TestCli::try_parse_from(["test", "configure", "--max-concurrent", "8"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                WorkerCommand::Configure {
                    max_concurrent,
                    max_per_minute,
                    drive_concurrency,
                } => {
                    assert_eq!(max_concurrent, Some(8));
                    assert!(max_per_minute.is_none());
                    assert!(drive_concurrency.is_none());
                }
                _ => panic!("Expected Configure variant"),
            }
        }
    }

    #[test]
    fn test_worker_configure_all_flags() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: WorkerCommand,
        }

        let cli = TestCli::try_parse_from([
            "test",
            "configure",
            "--max-concurrent",
            "8",
            "--max-per-minute",
            "100",
            "--drive-concurrency",
            "4",
        ]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                WorkerCommand::Configure {
                    max_concurrent,
                    max_per_minute,
                    drive_concurrency,
                } => {
                    assert_eq!(max_concurrent, Some(8));
                    assert_eq!(max_per_minute, Some(100));
                    assert_eq!(drive_concurrency, Some(4));
                }
                _ => panic!("Expected Configure variant"),
            }
        }
    }

    #[test]
    fn test_worker_configure_no_flags_errors() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: WorkerCommand,
        }

        // clap still parses successfully with no flags — the execute method checks
        let cli = TestCli::try_parse_from(["test", "configure"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_term_construction_for_configure() {
        let term = Term::Map(Map {
            map: HashMap::from([(
                Term::Binary(Binary {
                    bytes: b"max_concurrent".to_vec(),
                }),
                Term::FixInteger(FixInteger::from(8)),
            )]),
        });

        if let Term::Map(Map { map }) = term {
            assert_eq!(map.len(), 1);
        } else {
            panic!("Expected Map term");
        }
    }
}
