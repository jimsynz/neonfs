//! Replica-count repair commands (#709)
//!
//! Operator-facing surface for `NeonFS.Core.ReplicaRepair` (the
//! data-plane primitive landed in #706 and wrapped by the JobTracker
//! runner + scheduler in #707 / #708). Mirrors the `scrub` command
//! shape, with two differences:
//!
//! 1. `start` returns a *list* of jobs (one per volume in scope),
//!    not a single map — the no-`--volume` form fans out across
//!    every volume in the registry.
//! 2. `status` accepts an optional `--volume` filter for narrowing
//!    the report to one volume's repair history.

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::JobInfo;
use crate::term::{extract_error, term_to_list, term_to_map, term_to_string, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, Map, Term};

/// Replica-count repair subcommands
#[derive(Debug, Subcommand)]
pub enum RepairCommand {
    /// Queue a replica-count repair pass
    Start {
        /// Restrict the pass to a specific volume. Without this
        /// flag, queues a pass for every volume in the registry.
        #[arg(long)]
        volume: Option<String>,
    },

    /// Show recent replica-repair job history
    Status {
        /// Filter the report to a single volume's repair history.
        #[arg(long)]
        volume: Option<String>,
    },
}

impl RepairCommand {
    /// Execute the repair command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            RepairCommand::Start { volume } => self.start(volume.as_deref(), format),
            RepairCommand::Status { volume } => self.status(volume.as_deref(), format),
        }
    }

    fn start(&self, volume: Option<&str>, format: OutputFormat) -> Result<()> {
        let opts_term = build_volume_opts(volume);

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_repair_start",
                vec![opts_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let job_terms = term_to_list(&data)?;

        let job_ids: Vec<String> = job_terms
            .iter()
            .filter_map(|t| term_to_map(t).ok())
            .filter_map(|m| m.get("id").and_then(|v| term_to_string(v).ok()))
            .collect();

        let volume_display = volume.unwrap_or("all");

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "started",
                    "job_ids": job_ids,
                    "volume": volume_display,
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                if job_ids.is_empty() {
                    println!("No new repair jobs queued");
                    println!("  (every target volume already has a running repair pass)");
                } else {
                    println!("Replica-count repair started");
                    println!("  Volume: {}", volume_display);
                    println!("  Jobs queued: {}", job_ids.len());
                    for id in &job_ids {
                        println!("    - {}", id);
                    }
                    println!("\nTrack progress with: neonfs job show <job-id>");
                }
            }
        }
        Ok(())
    }

    fn status(&self, volume: Option<&str>, format: OutputFormat) -> Result<()> {
        let opts_term = build_volume_opts(volume);

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_repair_status",
                vec![opts_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let job_terms = term_to_list(&data)?;
        let jobs: Result<Vec<JobInfo>> = job_terms.into_iter().map(JobInfo::from_term).collect();
        let jobs = jobs?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&jobs)?);
            }
            OutputFormat::Table => {
                if jobs.is_empty() {
                    println!("No repair jobs found");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "ID".to_string(),
                        "STATUS".to_string(),
                        "PROGRESS".to_string(),
                        "NODE".to_string(),
                        "STARTED".to_string(),
                    ]);
                    for job in &jobs {
                        tbl.add_row(vec![
                            job.id_short(),
                            job.status.clone(),
                            job.progress_string(),
                            job.node.clone(),
                            job.started_at.clone().unwrap_or_default(),
                        ]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }
}

fn build_volume_opts(volume: Option<&str>) -> Term {
    let mut entries = vec![];
    if let Some(vol) = volume {
        entries.push((
            Term::Binary(Binary {
                bytes: b"volume".to_vec(),
            }),
            Term::Binary(Binary {
                bytes: vol.as_bytes().to_vec(),
            }),
        ));
    }
    Term::Map(Map {
        map: entries.into_iter().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: RepairCommand,
    }

    #[test]
    fn test_repair_start_no_volume() {
        let cli = TestCli::try_parse_from(["test", "start"]).unwrap();
        match cli.command {
            RepairCommand::Start { volume } => assert!(volume.is_none()),
            _ => panic!("Expected Start variant"),
        }
    }

    #[test]
    fn test_repair_start_with_volume() {
        let cli = TestCli::try_parse_from(["test", "start", "--volume", "foo"]).unwrap();
        match cli.command {
            RepairCommand::Start { volume } => assert_eq!(volume.as_deref(), Some("foo")),
            _ => panic!("Expected Start variant"),
        }
    }

    #[test]
    fn test_repair_status_no_volume() {
        let cli = TestCli::try_parse_from(["test", "status"]).unwrap();
        match cli.command {
            RepairCommand::Status { volume } => assert!(volume.is_none()),
            _ => panic!("Expected Status variant"),
        }
    }

    #[test]
    fn test_repair_status_with_volume() {
        let cli = TestCli::try_parse_from(["test", "status", "--volume", "bar"]).unwrap();
        match cli.command {
            RepairCommand::Status { volume } => assert_eq!(volume.as_deref(), Some("bar")),
            _ => panic!("Expected Status variant"),
        }
    }
}
