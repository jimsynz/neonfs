//! Job management commands

use crate::daemon::DaemonConnection;
use crate::error::{CliError, Result};
use crate::output::{json, table, OutputFormat};
use crate::term::types::JobInfo;
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Atom, Binary, Map, Term};
use std::time::Duration;

/// How often `--wait` re-polls a job's status.
const JOB_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Job management subcommands
#[derive(Debug, Subcommand)]
pub enum JobCommand {
    /// List background jobs
    List {
        /// Filter by status (e.g. running, completed, failed)
        #[arg(long)]
        status: Option<String>,

        /// Filter by job type (e.g. key-rotation)
        #[arg(long, rename_all = "kebab-case")]
        r#type: Option<String>,

        /// Only show jobs on the local node (skip cluster-wide query)
        #[arg(long)]
        node_only: bool,
    },

    /// Show details of a specific job
    Show {
        /// Job identifier
        job_id: String,

        /// Block until the job reaches a terminal state, then print its
        /// final status. Exits non-zero if the job failed or was
        /// cancelled. A pure observer — interrupting leaves the job running.
        #[arg(long)]
        wait: bool,
    },

    /// Cancel a running or pending job
    Cancel {
        /// Job identifier
        job_id: String,
    },
}

impl JobCommand {
    /// Execute the job command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            JobCommand::List {
                status,
                r#type,
                node_only,
            } => self.list(status.as_deref(), r#type.as_deref(), *node_only, format),
            JobCommand::Show { job_id, wait } => {
                if *wait {
                    wait_and_report(job_id, format)
                } else {
                    self.show(job_id, format)
                }
            }
            JobCommand::Cancel { job_id } => self.cancel(job_id, format),
        }
    }

    fn list(
        &self,
        status: Option<&str>,
        type_filter: Option<&str>,
        node_only: bool,
        format: OutputFormat,
    ) -> Result<()> {
        let mut filter_entries = vec![];

        if let Some(s) = status {
            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"status".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: s.as_bytes().to_vec(),
                }),
            ));
        }

        if let Some(t) = type_filter {
            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"type".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: t.as_bytes().to_vec(),
                }),
            ));
        }

        if node_only {
            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"cluster".to_vec(),
                }),
                Term::Atom(Atom::from("false")),
            ));
        }

        let filters_term = Term::Map(Map {
            map: filter_entries.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_list_jobs",
                vec![filters_term],
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
                    println!("No jobs found");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "ID".to_string(),
                        "TYPE".to_string(),
                        "STATUS".to_string(),
                        "PROGRESS".to_string(),
                        "NODE".to_string(),
                        "STARTED".to_string(),
                    ]);
                    for job in &jobs {
                        tbl.add_row(vec![
                            job.id_short(),
                            job.job_type.clone(),
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

    fn show(&self, job_id: &str, format: OutputFormat) -> Result<()> {
        let job_id_term = Term::Binary(Binary {
            bytes: job_id.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_get_job",
                vec![job_id_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let job = JobInfo::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&job)?);
            }
            OutputFormat::Table => {
                println!("Job {}", job.id);
                println!("  Type:      {}", job.job_type);
                println!("  Status:    {}", job.status);
                println!("  Node:      {}", job.node);
                println!("  Progress:  {}", job.progress_detail());
                println!("  Created:   {}", job.created_at);
                if let Some(ref started) = job.started_at {
                    println!("  Started:   {}", started);
                }
                if let Some(ref completed) = job.completed_at {
                    println!("  Completed: {}", completed);
                }
                if let Some(ref error) = job.error {
                    println!("  Error:     {}", error);
                }
                if !job.params.is_empty() {
                    println!("  Params:");
                    for (k, v) in &job.params {
                        println!("    {}: {}", k, v);
                    }
                }
            }
        }
        Ok(())
    }

    fn cancel(&self, job_id: &str, format: OutputFormat) -> Result<()> {
        let job_id_term = Term::Binary(Binary {
            bytes: job_id.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_cancel_job",
                vec![job_id_term],
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
                    "job_id": job_id,
                    "message": "Job cancelled"
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("Job '{}' cancelled", job_id);
            }
        }
        Ok(())
    }
}

/// Blocks until `job_id` reaches a terminal state, renders the final
/// status, and returns `Err` (non-zero exit) if it failed or was
/// cancelled. Shared by every `--wait`-capable job-starting command.
pub fn wait_and_report(job_id: &str, format: OutputFormat) -> Result<()> {
    let job = smol::block_on(poll_until_terminal(job_id, format))?;

    match format {
        OutputFormat::Json => println!("{}", json::format(&job)?),
        OutputFormat::Table => print_terminal_line(&job),
    }

    fail_if_unsuccessful(std::slice::from_ref(&job))
}

/// Like `wait_and_report` but blocks on several jobs at once (e.g.
/// `repair start` queues one per volume). Waits for all to finish and
/// fails if any did.
pub fn wait_and_report_many(job_ids: &[String], format: OutputFormat) -> Result<()> {
    if job_ids.is_empty() {
        return Ok(());
    }

    let jobs = smol::block_on(async {
        let mut jobs = Vec::with_capacity(job_ids.len());
        for id in job_ids {
            jobs.push(poll_until_terminal(id, format).await?);
        }
        Ok::<_, CliError>(jobs)
    })?;

    match format {
        OutputFormat::Json => println!("{}", json::format(&jobs)?),
        OutputFormat::Table => jobs.iter().for_each(print_terminal_line),
    }

    fail_if_unsuccessful(&jobs)
}

async fn poll_until_terminal(job_id: &str, format: OutputFormat) -> Result<JobInfo> {
    let mut last_status = String::new();

    loop {
        if let Some(job) = fetch_job_once(job_id).await {
            if matches!(format, OutputFormat::Table) && job.status != last_status {
                eprintln!("  {} … {}", job.id_short(), job.progress_detail());
                last_status = job.status.clone();
            }

            if job.is_terminal() {
                return Ok(job);
            }
        }

        // Transient connect/RPC failures (a core node may be mid-restart)
        // fall through and retry — the wait is deliberately unbounded so a
        // long scrub/repair isn't cut short; interrupt the CLI to stop it.
        smol::Timer::after(JOB_POLL_INTERVAL).await;
    }
}

async fn fetch_job_once(job_id: &str) -> Option<JobInfo> {
    let mut conn = DaemonConnection::connect().await.ok()?;
    let result = conn
        .call(
            "Elixir.NeonFS.CLI.Handler",
            "handle_get_job",
            vec![Term::Binary(Binary {
                bytes: job_id.as_bytes().to_vec(),
            })],
        )
        .await
        .ok()?;

    if extract_error(&result).is_some() {
        return None;
    }

    JobInfo::from_term(unwrap_ok_tuple(result).ok()?).ok()
}

fn print_terminal_line(job: &JobInfo) {
    println!("Job {} {}", job.id_short(), job.status);
    if let Some(error) = &job.error {
        println!("  Error: {}", error);
    }
}

fn fail_if_unsuccessful(jobs: &[JobInfo]) -> Result<()> {
    match jobs.iter().find(|job| job.is_failure()) {
        None => Ok(()),
        Some(job) => Err(CliError::RpcFailed(format!(
            "job {} {}{}",
            job.id_short(),
            job.status,
            job.error
                .as_ref()
                .map(|e| format!(": {}", e))
                .unwrap_or_default()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: JobCommand,
        }

        let cli = TestCli::try_parse_from(["test", "list"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "list", "--status", "running"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "list", "--type", "key-rotation"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "list", "--node-only"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "show", "abc123"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "show", "abc123", "--wait"]);
        assert!(cli.is_ok());
        match cli.unwrap().command {
            JobCommand::Show { job_id, wait } => {
                assert_eq!(job_id, "abc123");
                assert!(wait);
            }
            _ => panic!("Expected Show variant"),
        }

        let cli = TestCli::try_parse_from(["test", "cancel", "abc123"]);
        assert!(cli.is_ok());
    }
}
