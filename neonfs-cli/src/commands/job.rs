//! Job management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::JobInfo;
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Atom, Binary, Map, Term};

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
            JobCommand::Show { job_id } => self.show(job_id, format),
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
        let runtime = tokio::runtime::Runtime::new()?;

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
            entries: filter_entries,
        });

        let result = runtime.block_on(async {
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
        let runtime = tokio::runtime::Runtime::new()?;

        let job_id_term = Term::Binary(Binary {
            bytes: job_id.as_bytes().to_vec(),
        });

        let result = runtime.block_on(async {
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
        let runtime = tokio::runtime::Runtime::new()?;

        let job_id_term = Term::Binary(Binary {
            bytes: job_id.as_bytes().to_vec(),
        });

        let result = runtime.block_on(async {
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

        let cli = TestCli::try_parse_from(["test", "cancel", "abc123"]);
        assert!(cli.is_ok());
    }
}
