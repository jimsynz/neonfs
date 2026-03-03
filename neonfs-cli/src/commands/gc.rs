//! Garbage collection commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::JobInfo;
use crate::term::{extract_error, term_to_list, term_to_map, term_to_string, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, Map, Term};

/// Garbage collection subcommands
#[derive(Debug, Subcommand)]
pub enum GcCommand {
    /// Trigger garbage collection
    Collect {
        /// Restrict collection to a specific volume
        #[arg(long)]
        volume: Option<String>,
    },

    /// Show recent GC job history
    Status,
}

impl GcCommand {
    /// Execute the GC command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            GcCommand::Collect { volume } => self.collect(volume.as_deref(), format),
            GcCommand::Status => self.status(format),
        }
    }

    fn collect(&self, volume: Option<&str>, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let mut opts_entries = vec![];
        if let Some(vol) = volume {
            opts_entries.push((
                Term::Binary(Binary {
                    bytes: b"volume".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: vol.as_bytes().to_vec(),
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
                "handle_gc_collect",
                vec![opts_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let job_map = term_to_map(&data)?;

        let job_id = job_map
            .get("id")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();

        let volume_display = volume.unwrap_or("all");

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "started",
                    "job_id": job_id,
                    "volume": volume_display,
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("Garbage collection started");
                println!("  Volume: {}", volume_display);
                println!("  Job ID: {}", job_id);
                println!("\nTrack progress with: neonfs job show {}", job_id);
            }
        }
        Ok(())
    }

    fn status(&self, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "handle_gc_status", vec![])
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
                    println!("No GC jobs found");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gc_collect_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: GcCommand,
        }

        let cli = TestCli::try_parse_from(["test", "collect"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                GcCommand::Collect { volume } => {
                    assert!(volume.is_none());
                }
                _ => panic!("Expected Collect variant"),
            }
        }
    }

    #[test]
    fn test_gc_collect_with_volume() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: GcCommand,
        }

        let cli = TestCli::try_parse_from(["test", "collect", "--volume", "myvol"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                GcCommand::Collect { volume } => {
                    assert_eq!(volume.as_deref(), Some("myvol"));
                }
                _ => panic!("Expected Collect variant"),
            }
        }
    }

    #[test]
    fn test_gc_status_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: GcCommand,
        }

        let cli = TestCli::try_parse_from(["test", "status"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                GcCommand::Status => {}
                _ => panic!("Expected Status variant"),
            }
        }
    }
}
