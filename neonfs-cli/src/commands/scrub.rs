//! Integrity scrub commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::JobInfo;
use crate::term::{extract_error, term_to_list, term_to_map, term_to_string, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, Map, Term};

/// Integrity scrub subcommands
#[derive(Debug, Subcommand)]
pub enum ScrubCommand {
    /// Start an integrity scan
    Start {
        /// Restrict scrubbing to a specific volume
        #[arg(long)]
        volume: Option<String>,
    },

    /// Show recent scrub job history
    Status,
}

impl ScrubCommand {
    /// Execute the scrub command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            ScrubCommand::Start { volume } => self.start(volume.as_deref(), format),
            ScrubCommand::Status => self.status(format),
        }
    }

    fn start(&self, volume: Option<&str>, format: OutputFormat) -> Result<()> {
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
            map: opts_entries.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_scrub_start",
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
                println!("Integrity scrub started");
                println!("  Volume: {}", volume_display);
                println!("  Job ID: {}", job_id);
                println!("\nTrack progress with: neonfs job show {}", job_id);
            }
        }
        Ok(())
    }

    fn status(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "handle_scrub_status", vec![])
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
                    println!("No scrub jobs found");
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
    fn test_scrub_start_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: ScrubCommand,
        }

        let cli = TestCli::try_parse_from(["test", "start"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                ScrubCommand::Start { volume } => {
                    assert!(volume.is_none());
                }
                _ => panic!("Expected Start variant"),
            }
        }
    }

    #[test]
    fn test_scrub_start_with_volume() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: ScrubCommand,
        }

        let cli = TestCli::try_parse_from(["test", "start", "--volume", "myvol"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                ScrubCommand::Start { volume } => {
                    assert_eq!(volume.as_deref(), Some("myvol"));
                }
                _ => panic!("Expected Start variant"),
            }
        }
    }

    #[test]
    fn test_scrub_status_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: ScrubCommand,
        }

        let cli = TestCli::try_parse_from(["test", "status"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                ScrubCommand::Status => {}
                _ => panic!("Expected Status variant"),
            }
        }
    }
}
