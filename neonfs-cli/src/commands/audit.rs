//! Audit log commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::AuditEntry;
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, FixInteger, Map, Term};

/// Audit log subcommands
#[derive(Debug, Subcommand)]
pub enum AuditCommand {
    /// List audit log events
    List {
        /// Filter by event type (e.g. volume_created, acl_grant)
        #[arg(long, value_name = "TYPE")]
        r#type: Option<String>,

        /// Filter by actor UID
        #[arg(long)]
        uid: Option<u64>,

        /// Filter by resource
        #[arg(long)]
        resource: Option<String>,

        /// Show events since (ISO 8601 datetime)
        #[arg(long)]
        since: Option<String>,

        /// Show events until (ISO 8601 datetime)
        #[arg(long)]
        until: Option<String>,

        /// Maximum number of results
        #[arg(long, default_value = "50")]
        limit: u64,
    },
}

impl AuditCommand {
    /// Execute the audit command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            AuditCommand::List {
                r#type,
                uid,
                resource,
                since,
                until,
                limit,
            } => self.list(
                r#type.as_deref(),
                *uid,
                resource.as_deref(),
                since.as_deref(),
                until.as_deref(),
                *limit,
                format,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn list(
        &self,
        event_type: Option<&str>,
        uid: Option<u64>,
        resource: Option<&str>,
        since: Option<&str>,
        until: Option<&str>,
        limit: u64,
        format: OutputFormat,
    ) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        // Build filters map
        let mut filter_entries = vec![];

        if let Some(t) = event_type {
            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"type".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: t.as_bytes().to_vec(),
                }),
            ));
        }

        if let Some(u) = uid {
            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"actor_uid".to_vec(),
                }),
                Term::FixInteger(FixInteger::from(u as i32)),
            ));
        }

        if let Some(r) = resource {
            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"resource".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: r.as_bytes().to_vec(),
                }),
            ));
        }

        if let Some(s) = since {
            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"since".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: s.as_bytes().to_vec(),
                }),
            ));
        }

        if let Some(u) = until {
            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"until".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: u.as_bytes().to_vec(),
                }),
            ));
        }

        filter_entries.push((
            Term::Binary(Binary {
                bytes: b"limit".to_vec(),
            }),
            Term::FixInteger(FixInteger::from(limit as i32)),
        ));

        let filters_term = Term::Map(Map {
            entries: filter_entries,
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_audit_list",
                vec![filters_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let event_terms = term_to_list(&data)?;
        let events: Result<Vec<AuditEntry>> =
            event_terms.into_iter().map(AuditEntry::from_term).collect();
        let events = events?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&events)?);
            }
            OutputFormat::Table => {
                if events.is_empty() {
                    println!("No audit events found.");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "TIMESTAMP".to_string(),
                        "EVENT".to_string(),
                        "ACTOR".to_string(),
                        "RESOURCE".to_string(),
                        "OUTCOME".to_string(),
                    ]);
                    for event in &events {
                        tbl.add_row(vec![
                            event.timestamp.clone(),
                            event.event_type.clone(),
                            format!("uid:{}", event.actor_uid),
                            event.resource.clone(),
                            event.outcome.clone(),
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
    fn test_audit_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: AuditCommand,
        }

        let cli = TestCli::try_parse_from(["test", "list"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from([
            "test",
            "list",
            "--type",
            "volume_created",
            "--uid",
            "1000",
            "--limit",
            "10",
        ]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from([
            "test",
            "list",
            "--since",
            "2026-01-01T00:00:00Z",
            "--until",
            "2026-12-31T23:59:59Z",
        ]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "list", "--resource", "vol-123"]);
        assert!(cli.is_ok());
    }
}
