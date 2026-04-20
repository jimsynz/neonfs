//! Decision escalation commands
//!
//! Surfaces ambiguous cluster decisions — e.g. quorum loss mid-write, drive
//! flapping, cert expiry on a stale node — for operator review. Each
//! escalation carries a list of possible choices; `resolve` picks one.

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::EscalationEntry;
use crate::term::{extract_error, term_to_list, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Binary, Map, Term};

/// Escalation subcommands
#[derive(Debug, Subcommand)]
pub enum EscalationCommand {
    /// List escalations
    List {
        /// Filter by status (pending, resolved, expired)
        #[arg(long)]
        status: Option<String>,

        /// Filter by category (e.g. quorum_loss, drive_flapping)
        #[arg(long)]
        category: Option<String>,
    },

    /// Show details of a single escalation
    Show {
        /// Escalation ID
        id: String,
    },

    /// Resolve a pending escalation by choosing one of its options
    Resolve {
        /// Escalation ID
        id: String,

        /// Option value to select (see `neonfs escalation show <id>`)
        #[arg(long)]
        choice: String,
    },
}

impl EscalationCommand {
    /// Execute the escalation command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            EscalationCommand::List { status, category } => {
                self.list(status.as_deref(), category.as_deref(), format)
            }
            EscalationCommand::Show { id } => self.show(id, format),
            EscalationCommand::Resolve { id, choice } => self.resolve(id, choice, format),
        }
    }

    fn list(
        &self,
        status: Option<&str>,
        category: Option<&str>,
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

        if let Some(c) = category {
            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"category".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: c.as_bytes().to_vec(),
                }),
            ));
        }

        let filters_term = Term::Map(Map {
            map: filter_entries.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_escalation_list",
                vec![filters_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let entry_terms = term_to_list(&data)?;
        let entries: Result<Vec<EscalationEntry>> = entry_terms
            .into_iter()
            .map(EscalationEntry::from_term)
            .collect();
        let entries = entries?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&entries)?);
            }
            OutputFormat::Table => {
                if entries.is_empty() {
                    println!("No escalations found.");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "ID".to_string(),
                        "CATEGORY".to_string(),
                        "SEVERITY".to_string(),
                        "STATUS".to_string(),
                        "CREATED".to_string(),
                        "DESCRIPTION".to_string(),
                    ]);
                    for entry in &entries {
                        tbl.add_row(vec![
                            entry.id_short(),
                            entry.category.clone(),
                            entry.severity.clone(),
                            entry.status.clone(),
                            entry.created_at.clone(),
                            entry.description.clone(),
                        ]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }

    fn show(&self, id: &str, format: OutputFormat) -> Result<()> {
        let id_term = Term::Binary(Binary {
            bytes: id.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_escalation_show",
                vec![id_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let entry = EscalationEntry::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&entry)?);
            }
            OutputFormat::Table => {
                println!("Escalation {}", entry.id);
                println!("  Category:    {}", entry.category);
                println!("  Severity:    {}", entry.severity);
                println!("  Status:      {}", entry.status);
                println!("  Created:     {}", entry.created_at);
                if let Some(ref expires) = entry.expires_at {
                    println!("  Expires:     {}", expires);
                }
                if let Some(ref resolved) = entry.resolved_at {
                    println!("  Resolved:    {}", resolved);
                }
                if let Some(ref choice) = entry.choice {
                    println!("  Choice:      {}", choice);
                }
                println!("  Description: {}", entry.description);
                if !entry.options.is_empty() {
                    println!("  Options:");
                    for option in &entry.options {
                        println!("    {} — {}", option.value, option.label);
                    }
                }
            }
        }
        Ok(())
    }

    fn resolve(&self, id: &str, choice: &str, format: OutputFormat) -> Result<()> {
        let id_term = Term::Binary(Binary {
            bytes: id.as_bytes().to_vec(),
        });
        let choice_term = Term::Binary(Binary {
            bytes: choice.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_escalation_resolve",
                vec![id_term, choice_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let entry = EscalationEntry::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&entry)?);
            }
            OutputFormat::Table => {
                println!(
                    "Resolved escalation {} with choice '{}'",
                    entry.id,
                    entry.choice.as_deref().unwrap_or(choice)
                );
                if let Some(ref resolved_at) = entry.resolved_at {
                    println!("  Resolved at: {}", resolved_at);
                }
                println!("  Status:      {}", entry.status);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escalation_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: EscalationCommand,
        }

        let cli = TestCli::try_parse_from(["test", "list"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "list", "--status", "pending"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "list", "--category", "quorum_loss"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from([
            "test",
            "list",
            "--status",
            "pending",
            "--category",
            "quorum_loss",
        ]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "show", "esc-1"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "resolve", "esc-1", "--choice", "approve"]);
        assert!(cli.is_ok());

        // Resolve requires --choice
        let cli = TestCli::try_parse_from(["test", "resolve", "esc-1"]);
        assert!(cli.is_err());
    }
}
