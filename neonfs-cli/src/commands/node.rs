//! Node management commands

use crate::daemon::DaemonConnection;
use crate::error::{CliError, Result};
use crate::output::{json, table, OutputFormat};
use crate::term::types::NodeInfo;
use crate::term::{extract_error, term_to_list, term_to_map, term_to_string, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Atom, Binary, FixInteger, List, Map, Term};

/// Node management subcommands
#[derive(Debug, Subcommand)]
pub enum NodeCommand {
    /// Show node status
    Status {
        /// Node name (optional, defaults to current node)
        #[arg(long)]
        node: Option<String>,
    },

    /// List all nodes in the cluster
    List,
}

impl NodeCommand {
    /// Execute the node command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            NodeCommand::Status { node: _node } => self.status(format),
            NodeCommand::List => self.list(format),
        }
    }

    fn status(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.Client.CLIHandler",
                "handle_node_status",
                vec![],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let report = term_to_map(&data)?;

        let node = report
            .get("node")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();
        let status = report
            .get("status")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();
        let checked_at = report
            .get("checked_at")
            .map(|t| term_to_string(t).unwrap_or_default())
            .unwrap_or_default();

        let checks = report.get("checks").and_then(|t| term_to_map(t).ok());

        match format {
            OutputFormat::Json => {
                let json_report = term_to_json(&data);
                println!("{}", json::format(&json_report)?);
            }
            OutputFormat::Table => {
                let mut tbl = table::Table::new(vec!["Property".to_string(), "Value".to_string()]);
                tbl.add_row(vec!["Node".to_string(), node.clone()]);
                tbl.add_row(vec!["Status".to_string(), status.clone()]);
                tbl.add_row(vec!["Checked At".to_string(), checked_at]);

                if let Some(ref checks_map) = checks {
                    tbl.add_row(vec!["".to_string(), "".to_string()]);

                    let mut subsystem_names: Vec<&String> = checks_map.keys().collect();
                    subsystem_names.sort();

                    for name in subsystem_names {
                        if let Some(check_term) = checks_map.get(name) {
                            let check_status = term_to_map(check_term)
                                .ok()
                                .and_then(|m| m.get("status").map(|t| term_to_string(t).ok()))
                                .flatten()
                                .unwrap_or_else(|| "unknown".to_string());
                            tbl.add_row(vec![name.clone(), check_status]);
                        }
                    }
                }

                print!("{}", tbl.render()?);
            }
        }

        if status == "unhealthy" {
            return Err(CliError::HealthCheckFailed);
        }

        Ok(())
    }

    fn list(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call("Elixir.NeonFS.CLI.Handler", "handle_node_list", vec![])
                .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let node_terms = term_to_list(&data)?;
        let nodes: Result<Vec<NodeInfo>> =
            node_terms.into_iter().map(NodeInfo::from_term).collect();
        let nodes = nodes?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&nodes)?);
            }
            OutputFormat::Table => {
                if nodes.is_empty() {
                    println!("No nodes found");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "NODE".to_string(),
                        "TYPE".to_string(),
                        "ROLE".to_string(),
                        "STATUS".to_string(),
                        "UPTIME".to_string(),
                    ]);
                    for node in &nodes {
                        tbl.add_row(vec![
                            node.node.clone(),
                            node.node_type.clone(),
                            node.role.clone(),
                            node.status.clone(),
                            node.format_uptime(),
                        ]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }
}

/// Recursively convert an Erlang term to a serde_json::Value.
fn term_to_json(term: &Term) -> serde_json::Value {
    match term {
        Term::Atom(Atom { name }) => match name.as_str() {
            "true" => serde_json::Value::Bool(true),
            "false" => serde_json::Value::Bool(false),
            "nil" => serde_json::Value::Null,
            _ => serde_json::Value::String(name.clone()),
        },
        Term::Binary(Binary { bytes }) => {
            serde_json::Value::String(String::from_utf8_lossy(bytes).to_string())
        }
        Term::FixInteger(FixInteger { value }) => serde_json::json!(*value),
        Term::BigInteger(big) => {
            use num_traits::ToPrimitive;
            if let Some(n) = big.to_i64() {
                serde_json::json!(n)
            } else {
                serde_json::Value::String(big.to_string())
            }
        }
        Term::Float(f) => serde_json::json!(f.value),
        Term::Map(Map { map: entries }) => {
            let mut map = serde_json::Map::new();
            for (key, value) in entries {
                let key_str = match key {
                    Term::Atom(Atom { name }) => name.clone(),
                    Term::Binary(Binary { bytes }) => String::from_utf8_lossy(bytes).to_string(),
                    other => format!("{:?}", other),
                };
                map.insert(key_str, term_to_json(value));
            }
            serde_json::Value::Object(map)
        }
        Term::List(List { elements }) => {
            serde_json::Value::Array(elements.iter().map(term_to_json).collect())
        }
        Term::Tuple(eetf::Tuple { elements }) => {
            serde_json::Value::Array(elements.iter().map(term_to_json).collect())
        }
        _ => serde_json::Value::String(format!("{:?}", term)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_node_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: NodeCommand,
        }

        // Status without --node
        let cli = TestCli::try_parse_from(["test", "status"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            assert!(matches!(parsed.command, NodeCommand::Status { node: None }));
        }

        // Status with --node
        let cli = TestCli::try_parse_from(["test", "status", "--node", "core@host"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            match parsed.command {
                NodeCommand::Status { node } => {
                    assert_eq!(node.as_deref(), Some("core@host"));
                }
                _ => panic!("Expected Status variant"),
            }
        }

        // List
        let cli = TestCli::try_parse_from(["test", "list"]);
        assert!(cli.is_ok());
        if let Ok(parsed) = cli {
            assert!(matches!(parsed.command, NodeCommand::List));
        }
    }

    #[test]
    fn test_term_to_json_atoms() {
        let term = Term::Atom(Atom::from("healthy"));
        assert_eq!(term_to_json(&term), serde_json::json!("healthy"));

        let term = Term::Atom(Atom::from("true"));
        assert_eq!(term_to_json(&term), serde_json::json!(true));

        let term = Term::Atom(Atom::from("nil"));
        assert_eq!(term_to_json(&term), serde_json::Value::Null);
    }

    #[test]
    fn test_term_to_json_map() {
        let term = Term::Map(Map {
            map: HashMap::from([
                (
                    Term::Atom(Atom::from("status")),
                    Term::Atom(Atom::from("healthy")),
                ),
                (
                    Term::Atom(Atom::from("count")),
                    Term::FixInteger(FixInteger::from(42)),
                ),
            ]),
        });
        let json = term_to_json(&term);
        assert_eq!(json["status"], "healthy");
        assert_eq!(json["count"], 42);
    }

    #[test]
    fn test_term_to_json_nested_map() {
        let inner = Term::Map(Map {
            map: HashMap::from([(
                Term::Atom(Atom::from("status")),
                Term::Atom(Atom::from("degraded")),
            )]),
        });
        let term = Term::Map(Map {
            map: HashMap::from([(Term::Atom(Atom::from("cache")), inner)]),
        });
        let json = term_to_json(&term);
        assert_eq!(json["cache"]["status"], "degraded");
    }
}
