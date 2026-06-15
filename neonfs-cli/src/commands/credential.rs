//! Interface-agnostic credential management commands.
//!
//! Credentials are access-key / secret-key pairs bound to an identity and
//! usable by every interface that authenticates against the shared store
//! (S3 SigV4, WebDAV Basic auth).

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::{extract_error, term_to_list, term_to_map, term_to_string, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Atom, Binary, Map, Term};
use serde::Serialize;

/// Credential subcommands
#[derive(Debug, Subcommand)]
pub enum CredentialCommand {
    /// Create a new credential
    Create {
        /// User identity to associate with the credential
        #[arg(long, value_name = "USER")]
        user: String,
    },

    /// List credentials
    List {
        /// Filter by user identity
        #[arg(long)]
        user: Option<String>,
    },

    /// Show details of a credential
    Show {
        /// Access key ID to show
        access_key_id: String,
    },

    /// Rotate the secret key for a credential
    Rotate {
        /// Access key ID to rotate
        access_key_id: String,
    },

    /// Delete a credential
    Delete {
        /// Access key ID to delete
        access_key_id: String,
    },
}

/// Credential information
#[derive(Debug, Serialize)]
struct Credential {
    access_key_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    secret_access_key: Option<String>,
    identity: String,
    created_at: String,
}

impl Credential {
    fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        let identity = match map.get("identity") {
            Some(term) => format_identity(term),
            None => "unknown".to_string(),
        };

        Ok(Self {
            access_key_id: term_to_string(map.get("access_key_id").ok_or_else(|| {
                crate::error::CliError::TermConversionError(
                    "Missing 'access_key_id' field".to_string(),
                )
            })?)?,
            secret_access_key: map
                .get("secret_access_key")
                .and_then(|t| term_to_string(t).ok()),
            identity,
            created_at: map
                .get("created_at")
                .map(|t| term_to_string(t).unwrap_or_default())
                .unwrap_or_default(),
        })
    }
}

fn format_identity(term: &Term) -> String {
    match term_to_map(term) {
        Ok(map) => {
            if let Some(user) = map.get("user") {
                return term_to_string(user).unwrap_or_else(|_| "unknown".to_string());
            }
            format!("{:?}", map.keys().collect::<Vec<_>>())
        }
        Err(_) => term_to_string(term).unwrap_or_else(|_| "unknown".to_string()),
    }
}

fn make_identity_term(user: &str) -> Term {
    Term::Map(Map {
        map: vec![(
            Term::Atom(Atom::from("user")),
            Term::Binary(Binary {
                bytes: user.as_bytes().to_vec(),
            }),
        )]
        .into_iter()
        .collect(),
    })
}

impl CredentialCommand {
    /// Execute the credential command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            CredentialCommand::Create { user } => self.create(user, format),
            CredentialCommand::List { user } => self.list(user.as_deref(), format),
            CredentialCommand::Show { access_key_id } => self.show(access_key_id, format),
            CredentialCommand::Rotate { access_key_id } => self.rotate(access_key_id, format),
            CredentialCommand::Delete { access_key_id } => self.delete(access_key_id, format),
        }
    }

    fn create(&self, user: &str, format: OutputFormat) -> Result<()> {
        let identity = make_identity_term(user);

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_credential_create",
                vec![identity],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let cred = Credential::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&cred)?);
            }
            OutputFormat::Table => {
                println!("Credential created successfully.\n");
                println!("Access Key ID:     {}", cred.access_key_id);
                println!(
                    "Secret Access Key: {}",
                    cred.secret_access_key.as_deref().unwrap_or("(hidden)")
                );
                println!("\nSave the secret access key now — it cannot be retrieved later.");
            }
        }
        Ok(())
    }

    fn list(&self, user: Option<&str>, format: OutputFormat) -> Result<()> {
        let mut filter_entries = vec![];

        if let Some(u) = user {
            let identity = make_identity_term(u);

            filter_entries.push((
                Term::Binary(Binary {
                    bytes: b"identity".to_vec(),
                }),
                identity,
            ));
        }

        let filters_term = Term::Map(Map {
            map: filter_entries.into_iter().collect(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_credential_list",
                vec![filters_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let cred_terms = term_to_list(&data)?;
        let creds: Result<Vec<Credential>> =
            cred_terms.into_iter().map(Credential::from_term).collect();
        let creds = creds?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&creds)?);
            }
            OutputFormat::Table => {
                if creds.is_empty() {
                    println!("No credentials found.");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "ACCESS KEY ID".to_string(),
                        "USER".to_string(),
                        "CREATED".to_string(),
                    ]);
                    for cred in &creds {
                        tbl.add_row(vec![
                            cred.access_key_id.clone(),
                            cred.identity.clone(),
                            cred.created_at.clone(),
                        ]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }

    fn show(&self, access_key_id: &str, format: OutputFormat) -> Result<()> {
        let key_term = Term::Binary(Binary {
            bytes: access_key_id.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_credential_show",
                vec![key_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let cred = Credential::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&cred)?);
            }
            OutputFormat::Table => {
                println!("Access Key ID: {}", cred.access_key_id);
                println!("User:          {}", cred.identity);
                println!("Created:       {}", cred.created_at);
            }
        }
        Ok(())
    }

    fn rotate(&self, access_key_id: &str, format: OutputFormat) -> Result<()> {
        let key_term = Term::Binary(Binary {
            bytes: access_key_id.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_credential_rotate",
                vec![key_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let cred = Credential::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&cred)?);
            }
            OutputFormat::Table => {
                println!("Credential rotated successfully.\n");
                println!("Access Key ID:     {}", cred.access_key_id);
                println!(
                    "Secret Access Key: {}",
                    cred.secret_access_key.as_deref().unwrap_or("(hidden)")
                );
                println!("\nSave the new secret access key now — it cannot be retrieved later.");
            }
        }
        Ok(())
    }

    fn delete(&self, access_key_id: &str, format: OutputFormat) -> Result<()> {
        let key_term = Term::Binary(Binary {
            bytes: access_key_id.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_credential_delete",
                vec![key_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let _ = unwrap_ok_tuple(result)?;

        match format {
            OutputFormat::Json => {
                println!(
                    "{}",
                    json::format(&serde_json::json!({"deleted": access_key_id}))?
                );
            }
            OutputFormat::Table => {
                println!("Deleted credential: {access_key_id}");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credential_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: CredentialCommand,
        }

        let cli = TestCli::try_parse_from(["test", "create", "--user", "alice"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "list"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "list", "--user", "alice"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "delete", "NEONFS_SOME_KEY_ID"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "show", "NEONFS_SOME_KEY_ID"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "rotate", "NEONFS_SOME_KEY_ID"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_credential_from_term() {
        let term = Term::Map(Map {
            map: vec![
                (
                    Term::Atom(Atom::from("access_key_id")),
                    Term::Binary(Binary {
                        bytes: b"NEONFS_TEST123".to_vec(),
                    }),
                ),
                (
                    Term::Atom(Atom::from("secret_access_key")),
                    Term::Binary(Binary {
                        bytes: b"secret123".to_vec(),
                    }),
                ),
                (
                    Term::Atom(Atom::from("identity")),
                    Term::Map(Map {
                        map: vec![(
                            Term::Atom(Atom::from("user")),
                            Term::Binary(Binary {
                                bytes: b"alice".to_vec(),
                            }),
                        )]
                        .into_iter()
                        .collect(),
                    }),
                ),
                (
                    Term::Atom(Atom::from("created_at")),
                    Term::Binary(Binary {
                        bytes: b"2026-04-11T00:00:00Z".to_vec(),
                    }),
                ),
            ]
            .into_iter()
            .collect(),
        });

        let cred = Credential::from_term(term).unwrap();
        assert_eq!(cred.access_key_id, "NEONFS_TEST123");
        assert_eq!(cred.secret_access_key, Some("secret123".to_string()));
        assert_eq!(cred.identity, "alice");
    }

    #[test]
    fn test_credential_without_secret() {
        let term = Term::Map(Map {
            map: vec![
                (
                    Term::Atom(Atom::from("access_key_id")),
                    Term::Binary(Binary {
                        bytes: b"NEONFS_TEST123".to_vec(),
                    }),
                ),
                (
                    Term::Atom(Atom::from("identity")),
                    Term::Binary(Binary {
                        bytes: b"bob".to_vec(),
                    }),
                ),
                (
                    Term::Atom(Atom::from("created_at")),
                    Term::Binary(Binary {
                        bytes: b"2026-04-11T00:00:00Z".to_vec(),
                    }),
                ),
            ]
            .into_iter()
            .collect(),
        });

        let cred = Credential::from_term(term).unwrap();
        assert_eq!(cred.access_key_id, "NEONFS_TEST123");
        assert!(cred.secret_access_key.is_none());
        assert_eq!(cred.identity, "bob");
    }
}
