//! S3 credential and bucket management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::{
    extract_error, term_to_list, term_to_map, term_to_string, term_to_u64, unwrap_ok_tuple,
};
use clap::Subcommand;
use eetf::{Atom, Binary, Map, Term};
use serde::Serialize;

/// S3 subcommands
#[derive(Debug, Subcommand)]
pub enum S3Command {
    /// Create a new S3 credential
    CreateCredential {
        /// User identity to associate with the credential
        #[arg(long, value_name = "USER")]
        user: String,
    },

    /// List S3 credentials
    ListCredentials {
        /// Filter by user identity
        #[arg(long)]
        user: Option<String>,
    },

    /// Show details of an S3 credential
    ShowCredential {
        /// Access key ID to show
        access_key_id: String,
    },

    /// Rotate the secret key for an S3 credential
    RotateCredential {
        /// Access key ID to rotate
        access_key_id: String,
    },

    /// Delete an S3 credential
    DeleteCredential {
        /// Access key ID to delete
        access_key_id: String,
    },

    /// S3 bucket management
    Bucket {
        #[command(subcommand)]
        command: BucketCommand,
    },
}

/// Bucket subcommands
#[derive(Debug, Subcommand)]
pub enum BucketCommand {
    /// List all buckets (volumes available via S3)
    List,

    /// Show bucket details
    Show {
        /// Bucket name
        name: String,
    },
}

/// S3 credential information
#[derive(Debug, Serialize)]
struct S3Credential {
    access_key_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    secret_access_key: Option<String>,
    identity: String,
    created_at: String,
}

impl S3Credential {
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

/// S3 bucket information
#[derive(Debug, Serialize)]
struct S3Bucket {
    name: String,
    created_at: String,
    compression: String,
    logical_size: u64,
    physical_size: u64,
}

impl S3Bucket {
    fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            name: term_to_string(map.get("name").ok_or_else(|| {
                crate::error::CliError::TermConversionError("Missing 'name' field".to_string())
            })?)?,
            created_at: map
                .get("created_at")
                .map(|t| term_to_string(t).unwrap_or_default())
                .unwrap_or_default(),
            compression: map
                .get("compression")
                .map(|t| term_to_string(t).unwrap_or_default())
                .unwrap_or_default(),
            logical_size: map
                .get("logical_size")
                .and_then(|t| term_to_u64(t).ok())
                .unwrap_or(0),
            physical_size: map
                .get("physical_size")
                .and_then(|t| term_to_u64(t).ok())
                .unwrap_or(0),
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

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GiB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MiB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1_024 {
        format!("{:.1} KiB", bytes as f64 / 1_024.0)
    } else {
        format!("{bytes} B")
    }
}

impl S3Command {
    /// Execute the S3 command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            S3Command::CreateCredential { user } => self.create_credential(user, format),
            S3Command::ListCredentials { user } => self.list_credentials(user.as_deref(), format),
            S3Command::ShowCredential { access_key_id } => {
                self.show_credential(access_key_id, format)
            }
            S3Command::RotateCredential { access_key_id } => {
                self.rotate_credential(access_key_id, format)
            }
            S3Command::DeleteCredential { access_key_id } => {
                self.delete_credential(access_key_id, format)
            }
            S3Command::Bucket { command } => command.execute(format),
        }
    }

    fn create_credential(&self, user: &str, format: OutputFormat) -> Result<()> {
        let identity = make_identity_term(user);

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_s3_create_credential",
                vec![identity],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let cred = S3Credential::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&cred)?);
            }
            OutputFormat::Table => {
                println!("S3 credential created successfully.\n");
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

    fn list_credentials(&self, user: Option<&str>, format: OutputFormat) -> Result<()> {
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
                "handle_s3_list_credentials",
                vec![filters_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let cred_terms = term_to_list(&data)?;
        let creds: Result<Vec<S3Credential>> = cred_terms
            .into_iter()
            .map(S3Credential::from_term)
            .collect();
        let creds = creds?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&creds)?);
            }
            OutputFormat::Table => {
                if creds.is_empty() {
                    println!("No S3 credentials found.");
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

    fn show_credential(&self, access_key_id: &str, format: OutputFormat) -> Result<()> {
        let key_term = Term::Binary(Binary {
            bytes: access_key_id.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_s3_show_credential",
                vec![key_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let cred = S3Credential::from_term(data)?;

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

    fn rotate_credential(&self, access_key_id: &str, format: OutputFormat) -> Result<()> {
        let key_term = Term::Binary(Binary {
            bytes: access_key_id.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_s3_rotate_credential",
                vec![key_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let cred = S3Credential::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&cred)?);
            }
            OutputFormat::Table => {
                println!("S3 credential rotated successfully.\n");
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

    fn delete_credential(&self, access_key_id: &str, format: OutputFormat) -> Result<()> {
        let key_term = Term::Binary(Binary {
            bytes: access_key_id.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_s3_delete_credential",
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
                println!("Deleted S3 credential: {access_key_id}");
            }
        }
        Ok(())
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

impl BucketCommand {
    fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            BucketCommand::List => self.list_buckets(format),
            BucketCommand::Show { name } => self.show_bucket(name, format),
        }
    }

    fn list_buckets(&self, format: OutputFormat) -> Result<()> {
        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_s3_list_buckets",
                vec![],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let bucket_terms = term_to_list(&data)?;
        let buckets: Result<Vec<S3Bucket>> =
            bucket_terms.into_iter().map(S3Bucket::from_term).collect();
        let buckets = buckets?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&buckets)?);
            }
            OutputFormat::Table => {
                if buckets.is_empty() {
                    println!("No buckets found.");
                } else {
                    let mut tbl = table::Table::new(vec![
                        "BUCKET".to_string(),
                        "SIZE".to_string(),
                        "COMPRESSION".to_string(),
                        "CREATED".to_string(),
                    ]);
                    for bucket in &buckets {
                        tbl.add_row(vec![
                            bucket.name.clone(),
                            format_bytes(bucket.logical_size),
                            bucket.compression.clone(),
                            bucket.created_at.clone(),
                        ]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }

    fn show_bucket(&self, name: &str, format: OutputFormat) -> Result<()> {
        let name_term = Term::Binary(Binary {
            bytes: name.as_bytes().to_vec(),
        });

        let result = smol::block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_s3_show_bucket",
                vec![name_term],
            )
            .await
        })?;

        if let Some(err) = extract_error(&result) {
            return Err(err);
        }

        let data = unwrap_ok_tuple(result)?;
        let bucket = S3Bucket::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&bucket)?);
            }
            OutputFormat::Table => {
                println!("Bucket:        {}", bucket.name);
                println!("Logical Size:  {}", format_bytes(bucket.logical_size));
                println!("Physical Size: {}", format_bytes(bucket.physical_size));
                println!("Compression:   {}", bucket.compression);
                println!("Created:       {}", bucket.created_at);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: S3Command,
        }

        let cli = TestCli::try_parse_from(["test", "create-credential", "--user", "alice"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "list-credentials"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "list-credentials", "--user", "alice"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "delete-credential", "NEONFS_SOME_KEY_ID"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "show-credential", "NEONFS_SOME_KEY_ID"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "rotate-credential", "NEONFS_SOME_KEY_ID"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_bucket_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: S3Command,
        }

        let cli = TestCli::try_parse_from(["test", "bucket", "list"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "bucket", "show", "my-bucket"]);
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

        let cred = S3Credential::from_term(term).unwrap();
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

        let cred = S3Credential::from_term(term).unwrap();
        assert_eq!(cred.access_key_id, "NEONFS_TEST123");
        assert!(cred.secret_access_key.is_none());
        assert_eq!(cred.identity, "bob");
    }

    #[test]
    fn test_bucket_from_term() {
        use eetf::FixInteger;

        let term = Term::Map(Map {
            map: vec![
                (
                    Term::Atom(Atom::from("name")),
                    Term::Binary(Binary {
                        bytes: b"my-bucket".to_vec(),
                    }),
                ),
                (
                    Term::Atom(Atom::from("created_at")),
                    Term::Binary(Binary {
                        bytes: b"2026-04-11T00:00:00Z".to_vec(),
                    }),
                ),
                (
                    Term::Atom(Atom::from("compression")),
                    Term::Atom(Atom::from("zstd")),
                ),
                (
                    Term::Atom(Atom::from("logical_size")),
                    Term::FixInteger(FixInteger::from(1048576)),
                ),
                (
                    Term::Atom(Atom::from("physical_size")),
                    Term::FixInteger(FixInteger::from(524288)),
                ),
            ]
            .into_iter()
            .collect(),
        });

        let bucket = S3Bucket::from_term(term).unwrap();
        assert_eq!(bucket.name, "my-bucket");
        assert_eq!(bucket.compression, "zstd");
        assert_eq!(bucket.logical_size, 1048576);
        assert_eq!(bucket.physical_size, 524288);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(1_048_576), "1.0 MiB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GiB");
        assert_eq!(format_bytes(2_684_354_560), "2.5 GiB");
    }
}
