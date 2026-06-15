//! S3 bucket management commands.
//!
//! Credential lifecycle is interface-agnostic and lives in the
//! `credential` command group (`crate::commands::credential`).

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::{
    extract_error, term_to_list, term_to_map, term_to_string, term_to_u64, unwrap_ok_tuple,
};
use clap::Subcommand;
use eetf::{Binary, Term};
use serde::Serialize;

/// S3 subcommands
#[derive(Debug, Subcommand)]
pub enum S3Command {
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
            S3Command::Bucket { command } => command.execute(format),
        }
    }
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
    use eetf::{Atom, Map};

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
