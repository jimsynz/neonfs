//! ACL management commands

use crate::daemon::DaemonConnection;
use crate::error::Result;
use crate::output::{json, table, OutputFormat};
use crate::term::types::{AclInfo, FileAclInfo};
use crate::term::{extract_error, unwrap_ok_tuple};
use clap::Subcommand;
use eetf::{Atom, Binary, List, Map, Term};

/// ACL management subcommands
#[derive(Debug, Subcommand)]
pub enum AclCommand {
    /// Grant permissions to a principal on a volume
    Grant {
        /// Volume name
        volume: String,

        /// Principal (uid:N or gid:N)
        principal: String,

        /// Permissions (comma-separated: read,write,admin)
        permissions: String,
    },

    /// Revoke all permissions for a principal on a volume
    Revoke {
        /// Volume name
        volume: String,

        /// Principal (uid:N or gid:N)
        principal: String,
    },

    /// Show volume ACL
    Show {
        /// Volume name
        volume: String,
    },

    /// Set file/directory ACL properties
    SetFile {
        /// Volume name
        volume: String,

        /// File path within the volume
        path: String,

        /// POSIX mode bits (octal, e.g. 755)
        #[arg(long)]
        mode: Option<String>,

        /// Owner UID
        #[arg(long)]
        uid: Option<u64>,
    },

    /// Get file/directory ACL
    GetFile {
        /// Volume name
        volume: String,

        /// File path within the volume
        path: String,
    },
}

impl AclCommand {
    /// Execute the ACL command
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            AclCommand::Grant {
                volume,
                principal,
                permissions,
            } => self.grant(volume, principal, permissions, format),
            AclCommand::Revoke { volume, principal } => self.revoke(volume, principal, format),
            AclCommand::Show { volume } => self.show(volume, format),
            AclCommand::SetFile {
                volume,
                path,
                mode,
                uid,
            } => self.set_file(volume, path, mode.as_deref(), uid.as_ref(), format),
            AclCommand::GetFile { volume, path } => self.get_file(volume, path, format),
        }
    }

    fn grant(
        &self,
        volume: &str,
        principal: &str,
        permissions: &str,
        format: OutputFormat,
    ) -> Result<()> {
        validate_principal(principal)?;
        let perm_list = parse_permissions(permissions)?;

        let runtime = tokio::runtime::Runtime::new()?;

        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });
        let principal_term = Term::Binary(Binary {
            bytes: principal.as_bytes().to_vec(),
        });
        let perms_term = Term::List(List {
            elements: perm_list
                .iter()
                .map(|p| {
                    Term::Binary(Binary {
                        bytes: p.as_bytes().to_vec(),
                    })
                })
                .collect(),
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_acl_grant",
                vec![volume_term, principal_term, perms_term],
            )
            .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "success",
                    "volume": volume,
                    "principal": principal,
                    "permissions": perm_list,
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!(
                    "Granted [{}] to {} on volume '{}'",
                    permissions, principal, volume
                );
            }
        }
        Ok(())
    }

    fn revoke(&self, volume: &str, principal: &str, format: OutputFormat) -> Result<()> {
        validate_principal(principal)?;

        let runtime = tokio::runtime::Runtime::new()?;

        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });
        let principal_term = Term::Binary(Binary {
            bytes: principal.as_bytes().to_vec(),
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_acl_revoke",
                vec![volume_term, principal_term],
            )
            .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "success",
                    "volume": volume,
                    "principal": principal,
                    "message": "Permissions revoked"
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!(
                    "Revoked all permissions for {} on volume '{}'",
                    principal, volume
                );
            }
        }
        Ok(())
    }

    fn show(&self, volume: &str, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_acl_show",
                vec![volume_term],
            )
            .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        let data = unwrap_ok_tuple(result)?;
        let acl = AclInfo::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&acl)?);
            }
            OutputFormat::Table => {
                println!("Volume ACL for '{}'", volume);
                println!("  Owner UID: {}", acl.owner_uid);
                println!("  Owner GID: {}", acl.owner_gid);
                println!();

                if acl.entries.is_empty() {
                    println!("  No additional ACL entries.");
                } else {
                    let mut tbl =
                        table::Table::new(vec!["PRINCIPAL".to_string(), "PERMISSIONS".to_string()]);
                    for entry in &acl.entries {
                        tbl.add_row(vec![entry.principal.clone(), entry.permissions.join(",")]);
                    }
                    print!("{}", tbl.render()?);
                }
            }
        }
        Ok(())
    }

    fn set_file(
        &self,
        volume: &str,
        path: &str,
        mode: Option<&str>,
        uid: Option<&u64>,
        format: OutputFormat,
    ) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });
        let path_term = Term::Binary(Binary {
            bytes: path.as_bytes().to_vec(),
        });

        // Build ACL entries list from provided options
        let mut entries = vec![];

        if let Some(mode_str) = mode {
            let mode_val = u32::from_str_radix(mode_str, 8).map_err(|_| {
                crate::error::CliError::InvalidArgument(format!("Invalid octal mode: {}", mode_str))
            })?;
            entries.push(Term::Map(Map {
                entries: vec![
                    (
                        Term::Atom(Atom::from("type")),
                        Term::Atom(Atom::from("mode")),
                    ),
                    (
                        Term::Atom(Atom::from("value")),
                        Term::FixInteger(eetf::FixInteger::from(mode_val as i32)),
                    ),
                ],
            }));
        }

        if let Some(&uid_val) = uid {
            entries.push(Term::Map(Map {
                entries: vec![
                    (
                        Term::Atom(Atom::from("type")),
                        Term::Atom(Atom::from("uid")),
                    ),
                    (
                        Term::Atom(Atom::from("value")),
                        Term::FixInteger(eetf::FixInteger::from(uid_val as i32)),
                    ),
                ],
            }));
        }

        if entries.is_empty() {
            return Err(crate::error::CliError::InvalidArgument(
                "At least one of --mode or --uid must be specified".to_string(),
            ));
        }

        let entries_term = Term::List(List { elements: entries });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_set_file_acl",
                vec![volume_term, path_term, entries_term],
            )
            .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        match format {
            OutputFormat::Json => {
                let response = serde_json::json!({
                    "status": "success",
                    "volume": volume,
                    "path": path,
                });
                println!("{}", json::format(&response)?);
            }
            OutputFormat::Table => {
                println!("Updated file ACL for '{}' in volume '{}'", path, volume);
            }
        }
        Ok(())
    }

    fn get_file(&self, volume: &str, path: &str, format: OutputFormat) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;

        let volume_term = Term::Binary(Binary {
            bytes: volume.as_bytes().to_vec(),
        });
        let path_term = Term::Binary(Binary {
            bytes: path.as_bytes().to_vec(),
        });

        let result = runtime.block_on(async {
            let mut conn = DaemonConnection::connect().await?;
            conn.call(
                "Elixir.NeonFS.CLI.Handler",
                "handle_get_file_acl",
                vec![volume_term, path_term],
            )
            .await
        })?;

        if let Some(err_msg) = extract_error(&result) {
            return Err(crate::error::CliError::RpcError(err_msg));
        }

        let data = unwrap_ok_tuple(result)?;
        let file_acl = FileAclInfo::from_term(data)?;

        match format {
            OutputFormat::Json => {
                println!("{}", json::format(&file_acl)?);
            }
            OutputFormat::Table => {
                let mut tbl = table::Table::new(vec!["Property".to_string(), "Value".to_string()]);
                tbl.add_row(vec!["Mode".to_string(), format!("{:04o}", file_acl.mode)]);
                tbl.add_row(vec!["Owner UID".to_string(), file_acl.uid.to_string()]);
                tbl.add_row(vec!["Owner GID".to_string(), file_acl.gid.to_string()]);
                print!("{}", tbl.render()?);

                if !file_acl.acl_entries.is_empty() {
                    println!("\nExtended ACL entries:");
                    for entry in &file_acl.acl_entries {
                        println!("  {}", entry);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Validate principal format (uid:N or gid:N)
fn validate_principal(principal: &str) -> Result<()> {
    if let Some(uid_str) = principal.strip_prefix("uid:") {
        uid_str.parse::<u64>().map_err(|_| {
            crate::error::CliError::InvalidArgument(format!("Invalid UID: {}", uid_str))
        })?;
        Ok(())
    } else if let Some(gid_str) = principal.strip_prefix("gid:") {
        gid_str.parse::<u64>().map_err(|_| {
            crate::error::CliError::InvalidArgument(format!("Invalid GID: {}", gid_str))
        })?;
        Ok(())
    } else {
        Err(crate::error::CliError::InvalidArgument(
            "Principal must be in format uid:N or gid:N".to_string(),
        ))
    }
}

/// Parse comma-separated permissions string
fn parse_permissions(permissions: &str) -> Result<Vec<String>> {
    let valid = ["read", "write", "admin"];
    let perms: Vec<String> = permissions
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    for perm in &perms {
        if !valid.contains(&perm.as_str()) {
            return Err(crate::error::CliError::InvalidArgument(format!(
                "Invalid permission '{}'. Valid: read, write, admin",
                perm
            )));
        }
    }

    Ok(perms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_principal_uid() {
        assert!(validate_principal("uid:1000").is_ok());
        assert!(validate_principal("uid:0").is_ok());
    }

    #[test]
    fn test_validate_principal_gid() {
        assert!(validate_principal("gid:100").is_ok());
        assert!(validate_principal("gid:0").is_ok());
    }

    #[test]
    fn test_validate_principal_invalid() {
        assert!(validate_principal("user:1000").is_err());
        assert!(validate_principal("uid:abc").is_err());
        assert!(validate_principal("1000").is_err());
    }

    #[test]
    fn test_parse_permissions_valid() {
        let perms = parse_permissions("read,write").unwrap();
        assert_eq!(perms, vec!["read", "write"]);
    }

    #[test]
    fn test_parse_permissions_single() {
        let perms = parse_permissions("admin").unwrap();
        assert_eq!(perms, vec!["admin"]);
    }

    #[test]
    fn test_parse_permissions_invalid() {
        assert!(parse_permissions("read,delete").is_err());
    }

    #[test]
    fn test_parse_permissions_with_spaces() {
        let perms = parse_permissions("read, write, admin").unwrap();
        assert_eq!(perms, vec!["read", "write", "admin"]);
    }

    #[test]
    fn test_acl_command_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: AclCommand,
        }

        let cli = TestCli::try_parse_from(["test", "grant", "myvol", "uid:1000", "read,write"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "revoke", "myvol", "gid:100"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "show", "myvol"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "set-file", "myvol", "/path", "--mode", "755"]);
        assert!(cli.is_ok());

        let cli = TestCli::try_parse_from(["test", "get-file", "myvol", "/path"]);
        assert!(cli.is_ok());
    }
}
