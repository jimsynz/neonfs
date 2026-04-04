//! Daemon connection module for communicating with the NeonFS daemon.
//!
//! This module implements connection to the NeonFS daemon using:
//! - Cookie-based authentication (Erlang standard)
//! - EPMD (Erlang Port Mapper Daemon) for node discovery
//! - Full Erlang distribution protocol via erl_rpc crate
//!
//! The CLI connects as an Erlang node and makes RPC calls using the standard
//! Erlang `:rpc.call/4` mechanism via the `erl_rpc` high-level client.

use crate::error::{CliError, Result};
use eetf::Term;
use erl_rpc::RpcClient;
use std::fs;
use std::path::Path;

/// Default cookie file location
const DEFAULT_COOKIE_PATH: &str = "/var/lib/neonfs/.erlang.cookie";

/// Default daemon node name (last resort fallback)
const DEFAULT_DAEMON_NODE: &str = "neonfs@localhost";

/// Runtime file written by the daemon wrapper with the actual node name
const RUNTIME_NODE_NAME_PATH: &str = "/run/neonfs/core_node_name";

/// Connection to the NeonFS daemon via Erlang distribution
pub struct DaemonConnection {
    handle: erl_rpc::RpcClientHandle,
}

impl DaemonConnection {
    /// Connect to the NeonFS daemon
    ///
    /// This performs the following steps:
    /// 1. Read the authentication cookie (from NEONFS_COOKIE env or file)
    /// 2. Determine daemon node (from NEONFS_NODE env or default)
    /// 3. Connect to the daemon via Erlang distribution
    /// 4. Start the RPC client as a background task
    pub async fn connect() -> Result<Self> {
        let daemon_node = std::env::var("NEONFS_NODE").unwrap_or_else(|_| {
            fs::read_to_string(RUNTIME_NODE_NAME_PATH)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| DEFAULT_DAEMON_NODE.to_string())
        });
        Self::connect_with_options(DEFAULT_COOKIE_PATH, &daemon_node).await
    }

    /// Connect with custom options (for testing or multi-node setups)
    pub async fn connect_with_options(cookie_path: &str, daemon_node: &str) -> Result<Self> {
        let cookie = read_cookie(cookie_path)?;

        // Connect to the Erlang node using erl_rpc
        let client = RpcClient::connect(daemon_node, &cookie)
            .await
            .map_err(|e| {
                CliError::ConnectionFailed(format!("Failed to connect to daemon: {}", e))
            })?;

        let handle = client.handle();

        // Run the RPC client as a background task
        tokio::spawn(async move {
            if let Err(e) = client.run().await {
                eprintln!("RPC client error: {}", e);
            }
        });

        Ok(Self { handle })
    }

    /// Make an RPC call to the daemon
    ///
    /// Calls `module:function(args)` on the daemon using Erlang distribution.
    ///
    /// # Arguments
    /// * `module` - Elixir module name (e.g., "Elixir.NeonFS.CLI.Handler")
    /// * `function` - Function name (e.g., "list_volumes")
    /// * `args` - List of arguments as Erlang terms
    pub async fn call(&mut self, module: &str, function: &str, args: Vec<Term>) -> Result<Term> {
        // Convert args to eetf::List
        let args_list = eetf::List { elements: args };

        // Make RPC call
        self.handle
            .call(module.into(), function.into(), args_list)
            .await
            .map_err(|e| CliError::RpcFailed(format!("RPC call failed: {}", e)))
    }
}

/// Read the Erlang cookie from environment variable or file
///
/// Checks in order:
/// 1. NEONFS_COOKIE environment variable
/// 2. Cookie file at the given path
fn read_cookie(path: &str) -> Result<String> {
    // First check environment variable
    if let Ok(cookie) = std::env::var("NEONFS_COOKIE") {
        let cookie = cookie.trim().to_string();
        if !cookie.is_empty() {
            return Ok(cookie);
        }
    }

    // Fall back to file
    let path_obj = Path::new(path);

    if !path_obj.exists() {
        return Err(CliError::CookieNotFound(format!(
            "Cookie file not found at {}. Is the NeonFS daemon running?",
            path
        )));
    }

    fs::read_to_string(path_obj)
        .map(|s| s.trim().to_string())
        .map_err(|e| CliError::CookieReadError(format!("Failed to read cookie file: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_read_cookie_success() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "test_cookie_123").unwrap();

        let cookie = read_cookie(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(cookie, "test_cookie_123");
    }

    #[test]
    fn test_read_cookie_trims_whitespace() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "  test_cookie_456  ").unwrap();

        let cookie = read_cookie(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(cookie, "test_cookie_456");
    }

    #[test]
    fn test_read_cookie_not_found() {
        let result = read_cookie("/nonexistent/path/.erlang.cookie");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CliError::CookieNotFound(_)));
    }
}
