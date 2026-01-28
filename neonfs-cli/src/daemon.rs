//! Daemon connection module for communicating with the NeonFS daemon.
//!
//! This module implements connection to the NeonFS daemon using:
//! - Cookie-based authentication (Erlang standard)
//! - EPMD (Erlang Port Mapper Daemon) for node discovery
//! - ETF (Erlang External Term Format) for RPC encoding
//!
//! Phase 1 uses a simplified RPC protocol over TCP with ETF encoding.
//! Phase 2 may upgrade to full Erlang distribution protocol if needed.

use crate::error::{CliError, Result};
use eetf::{Atom, Binary, List, Term, Tuple};
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Default cookie file location
#[allow(dead_code)]
const DEFAULT_COOKIE_PATH: &str = "/var/lib/neonfs/.erlang.cookie";

/// Default EPMD port (standard Erlang)
const EPMD_PORT: u16 = 4369;

/// Default RPC timeout
#[allow(dead_code)]
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Connection to the NeonFS daemon
pub struct DaemonConnection {
    node_name: String,
    rpc_port: u16,
    #[allow(dead_code)]
    cookie: String,
}

impl DaemonConnection {
    /// Connect to the NeonFS daemon
    ///
    /// This performs the following steps:
    /// 1. Read the authentication cookie
    /// 2. Look up the daemon node via EPMD
    /// 3. Establish connection
    #[allow(dead_code)]
    pub async fn connect() -> Result<Self> {
        let cookie = read_cookie(DEFAULT_COOKIE_PATH)?;
        let (node_name, rpc_port) = epmd_lookup("neonfs").await?;

        Ok(Self {
            node_name,
            rpc_port,
            cookie,
        })
    }

    /// Connect with custom cookie path (for testing)
    #[allow(dead_code)]
    pub async fn connect_with_cookie_path(cookie_path: &str) -> Result<Self> {
        let cookie = read_cookie(cookie_path)?;
        let (node_name, rpc_port) = epmd_lookup("neonfs").await?;

        Ok(Self {
            node_name,
            rpc_port,
            cookie,
        })
    }

    /// Make an RPC call to the daemon
    ///
    /// Calls `module:function(args)` on the daemon and returns the result.
    ///
    /// # Arguments
    /// * `module` - Elixir module name (e.g., "NeonFS.CLI.Handler")
    /// * `function` - Function name (e.g., "list_volumes")
    /// * `args` - List of arguments as Erlang terms
    #[allow(dead_code)]
    pub async fn call(&self, module: &str, function: &str, args: Vec<Term>) -> Result<Term> {
        // Connect to the daemon's RPC port
        let addr = format!("127.0.0.1:{}", self.rpc_port);
        let mut stream = TcpStream::connect(&addr).await.map_err(|e| {
            CliError::ConnectionFailed(format!("Failed to connect to daemon: {}", e))
        })?;

        // Build RPC request
        let request = build_rpc_request(module, function, args, &self.cookie)?;

        // Send request with timeout
        tokio::time::timeout(RPC_TIMEOUT, async {
            // Send request length (4 bytes, big-endian)
            let request_len = request.len() as u32;
            stream.write_all(&request_len.to_be_bytes()).await?;

            // Send request data
            stream.write_all(&request).await?;

            // Read response length
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await?;
            let response_len = u32::from_be_bytes(len_buf) as usize;

            // Validate response size
            if response_len > 10_000_000 {
                // 10MB limit
                return Err(CliError::RpcError("Response too large".to_string()));
            }

            // Read response data
            let mut response_buf = vec![0u8; response_len];
            stream.read_exact(&mut response_buf).await?;

            // Decode response
            let mut cursor = Cursor::new(response_buf);
            let response = eetf::Term::decode(&mut cursor)
                .map_err(|e| CliError::RpcError(format!("Failed to decode response: {}", e)))?;

            Ok::<Term, CliError>(response)
        })
        .await
        .map_err(|_| CliError::Timeout)?
    }

    /// Get node name
    #[allow(dead_code)]
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    /// Get RPC port
    #[allow(dead_code)]
    pub fn rpc_port(&self) -> u16 {
        self.rpc_port
    }
}

/// Read the Erlang cookie from the specified path
fn read_cookie<P: AsRef<Path>>(path: P) -> Result<String> {
    let path = path.as_ref();

    // Check if file exists
    if !path.exists() {
        return Err(CliError::CookieNotFound(
            "NeonFS not initialised. Run: neonfs cluster init".to_string(),
        ));
    }

    // Try to read the file
    let cookie = fs::read_to_string(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            CliError::CookiePermissionDenied(
                "Permission denied reading cookie. Run as neonfs user or use sudo.".to_string(),
            )
        } else {
            CliError::CookieReadError(format!("Failed to read cookie: {}", e))
        }
    })?;

    // Trim whitespace
    let cookie = cookie.trim().to_string();

    // Validate cookie is not empty
    if cookie.is_empty() {
        return Err(CliError::CookieReadError(
            "Cookie file is empty".to_string(),
        ));
    }

    Ok(cookie)
}

/// Look up a node in EPMD
///
/// Returns the node's full name and port number.
async fn epmd_lookup(node_name: &str) -> Result<(String, u16)> {
    let addr = format!("127.0.0.1:{}", EPMD_PORT);

    // Connect to EPMD
    let mut stream = TcpStream::connect(&addr).await.map_err(|_| {
        CliError::EpmdNotRunning(
            "Cannot connect to NeonFS daemon. Is the service running?".to_string(),
        )
    })?;

    // Build EPMD PORT_PLEASE2_REQ message
    // Format: [length:16, 'z', name_len:..., name:...]
    let name_bytes = node_name.as_bytes();
    let name_len = name_bytes.len();
    let msg_len = 1 + name_len; // 'z' + name

    let mut request = Vec::with_capacity(2 + msg_len);
    request.extend_from_slice(&(msg_len as u16).to_be_bytes());
    request.push(b'z'); // PORT_PLEASE2_REQ
    request.extend_from_slice(name_bytes);

    // Send request
    stream
        .write_all(&request)
        .await
        .map_err(|e| CliError::EpmdLookupFailed(format!("Failed to send EPMD request: {}", e)))?;

    // Read response
    // Format: ['w', Result, ...]
    let mut response = vec![0u8; 1024];
    let n = stream
        .read(&mut response)
        .await
        .map_err(|e| CliError::EpmdLookupFailed(format!("Failed to read EPMD response: {}", e)))?;

    if n < 2 {
        return Err(CliError::EpmdLookupFailed(
            "Invalid EPMD response".to_string(),
        ));
    }

    // Check response type
    if response[0] != b'w' {
        return Err(CliError::EpmdLookupFailed(format!(
            "Unexpected EPMD response type: {}",
            response[0]
        )));
    }

    // Check result code
    if response[1] != 0 {
        return Err(CliError::NodeNotFound(
            "NeonFS daemon not found. The service may be starting.".to_string(),
        ));
    }

    // Parse port number (bytes 2-3, big-endian)
    if n < 4 {
        return Err(CliError::EpmdLookupFailed(
            "EPMD response too short".to_string(),
        ));
    }

    let port = u16::from_be_bytes([response[2], response[3]]);

    // For Phase 1, we use a simplified protocol where the RPC port is daemon's port + 1
    // In Phase 2, we'll use the actual distribution protocol
    let rpc_port = port + 1;

    let full_node_name = format!("{}@127.0.0.1", node_name);

    Ok((full_node_name, rpc_port))
}

/// Build an RPC request encoded in ETF
///
/// Request format: {:rpc, cookie, module, function, args}
#[allow(dead_code)]
fn build_rpc_request(
    module: &str,
    function: &str,
    args: Vec<Term>,
    cookie: &str,
) -> Result<Vec<u8>> {
    // Build request tuple
    let request = Term::Tuple(Tuple {
        elements: vec![
            Term::Atom(Atom::from("rpc")),
            Term::Binary(Binary {
                bytes: cookie.as_bytes().to_vec(),
            }),
            Term::Binary(Binary {
                bytes: module.as_bytes().to_vec(),
            }),
            Term::Binary(Binary {
                bytes: function.as_bytes().to_vec(),
            }),
            Term::List(List { elements: args }),
        ],
    });

    // Encode to ETF
    let mut buf = Vec::new();
    request
        .encode(&mut buf)
        .map_err(|e| CliError::RpcError(format!("Failed to encode request: {}", e)))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_read_cookie_success() {
        let temp_dir = TempDir::new().unwrap();
        let cookie_path = temp_dir.path().join(".erlang.cookie");

        // Write test cookie
        let mut file = fs::File::create(&cookie_path).unwrap();
        file.write_all(b"test-cookie-123").unwrap();
        drop(file);

        let cookie = read_cookie(&cookie_path).unwrap();
        assert_eq!(cookie, "test-cookie-123");
    }

    #[test]
    fn test_read_cookie_with_whitespace() {
        let temp_dir = TempDir::new().unwrap();
        let cookie_path = temp_dir.path().join(".erlang.cookie");

        // Write cookie with whitespace
        let mut file = fs::File::create(&cookie_path).unwrap();
        file.write_all(b"  test-cookie-123\n\n").unwrap();
        drop(file);

        let cookie = read_cookie(&cookie_path).unwrap();
        assert_eq!(cookie, "test-cookie-123");
    }

    #[test]
    fn test_read_cookie_not_found() {
        let result = read_cookie("/nonexistent/path/.erlang.cookie");
        assert!(matches!(result, Err(CliError::CookieNotFound(_))));
    }

    #[test]
    fn test_read_cookie_empty() {
        let temp_dir = TempDir::new().unwrap();
        let cookie_path = temp_dir.path().join(".erlang.cookie");

        // Write empty file
        fs::File::create(&cookie_path).unwrap();

        let result = read_cookie(&cookie_path);
        assert!(matches!(result, Err(CliError::CookieReadError(_))));
    }

    #[test]
    fn test_build_rpc_request() {
        let request =
            build_rpc_request("NeonFS.CLI.Handler", "list_volumes", vec![], "test-cookie").unwrap();

        // Decode and verify
        let mut cursor = Cursor::new(request);
        let term = Term::decode(&mut cursor).unwrap();
        match term {
            Term::Tuple(fields) => {
                assert_eq!(fields.elements.len(), 5);
                assert_eq!(fields.elements[0], Term::Atom(Atom::from("rpc")));
            }
            _ => panic!("Expected tuple"),
        }
    }

    #[test]
    fn test_build_rpc_request_with_args() {
        use eetf::FixInteger;
        let args = vec![
            Term::Atom(Atom::from("volume1")),
            Term::FixInteger(FixInteger::from(42)),
        ];

        let request = build_rpc_request(
            "NeonFS.CLI.Handler",
            "get_volume",
            args.clone(),
            "test-cookie",
        )
        .unwrap();

        // Decode and verify
        let mut cursor = Cursor::new(request);
        let term = Term::decode(&mut cursor).unwrap();
        match term {
            Term::Tuple(fields) => {
                assert_eq!(fields.elements.len(), 5);
                if let Term::List(decoded_args) = &fields.elements[4] {
                    assert_eq!(decoded_args.elements.len(), args.len());
                } else {
                    panic!("Expected list for args");
                }
            }
            _ => panic!("Expected tuple"),
        }
    }
}
