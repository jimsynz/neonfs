//! Daemon connection module for communicating with the NeonFS daemon.
//!
//! Connects to the NeonFS daemon via Erlang distribution using the `erl_dist`
//! crate directly. Currently uses plain TCP; TLS distribution support will be
//! added once the async stream Clone wrapper is resolved (erl_dist's channel
//! function requires `T: Clone`, which TLS streams don't natively support).
//!
//! Connection flow:
//! 1. EPMD lookup to discover the daemon's distribution port
//! 2. TCP connect to distribution port
//! 3. Erlang distribution handshake (cookie authentication)
//! 4. RPC via SpawnRequest/MonitorPExit (erpc protocol)

use crate::error::{CliError, Result};
use eetf::{Atom, List, Term, Tuple};
use erl_dist::epmd::{EpmdClient, DEFAULT_EPMD_PORT};
use erl_dist::handshake::{ClientSideHandshake, HandshakeStatus};
use erl_dist::message;
use erl_dist::node::{Creation, LocalNode, NodeName};
use erl_dist::term::Mfa;
use erl_dist::DistributionFlags;
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
    sender: message::Sender<smol::net::TcpStream>,
    receiver: message::Receiver<smol::net::TcpStream>,
    local_node_name: String,
    creation: Creation,
    ref_counter: u32,
}

impl DaemonConnection {
    /// Connect to the NeonFS daemon
    pub async fn connect() -> Result<Self> {
        let daemon_node = std::env::var("NEONFS_NODE").unwrap_or_else(|_| {
            fs::read_to_string(RUNTIME_NODE_NAME_PATH)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| DEFAULT_DAEMON_NODE.to_string())
        });
        Self::connect_to(&daemon_node).await
    }

    async fn connect_to(daemon_node: &str) -> Result<Self> {
        let cookie = read_cookie(DEFAULT_COOKIE_PATH)?;
        let node_name: NodeName = daemon_node.parse().map_err(|e| {
            CliError::ConnectionFailed(format!("Invalid node name '{}': {}", daemon_node, e))
        })?;

        // 1. EPMD lookup to get distribution port
        let epmd_host = node_name.host();
        let epmd_addr = format!("{}:{}", epmd_host, DEFAULT_EPMD_PORT);
        let epmd_stream = smol::net::TcpStream::connect(&epmd_addr)
            .await
            .map_err(|e| {
                CliError::ConnectionFailed(format!(
                    "Cannot connect to EPMD at {}: {}. Is the daemon running?",
                    epmd_addr, e
                ))
            })?;

        let epmd_client = EpmdClient::new(epmd_stream);
        let node_entry = epmd_client
            .get_node(node_name.name())
            .await
            .map_err(|e| CliError::ConnectionFailed(format!("EPMD lookup failed: {}", e)))?
            .ok_or_else(|| {
                CliError::ConnectionFailed(format!(
                    "Node '{}' not found in EPMD. Is the daemon running?",
                    daemon_node
                ))
            })?;

        let dist_port = node_entry.port;

        // 2. TCP connect to distribution port
        let dist_addr = format!("{}:{}", epmd_host, dist_port);
        let tcp_stream = smol::net::TcpStream::connect(&dist_addr)
            .await
            .map_err(|e| {
                CliError::ConnectionFailed(format!(
                    "Cannot connect to distribution port {}: {}",
                    dist_addr, e
                ))
            })?;

        // 3. Erlang distribution handshake
        // Use "nonode@localhost" as tentative name — NAME_ME flag means the
        // peer will assign us a real name during handshake.
        let creation = Creation::random();
        let tentative_name: NodeName = "nonode@localhost".parse().map_err(|e| {
            CliError::ConnectionFailed(format!("Failed to create node name: {}", e))
        })?;

        let mut local_node = LocalNode::new(tentative_name, creation);
        // Add flags to the defaults (don't replace — LocalNode::new sets essential flags)
        local_node.flags |= DistributionFlags::NAME_ME;
        local_node.flags |= DistributionFlags::SPAWN;
        local_node.flags |= DistributionFlags::DIST_MONITOR;
        local_node.flags |= DistributionFlags::DIST_MONITOR_NAME;

        let mut handshake = ClientSideHandshake::new(tcp_stream, local_node.clone(), &cookie);
        let status = handshake.execute_send_name(6).await.map_err(|e| {
            CliError::ConnectionFailed(format!("Distribution handshake failed: {}", e))
        })?;

        // Handle NAME_ME response — peer assigns us a name and creation
        match status {
            HandshakeStatus::Named {
                name,
                creation: peer_creation,
            } => {
                local_node.name = NodeName::new(&name, local_node.name.host()).map_err(|e| {
                    CliError::ConnectionFailed(format!("Invalid assigned name: {}", e))
                })?;
                local_node.creation = peer_creation;
            }
            _ => {
                return Err(CliError::ConnectionFailed(format!(
                    "Distribution handshake rejected: {:?}",
                    status
                )));
            }
        }

        let (stream, peer_node) = handshake.execute_rest(true).await.map_err(|e| {
            CliError::ConnectionFailed(format!("Distribution handshake failed: {}", e))
        })?;

        // 4. Create message channel
        let negotiated_flags = local_node.flags & peer_node.flags;
        let (sender, receiver) = message::channel(stream, negotiated_flags);

        Ok(Self {
            sender,
            receiver,
            local_node_name: local_node.name.to_string(),
            creation: local_node.creation,
            ref_counter: 0,
        })
    }

    /// Make an RPC call to the daemon
    pub async fn call(&mut self, module: &str, function: &str, args: Vec<Term>) -> Result<Term> {
        let args_list = Term::List(List { elements: args });

        // Create a unique reference for this call
        self.ref_counter += 1;
        let monitor_ref = eetf::Reference {
            node: Atom::from(self.local_node_name.as_str()),
            id: vec![
                self.ref_counter,
                rand::random::<u32>(),
                rand::random::<u32>(),
            ],
            creation: self.creation.get(),
        };

        let local_pid = eetf::Pid {
            node: Atom::from(self.local_node_name.as_str()),
            id: 0,
            serial: 0,
            creation: self.creation.get(),
        };

        // Build spawn_request to call erpc:execute_call/4 on the remote node
        // erpc:execute_call(ReplyMonitorRef, Module, Function, Args)
        let mfa = Mfa {
            module: Atom::from("erpc"),
            function: Atom::from("execute_call"),
            arity: eetf::FixInteger { value: 4 },
        };

        let opt_list = List {
            elements: vec![Term::Atom(Atom::from("monitor"))],
        };

        // The reply monitor ref is a separate reference passed to erpc:execute_call
        let reply_ref = eetf::Reference {
            node: Atom::from(self.local_node_name.as_str()),
            id: vec![
                self.ref_counter + 1000,
                rand::random::<u32>(),
                rand::random::<u32>(),
            ],
            creation: self.creation.get(),
        };

        let arg_list = List {
            elements: vec![
                Term::Reference(Box::new(reply_ref)),
                Term::Atom(Atom::from(module)),
                Term::Atom(Atom::from(function)),
                args_list,
            ],
        };

        let spawn_msg = message::Message::spawn_request(
            monitor_ref.clone(),
            local_pid.clone(),
            local_pid.clone(), // group_leader = self
            mfa,
            opt_list,
            arg_list,
        );

        self.sender
            .send(spawn_msg)
            .await
            .map_err(|e| CliError::RpcFailed(format!("Failed to send RPC request: {}", e)))?;

        // Wait for SpawnReply
        loop {
            let msg =
                self.receiver.recv().await.map_err(|e| {
                    CliError::RpcFailed(format!("Failed to receive response: {}", e))
                })?;

            match msg {
                message::Message::SpawnReply(..) => break,
                message::Message::Tick => {
                    let _ = self.sender.send(message::Message::Tick).await;
                }
                _ => {}
            }
        }

        // Wait for MonitorPExit containing the result
        loop {
            let msg = self
                .receiver
                .recv()
                .await
                .map_err(|e| CliError::RpcFailed(format!("Failed to receive result: {}", e)))?;

            match msg {
                message::Message::MonitorPExit(exit_msg) => {
                    return extract_erpc_result(exit_msg.reason);
                }
                message::Message::PayloadMonitorPExit(exit_msg) => {
                    return extract_erpc_result(exit_msg.reason);
                }
                message::Message::Tick => {
                    let _ = self.sender.send(message::Message::Tick).await;
                }
                _ => {}
            }
        }
    }
}

/// Extract the result from an erpc response.
///
/// The MonitorPExit reason is a tuple:
/// - `{ref, :return, value}` — successful return
/// - `{ref, :throw | :error | :exit, value}` — error
fn extract_erpc_result(reason: Term) -> Result<Term> {
    match reason {
        Term::Tuple(Tuple { elements }) if elements.len() == 3 => match &elements[1] {
            Term::Atom(Atom { name }) if name == "return" => Ok(elements[2].clone()),
            Term::Atom(Atom { name }) => Err(CliError::RpcFailed(format!(
                "Remote {}: {:?}",
                name, elements[2]
            ))),
            _ => Err(CliError::RpcFailed(format!(
                "Unexpected erpc result: {:?}",
                elements
            ))),
        },
        other => Err(CliError::RpcFailed(format!(
            "Remote process exited: {:?}",
            other
        ))),
    }
}

/// Read the Erlang cookie from environment variable or file
fn read_cookie(path: &str) -> Result<String> {
    if let Ok(cookie) = std::env::var("NEONFS_COOKIE") {
        let cookie = cookie.trim().to_string();
        if !cookie.is_empty() {
            return Ok(cookie);
        }
    }

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
