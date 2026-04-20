//! Daemon connection module for communicating with the NeonFS daemon.
//!
//! Connects to the NeonFS daemon via Erlang distribution using the `erl_dist`
//! crate directly. Uses TLS when the daemon has TLS distribution configured
//! (ssl_dist.conf present), falling back to plain TCP otherwise.
//!
//! Connection flow:
//! 1. Read distribution port from env or runtime file
//! 2. TCP connect to distribution port
//! 3. TLS handshake if ssl_dist.conf exists (using cli.crt/cli.key)
//! 4. Erlang distribution handshake (cookie authentication)
//! 5. RPC via SpawnRequest/MonitorPExit (erpc protocol)

use crate::error::{CliError, Result};
use crate::tls;
use eetf::{Atom, List, Term, Tuple};
use erl_dist::handshake::{ClientSideHandshake, HandshakeStatus};
use erl_dist::message;
use erl_dist::node::{Creation, LocalNode, NodeName};
use erl_dist::term::Mfa;
use erl_dist::DistributionFlags;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::fs;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Default cookie file location
const DEFAULT_COOKIE_PATH: &str = "/var/lib/neonfs/.erlang.cookie";

/// Default daemon node name (last resort fallback)
const DEFAULT_DAEMON_NODE: &str = "neonfs@localhost";

/// Runtime file written by the daemon wrapper with the actual node name
const RUNTIME_NODE_NAME_PATH: &str = "/run/neonfs/core_node_name";

/// Runtime file written by the daemon wrapper with the distribution port
const RUNTIME_DIST_PORT_PATH: &str = "/run/neonfs/dist_port";

// -- Stream wrappers for erl_dist compatibility --
//
// `erl_dist::message::channel()` requires `T: Clone` so it can give one
// handle to Sender and another to Receiver. `smol::net::TcpStream` is
// Clone (internal Arc), but TLS streams are not. `SharedStream` wraps any
// async stream in Arc<Mutex<T>> to satisfy the Clone bound. This is safe
// because our CLI uses Sender and Receiver sequentially (never concurrently).

struct SharedStream<T>(Arc<Mutex<T>>);

impl<T> SharedStream<T> {
    fn new(stream: T) -> Self {
        Self(Arc::new(Mutex::new(stream)))
    }
}

impl<T> Clone for SharedStream<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: Unpin> Unpin for SharedStream<T> {}

impl<T: AsyncRead + Unpin> AsyncRead for SharedStream<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut inner = self.0.lock().unwrap();
        Pin::new(&mut *inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for SharedStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut inner = self.0.lock().unwrap();
        Pin::new(&mut *inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut inner = self.0.lock().unwrap();
        Pin::new(&mut *inner).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut inner = self.0.lock().unwrap();
        Pin::new(&mut *inner).poll_close(cx)
    }
}

type TlsStream = SharedStream<futures_rustls::client::TlsStream<smol::net::TcpStream>>;

/// Distribution stream — TLS when available, plain TCP otherwise.
#[derive(Clone)]
enum DistStream {
    Tcp(smol::net::TcpStream),
    Tls(TlsStream),
}

impl AsyncRead for DistStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            DistStream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            DistStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for DistStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            DistStream::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            DistStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            DistStream::Tcp(s) => Pin::new(s).poll_flush(cx),
            DistStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            DistStream::Tcp(s) => Pin::new(s).poll_close(cx),
            DistStream::Tls(s) => Pin::new(s).poll_close(cx),
        }
    }
}

impl Unpin for DistStream {}

/// Connection to the NeonFS daemon via Erlang distribution
pub struct DaemonConnection {
    sender: message::Sender<DistStream>,
    receiver: message::Receiver<DistStream>,
    local_node_name: String,
    creation: Creation,
    ref_counter: u32,
}

impl DaemonConnection {
    /// Connect to the NeonFS daemon
    pub async fn connect() -> Result<Self> {
        let daemon_node = std::env::var("NEONFS_NODE").unwrap_or_else(|_| {
            // audit:bounded runtime node name file is a single short atom
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

        // 1. Read distribution port from env or runtime file
        let epmd_host = node_name.host();
        let dist_port = read_dist_port()?;

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

        let mut local_node = make_local_node();

        // 3. TLS handshake (if configured) + distribution handshake
        //    The distribution handshake doesn't need Clone — only channel()
        //    does — so we use the raw stream for the handshake and wrap in
        //    SharedStream/DistStream only for the message channel.
        if tls::tls_configured() {
            let tls_config = tls::build_client_config()?;
            let connector = futures_rustls::TlsConnector::from(tls_config);
            let server_name = rustls::pki_types::ServerName::try_from("localhost")
                .map_err(|e| CliError::ConnectionFailed(format!("Invalid TLS server name: {}", e)))?
                .to_owned();
            let mut tls_stream = connector
                .connect(server_name, tcp_stream)
                .await
                .map_err(|e| CliError::ConnectionFailed(format!("TLS handshake failed: {}", e)))?;

            // TLS distribution uses packet-4 framing (4-byte length prefix)
            let peer_node = tls_dist_handshake(&mut tls_stream, &mut local_node, &cookie).await?;

            let negotiated_flags = local_node.flags & peer_node.flags;
            let (sender, receiver) = message::channel(
                DistStream::Tls(SharedStream::new(tls_stream)),
                negotiated_flags,
            );

            Ok(Self {
                sender,
                receiver,
                local_node_name: local_node.name.to_string(),
                creation: local_node.creation,
                ref_counter: 0,
            })
        } else {
            // TCP distribution uses packet-2 framing during handshake
            let (tcp_stream, local_node, peer_node) =
                tcp_dist_handshake(tcp_stream, local_node, &cookie).await?;

            let negotiated_flags = local_node.flags & peer_node.flags;
            let (sender, receiver) =
                message::channel(DistStream::Tcp(tcp_stream), negotiated_flags);

            Ok(Self {
                sender,
                receiver,
                local_node_name: local_node.name.to_string(),
                creation: local_node.creation,
                ref_counter: 0,
            })
        }
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

fn make_local_node() -> LocalNode {
    let creation = Creation::random();
    let tentative_name: NodeName = "nonode@localhost".parse().expect("valid node name");
    let mut local_node = LocalNode::new(tentative_name, creation);
    local_node.flags |= DistributionFlags::NAME_ME;
    local_node.flags |= DistributionFlags::SPAWN;
    local_node.flags |= DistributionFlags::DIST_MONITOR;
    local_node.flags |= DistributionFlags::DIST_MONITOR_NAME;
    local_node
}

/// Perform the Erlang distribution handshake over a plain TCP stream.
///
/// TCP distribution uses 2-byte (packet-2) framing during the handshake,
/// which matches what `erl_dist::ClientSideHandshake` expects.
async fn tcp_dist_handshake(
    stream: smol::net::TcpStream,
    mut local_node: LocalNode,
    cookie: &str,
) -> Result<(smol::net::TcpStream, LocalNode, erl_dist::node::PeerNode)> {
    let mut handshake = ClientSideHandshake::new(stream, local_node.clone(), cookie);
    let status = handshake
        .execute_send_name(6)
        .await
        .map_err(|e| CliError::ConnectionFailed(format!("Distribution handshake failed: {}", e)))?;

    match status {
        HandshakeStatus::Named {
            name,
            creation: peer_creation,
        } => {
            local_node.name = NodeName::new(&name, local_node.name.host())
                .map_err(|e| CliError::ConnectionFailed(format!("Invalid assigned name: {}", e)))?;
            local_node.creation = peer_creation;
        }
        _ => {
            return Err(CliError::ConnectionFailed(format!(
                "Distribution handshake rejected: {:?}",
                status
            )));
        }
    }

    let (stream, peer_node) = handshake
        .execute_rest(true)
        .await
        .map_err(|e| CliError::ConnectionFailed(format!("Distribution handshake failed: {}", e)))?;

    Ok((stream, local_node, peer_node))
}

/// Perform the Erlang distribution handshake over a TLS stream.
///
/// Erlang's `inet_tls_dist` uses 4-byte (packet-4) framing for the entire
/// connection, including the handshake. The `erl_dist` crate uses 2-byte
/// framing for handshake messages, so we implement the handshake directly
/// with the correct framing.
async fn tls_dist_handshake<T>(
    stream: &mut T,
    local_node: &mut LocalNode,
    cookie: &str,
) -> Result<erl_dist::node::PeerNode>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    // 1. Send NAME (version 6, tag 'N')
    let mut msg = Vec::new();
    msg.push(b'N');
    msg.extend_from_slice(&local_node.flags.bits().to_be_bytes());
    msg.extend_from_slice(&local_node.creation.get().to_be_bytes());
    let host = if local_node.flags.contains(DistributionFlags::NAME_ME) {
        local_node.name.host().to_string()
    } else {
        local_node.name.to_string()
    };
    msg.extend_from_slice(&(host.len() as u16).to_be_bytes());
    msg.extend_from_slice(host.as_bytes());
    write_packet4(stream, &msg).await?;

    // 2. Read STATUS
    let status_msg = read_packet4(stream).await?;
    if status_msg.is_empty() || status_msg[0] != b's' {
        return Err(CliError::ConnectionFailed(format!(
            "Expected STATUS tag 's', got {:?}",
            status_msg.first()
        )));
    }
    let status_body = &status_msg[1..];
    if status_body.starts_with(b"named:") {
        let rest = &status_body[6..];
        if rest.len() < 6 {
            return Err(CliError::ConnectionFailed(
                "STATUS named: response too short".to_string(),
            ));
        }
        let name_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
        if rest.len() < 2 + name_len + 4 {
            return Err(CliError::ConnectionFailed(
                "STATUS named: response truncated".to_string(),
            ));
        }
        let name = std::str::from_utf8(&rest[2..2 + name_len]).map_err(|_| {
            CliError::ConnectionFailed("Invalid UTF-8 in assigned node name".to_string())
        })?;
        let creation_bytes = &rest[2 + name_len..2 + name_len + 4];
        let peer_creation = Creation::new(u32::from_be_bytes(creation_bytes.try_into().unwrap()));
        let node_name: NodeName = name.parse().map_err(|e| {
            CliError::ConnectionFailed(format!("Invalid assigned name '{}': {}", name, e))
        })?;
        local_node.name = NodeName::new(node_name.name(), local_node.name.host())
            .map_err(|e| CliError::ConnectionFailed(format!("Invalid assigned name: {}", e)))?;
        local_node.creation = peer_creation;
        local_node.flags |= DistributionFlags::NAME_ME;
    } else if status_body == b"ok" || status_body == b"ok_simultaneous" {
        // Continue
    } else {
        return Err(CliError::ConnectionFailed(format!(
            "Distribution handshake rejected with status: {}",
            String::from_utf8_lossy(status_body)
        )));
    }

    // 3. Read CHALLENGE
    let challenge_msg = read_packet4(stream).await?;
    if challenge_msg.is_empty() {
        return Err(CliError::ConnectionFailed(
            "Empty CHALLENGE message".to_string(),
        ));
    }
    let (peer_node, peer_challenge) = match challenge_msg[0] {
        b'N' => {
            if challenge_msg.len() < 1 + 8 + 4 + 4 + 2 {
                return Err(CliError::ConnectionFailed(
                    "CHALLENGE message too short".to_string(),
                ));
            }
            let flags = DistributionFlags::from_bits_truncate(u64::from_be_bytes(
                challenge_msg[1..9].try_into().unwrap(),
            ));
            let challenge = u32::from_be_bytes(challenge_msg[9..13].try_into().unwrap());
            let creation = Creation::new(u32::from_be_bytes(
                challenge_msg[13..17].try_into().unwrap(),
            ));
            let name_len = u16::from_be_bytes(challenge_msg[17..19].try_into().unwrap()) as usize;
            let name_str =
                std::str::from_utf8(&challenge_msg[19..19 + name_len]).map_err(|_| {
                    CliError::ConnectionFailed("Invalid UTF-8 in peer name".to_string())
                })?;
            let name: NodeName = name_str
                .parse()
                .map_err(|e| CliError::ConnectionFailed(format!("Invalid peer name: {}", e)))?;
            let peer = erl_dist::node::PeerNode {
                name,
                flags,
                creation: Some(creation),
            };
            (peer, challenge)
        }
        tag => {
            return Err(CliError::ConnectionFailed(format!(
                "Expected CHALLENGE tag 'N', got {}",
                tag
            )));
        }
    };

    // 4. No COMPLEMENT needed — version 6 NAME already includes full
    //    64-bit flags and 32-bit creation.

    // 5. Send CHALLENGE_REPLY
    let digest = md5::compute(format!("{}{}", cookie, peer_challenge));

    let mut reply = Vec::with_capacity(21);
    reply.push(b'r');
    let local_challenge: u32 = rand::random();
    reply.extend_from_slice(&local_challenge.to_be_bytes());
    reply.extend_from_slice(&digest.0);
    write_packet4(stream, &reply).await?;

    // 6. Read CHALLENGE_ACK
    let ack_msg = read_packet4(stream).await?;
    if ack_msg.is_empty() || ack_msg[0] != b'a' {
        return Err(CliError::ConnectionFailed(format!(
            "Expected CHALLENGE_ACK tag 'a', got {:?}",
            ack_msg.first()
        )));
    }
    let expected_digest = md5::compute(format!("{}{}", cookie, local_challenge));
    if ack_msg[1..] != expected_digest.0 {
        return Err(CliError::ConnectionFailed(
            "Cookie mismatch in CHALLENGE_ACK".to_string(),
        ));
    }

    Ok(peer_node)
}

/// Write a message with 4-byte length prefix (Erlang packet-4 framing).
async fn write_packet4<T: AsyncWrite + Unpin>(stream: &mut T, data: &[u8]) -> Result<()> {
    let len = data.len() as u32;

    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| CliError::ConnectionFailed(format!("Failed to write packet length: {}", e)))?;
    stream
        .write_all(data)
        .await
        .map_err(|e| CliError::ConnectionFailed(format!("Failed to write packet data: {}", e)))?;
    stream
        .flush()
        .await
        .map_err(|e| CliError::ConnectionFailed(format!("Failed to flush: {}", e)))?;
    Ok(())
}

/// Read a message with 4-byte length prefix (Erlang packet-4 framing).
async fn read_packet4<T: AsyncRead + Unpin>(stream: &mut T) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| CliError::ConnectionFailed(format!("Failed to read packet length: {}", e)))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| CliError::ConnectionFailed(format!("Failed to read packet data: {}", e)))?;
    Ok(buf)
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

/// Read the distribution port from environment variable or runtime file
fn read_dist_port() -> Result<u16> {
    if let Ok(port_str) = std::env::var("NEONFS_DIST_PORT") {
        let port_str = port_str.trim();
        if !port_str.is_empty() {
            return port_str.parse::<u16>().map_err(|e| {
                CliError::ConnectionFailed(format!(
                    "Invalid NEONFS_DIST_PORT '{}': {}",
                    port_str, e
                ))
            });
        }
    }

    let path = Path::new(RUNTIME_DIST_PORT_PATH);
    if !path.exists() {
        return Err(CliError::ConnectionFailed(format!(
            "Distribution port file not found at {}. Is the daemon running?",
            RUNTIME_DIST_PORT_PATH
        )));
    }

    // audit:bounded dist port file is a single integer
    let content = fs::read_to_string(path).map_err(|e| {
        CliError::ConnectionFailed(format!("Failed to read distribution port file: {}", e))
    })?;

    content.trim().parse::<u16>().map_err(|e| {
        CliError::ConnectionFailed(format!(
            "Invalid distribution port in {}: {}",
            RUNTIME_DIST_PORT_PATH, e
        ))
    })
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

    // audit:bounded Erlang cookie file holds a single short atom
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
