mod channel;
mod operation;
mod server;

use operation::{FuseOperation, FuseReply};
use rustler::{LocalPid, ResourceArc, Term};
use server::FuseServer;
use std::sync::Mutex;

/// Wrapper for FuseServer to be used as a Rustler Resource
pub struct FuseServerResource {
    server: Mutex<FuseServer>,
}

impl FuseServerResource {
    fn new(server: FuseServer) -> Self {
        Self {
            server: Mutex::new(server),
        }
    }
}

#[rustler::resource_impl]
impl rustler::Resource for FuseServerResource {}

/// Start a FUSE server that communicates with the given callback PID
///
/// Returns a resource handle to the server. Operations will be sent as messages
/// to the callback_pid in the format: {:fuse_op, ref, operation}
#[rustler::nif]
fn start_fuse_server(
    callback_pid: LocalPid,
) -> (rustler::types::atom::Atom, ResourceArc<FuseServerResource>) {
    let (server, mut operation_rx) = FuseServer::new(callback_pid);
    let _reply_manager = server.reply_manager();

    // Spawn a background task to forward operations to Elixir
    let _pid = server.callback_pid();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            while let Some(request) = operation_rx.recv().await {
                // Send operation to Elixir process
                // Format: {:fuse_op, ref, operation_tuple}
                // Note: In real implementation, we'd use env.send() here
                // For now, we just track it in the reply manager
                log::debug!(
                    "Operation {} sent to Elixir (mock): {:?}",
                    request.id,
                    request.operation
                );
            }
        });
    });

    let resource = ResourceArc::new(FuseServerResource::new(server));

    (rustler::types::atom::ok(), resource)
}

/// Stop a running FUSE server
#[rustler::nif]
fn stop_fuse_server(
    server_resource: ResourceArc<FuseServerResource>,
) -> rustler::types::atom::Atom {
    let server = server_resource.server.lock().unwrap();
    server.shutdown();
    rustler::types::atom::ok()
}

/// Reply to a FUSE operation
///
/// Takes the server resource, request ID, and the reply to send back.
#[rustler::nif]
fn reply_fuse_operation(
    server_resource: ResourceArc<FuseServerResource>,
    request_id: u64,
    reply_term: Term,
) -> Result<rustler::types::atom::Atom, String> {
    let server = server_resource.server.lock().unwrap();
    let reply_manager = server.reply_manager();

    // Parse the reply from Elixir term
    let reply = parse_reply(reply_term)?;

    reply_manager.reply(request_id, reply)?;

    Ok(rustler::types::atom::ok())
}

/// Send a test operation to the server (for testing the channel)
///
/// This is a test NIF that submits a mock operation and waits for a reply.
/// Used to verify the communication infrastructure works.
#[rustler::nif]
fn test_operation(
    server_resource: ResourceArc<FuseServerResource>,
    operation_type: String,
) -> Result<String, String> {
    let server = server_resource.server.lock().unwrap();

    let operation = match operation_type.as_str() {
        "read" => FuseOperation::Read {
            ino: 1,
            offset: 0,
            size: 4096,
        },
        "lookup" => FuseOperation::Lookup {
            parent: 1,
            name: "test.txt".to_string(),
        },
        _ => return Err(format!("Unknown operation type: {}", operation_type)),
    };

    // In a real scenario, we'd await the reply, but for this test NIF
    // we just verify the operation can be submitted
    match server.submit_operation(operation) {
        Ok(_rx) => Ok("Operation submitted".to_string()),
        Err(e) => Err(e),
    }
}

/// Get server stats (for testing/debugging)
#[rustler::nif]
fn server_stats(
    server_resource: ResourceArc<FuseServerResource>,
) -> (rustler::types::atom::Atom, (usize, bool)) {
    let server = server_resource.server.lock().unwrap();
    let pending = server.pending_count();
    let shutdown = server.is_shutdown();
    (rustler::types::atom::ok(), (pending, shutdown))
}

/// Parse Elixir term into FuseReply
fn parse_reply(term: Term) -> Result<FuseReply, String> {
    // First try to decode as atom (for :ok)
    if let Ok(atom_str) = term.atom_to_string() {
        if atom_str == "ok" {
            return Ok(FuseReply::Ok);
        }
        return Err(format!("Unknown atom reply: {}", atom_str));
    }

    // Try to decode as 2-element tuple for {:error, errno}
    if let Ok((status_term, value_term)) = term.decode::<(Term, Term)>() {
        let status = status_term
            .atom_to_string()
            .map_err(|_| "First tuple element must be atom".to_string())?;

        match status.as_str() {
            "ok" => {
                // {:ok, value} - for now just return Ok
                Ok(FuseReply::Ok)
            }
            "error" => {
                let errno: i32 = value_term
                    .decode()
                    .map_err(|_| "errno must be integer".to_string())?;
                Ok(FuseReply::Error { errno })
            }
            _ => Err(format!("Unknown reply status: {}", status)),
        }
    } else {
        Err("Reply must be an atom or 2-element tuple".to_string())
    }
}

rustler::init!("Elixir.NeonFS.FUSE.Native");

#[cfg(test)]
mod tests {
    #[test]
    fn test_module_compiles() {
        // Test that module compiles - existence of this test is sufficient
    }
}
