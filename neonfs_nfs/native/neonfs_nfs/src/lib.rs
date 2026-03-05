mod channel;
mod filesystem;
mod handle;
mod test_client;

use channel::{DirEntryInfo, FileAttrs, FileKind, NfsReply, NfsReplyData, ReplyManager};
use filesystem::{errno_to_nfsstat, NeonFilesystem};
use nfs3_server::tcp::{NFSTcp, NFSTcpListener};
use rustler::{Encoder, LocalPid, ResourceArc, Term};
use std::sync::{Arc, Mutex};

/// Wrapper for the NFS server to be used as a Rustler Resource
pub struct NfsServerResource {
    reply_manager: ReplyManager,
    shutdown: Arc<Mutex<bool>>,
    listen_port: Arc<Mutex<Option<u16>>>,
}

#[rustler::resource_impl]
impl rustler::Resource for NfsServerResource {}

/// Start an NFS server that communicates with the given callback PID.
///
/// The server listens on the specified bind address (e.g. "0.0.0.0:2049").
/// Operations will be sent as messages to the callback_pid in the format:
/// `{:nfs_op, request_id, {operation_name, params}}`
#[rustler::nif(schedule = "DirtyCpu")]
fn start_nfs_server(
    bind_address: String,
    callback_pid: LocalPid,
) -> Result<ResourceArc<NfsServerResource>, String> {
    let reply_manager = ReplyManager::new();
    let fs = NeonFilesystem::new(callback_pid, reply_manager.clone());
    let shutdown = Arc::new(Mutex::new(false));
    let shutdown_clone = shutdown.clone();
    let listen_port: Arc<Mutex<Option<u16>>> = Arc::new(Mutex::new(None));
    let listen_port_clone = listen_port.clone();

    // Start the NFS TCP listener in a background thread with its own Tokio runtime
    std::thread::spawn(move || {
        // Initialize tracing subscriber so nfs3_server internal logs are visible
        let _ = tracing_subscriber::fmt()
            .with_env_filter("debug")
            .with_writer(std::io::stderr)
            .try_init();

        eprintln!(
            "[neonfs_nfs] NFS server thread starting, bind={}",
            bind_address
        );

        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("[neonfs_nfs] Failed to create Tokio runtime: {}", e);
                return;
            }
        };

        rt.block_on(async move {
            eprintln!("[neonfs_nfs] Tokio runtime started, binding...");
            match NFSTcpListener::bind(&bind_address, fs).await {
                Ok(listener) => {
                    let port = listener.get_listen_port();
                    eprintln!(
                        "[neonfs_nfs] NFS server listening on {}:{}",
                        listener.get_listen_ip(),
                        port
                    );
                    if let Ok(mut lp) = listen_port_clone.lock() {
                        *lp = Some(port);
                    }
                    match listener.handle_forever().await {
                        Ok(()) => {
                            eprintln!("[neonfs_nfs] handle_forever returned Ok");
                        }
                        Err(e) => {
                            let is_shutdown = shutdown_clone.lock().map(|s| *s).unwrap_or(false);
                            if !is_shutdown {
                                eprintln!("[neonfs_nfs] NFS server error: {}", e);
                            } else {
                                eprintln!("[neonfs_nfs] NFS server shutdown");
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[neonfs_nfs] Failed to bind NFS server: {}", e);
                }
            }
            eprintln!("[neonfs_nfs] NFS server thread exiting");
        });
    });

    let resource = ResourceArc::new(NfsServerResource {
        reply_manager,
        shutdown,
        listen_port,
    });

    Ok(resource)
}

/// Stop a running NFS server
#[rustler::nif]
fn stop_nfs_server(server_resource: ResourceArc<NfsServerResource>) -> Result<(), String> {
    if let Ok(mut s) = server_resource.shutdown.lock() {
        *s = true;
    }
    // The server will notice shutdown on next operation attempt
    Ok(())
}

/// Reply to an NFS operation
///
/// Takes the server resource, request ID, and the reply to send back.
/// The reply is an Elixir term that encodes the operation result.
#[rustler::nif]
fn reply_nfs_operation(
    server_resource: ResourceArc<NfsServerResource>,
    request_id: u64,
    reply_term: Term,
) -> Result<(), String> {
    let reply = parse_reply(reply_term)?;
    server_resource.reply_manager.reply(request_id, reply)?;
    Ok(())
}

/// Get server stats (for testing/debugging)
#[rustler::nif]
fn server_stats(server_resource: ResourceArc<NfsServerResource>) -> Result<(usize, bool), String> {
    let pending = server_resource.reply_manager.pending_count();
    let shutdown = server_resource
        .shutdown
        .lock()
        .map(|s| *s)
        .map_err(|e| format!("Lock poisoned: {}", e))?;
    Ok((pending, shutdown))
}

/// Get the TCP port the NFS server is listening on.
///
/// Polls until the port is available (up to 5 seconds). Returns the port number.
/// This is useful for tests that start the server on port 0 (OS-assigned).
#[rustler::nif(schedule = "DirtyCpu")]
fn get_server_port(server_resource: ResourceArc<NfsServerResource>) -> Result<u16, String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Ok(guard) = server_resource.listen_port.lock() {
            if let Some(port) = *guard {
                return Ok(port);
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err("Timed out waiting for server to bind".to_string());
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Parse Elixir term into NfsReply
///
/// Expected formats:
/// - `{:ok, %{type: "attrs", ...}}` — Attributes reply
/// - `{:ok, %{type: "lookup", ...}}` — Lookup reply
/// - `{:ok, %{type: "read", data: binary, eof: bool}}` — Read reply
/// - `{:ok, %{type: "dir_entries", entries: [...]}}` — Directory listing
/// - `{:ok, %{type: "create", ...}}` — Create reply
/// - `{:ok, %{type: "write", ...}}` — Write reply
/// - `{:ok, %{type: "readlink", target: string}}` — Readlink reply
/// - `{:ok, %{type: "empty"}}` — Success with no data
/// - `{:error, errno}` — Error with errno code
fn parse_reply(term: Term) -> Result<NfsReply, String> {
    // Try to decode as 2-element tuple
    let (status_term, value_term): (Term, Term) = term
        .decode()
        .map_err(|_| "Reply must be a 2-element tuple".to_string())?;

    let status = status_term
        .atom_to_string()
        .map_err(|_| "First tuple element must be atom".to_string())?;

    match status.as_str() {
        "ok" => parse_ok_reply(value_term),
        "error" => {
            let errno: i32 = value_term
                .decode()
                .map_err(|_| "errno must be integer".to_string())?;
            Ok(NfsReply::Error(errno_to_nfsstat(errno)))
        }
        _ => Err(format!("Unknown reply status: {}", status)),
    }
}

fn parse_ok_reply(term: Term) -> Result<NfsReply, String> {
    // Decode the reply type from the map
    let reply_type: String = get_map_string(term, "type")?;

    match reply_type.as_str() {
        "empty" => Ok(NfsReply::Ok(NfsReplyData::Empty)),
        "attrs" => {
            let attrs = parse_file_attrs(term)?;
            Ok(NfsReply::Ok(NfsReplyData::Attrs(attrs)))
        }
        "lookup" => {
            let file_id: u64 =
                get_map_u64(term, "file_id").map_err(|_| "lookup missing file_id".to_string())?;
            let attrs = parse_file_attrs(term)?;
            let volume_id = get_map_binary(term, "volume_id").ok();
            Ok(NfsReply::Ok(NfsReplyData::Lookup(
                file_id, attrs, volume_id,
            )))
        }
        "read" => {
            let data: Vec<u8> = get_map_binary(term, "data")?;
            let eof: bool =
                get_map_bool(term, "eof").map_err(|_| "read missing eof".to_string())?;
            Ok(NfsReply::Ok(NfsReplyData::Read(data, eof)))
        }
        "dir_entries" => {
            let entries = parse_dir_entries(term)?;
            Ok(NfsReply::Ok(NfsReplyData::DirEntries(entries)))
        }
        "create" => {
            let file_id: u64 =
                get_map_u64(term, "file_id").map_err(|_| "create missing file_id".to_string())?;
            let attrs = parse_file_attrs(term)?;
            Ok(NfsReply::Ok(NfsReplyData::Create(file_id, attrs)))
        }
        "write" => {
            let count: u32 =
                get_map_u32(term, "count").map_err(|_| "write missing count".to_string())?;
            let attrs = parse_file_attrs(term)?;
            Ok(NfsReply::Ok(NfsReplyData::Write(count, attrs)))
        }
        "readlink" => {
            let target: String = get_map_string(term, "target")?;
            Ok(NfsReply::Ok(NfsReplyData::Readlink(target)))
        }
        _ => Err(format!("Unknown reply type: {}", reply_type)),
    }
}

fn parse_file_attrs(map_term: Term) -> Result<FileAttrs, String> {
    Ok(FileAttrs {
        file_id: get_map_u64(map_term, "file_id").unwrap_or(0),
        size: get_map_u64(map_term, "size").unwrap_or(0),
        kind: match get_map_string(map_term, "kind")
            .unwrap_or_else(|_| "file".to_string())
            .as_str()
        {
            "directory" => FileKind::Directory,
            "symlink" => FileKind::Symlink,
            _ => FileKind::File,
        },
        mode: get_map_u32(map_term, "mode").unwrap_or(0o644),
        uid: get_map_u32(map_term, "uid").unwrap_or(0),
        gid: get_map_u32(map_term, "gid").unwrap_or(0),
        nlink: get_map_u32(map_term, "nlink").unwrap_or(1),
        atime_secs: get_map_i64(map_term, "atime_secs").unwrap_or(0),
        atime_nsecs: get_map_u32(map_term, "atime_nsecs").unwrap_or(0),
        mtime_secs: get_map_i64(map_term, "mtime_secs").unwrap_or(0),
        mtime_nsecs: get_map_u32(map_term, "mtime_nsecs").unwrap_or(0),
        ctime_secs: get_map_i64(map_term, "ctime_secs").unwrap_or(0),
        ctime_nsecs: get_map_u32(map_term, "ctime_nsecs").unwrap_or(0),
    })
}

fn parse_dir_entries(map_term: Term) -> Result<Vec<DirEntryInfo>, String> {
    let entries_term: Vec<Term> = get_map_list(map_term, "entries")?;
    let mut result = Vec::new();

    for entry_term in entries_term {
        let file_id = get_map_u64(entry_term, "file_id").unwrap_or(0);
        let name = get_map_string(entry_term, "name")?;
        let attrs = parse_file_attrs(entry_term)?;
        let volume_id = get_map_binary(entry_term, "volume_id").ok();
        result.push(DirEntryInfo {
            file_id,
            name,
            attrs,
            volume_id,
        });
    }

    Ok(result)
}

// Map field extraction helpers

fn get_map_string(map_term: Term, key: &str) -> Result<String, String> {
    let env = map_term.get_env();
    let key_term = key.encode(env);
    match rustler::Term::map_get(map_term, key_term) {
        Ok(val) => val
            .decode::<String>()
            .map_err(|_| format!("Field '{}' must be a string", key)),
        Err(_) => Err(format!("Missing field '{}'", key)),
    }
}

fn get_map_u64(map_term: Term, key: &str) -> Result<u64, String> {
    let env = map_term.get_env();
    let key_term = key.encode(env);
    match rustler::Term::map_get(map_term, key_term) {
        Ok(val) => val
            .decode::<u64>()
            .map_err(|_| format!("Field '{}' must be a u64", key)),
        Err(_) => Err(format!("Missing field '{}'", key)),
    }
}

fn get_map_u32(map_term: Term, key: &str) -> Result<u32, String> {
    let env = map_term.get_env();
    let key_term = key.encode(env);
    match rustler::Term::map_get(map_term, key_term) {
        Ok(val) => val
            .decode::<u32>()
            .map_err(|_| format!("Field '{}' must be a u32", key)),
        Err(_) => Err(format!("Missing field '{}'", key)),
    }
}

fn get_map_i64(map_term: Term, key: &str) -> Result<i64, String> {
    let env = map_term.get_env();
    let key_term = key.encode(env);
    match rustler::Term::map_get(map_term, key_term) {
        Ok(val) => val
            .decode::<i64>()
            .map_err(|_| format!("Field '{}' must be an i64", key)),
        Err(_) => Err(format!("Missing field '{}'", key)),
    }
}

fn get_map_bool(map_term: Term, key: &str) -> Result<bool, String> {
    let env = map_term.get_env();
    let key_term = key.encode(env);
    match rustler::Term::map_get(map_term, key_term) {
        Ok(val) => val
            .decode::<bool>()
            .map_err(|_| format!("Field '{}' must be a bool", key)),
        Err(_) => Err(format!("Missing field '{}'", key)),
    }
}

fn get_map_binary<'a>(map_term: Term<'a>, key: &str) -> Result<Vec<u8>, String> {
    let env = map_term.get_env();
    let key_term = key.encode(env);
    match rustler::Term::map_get(map_term, key_term) {
        Ok(val) => {
            // Try decoding as Elixir binary first, then fall back to list of u8
            if let Ok(bin) = val.decode::<rustler::Binary>() {
                Ok(bin.as_slice().to_vec())
            } else {
                val.decode::<Vec<u8>>()
                    .map_err(|_| format!("Field '{}' must be a binary", key))
            }
        }
        Err(_) => Err(format!("Missing field '{}'", key)),
    }
}

fn get_map_list<'a>(map_term: Term<'a>, key: &str) -> Result<Vec<Term<'a>>, String> {
    let env = map_term.get_env();
    let key_term = key.encode(env);
    match rustler::Term::map_get(map_term, key_term) {
        Ok(val) => val
            .decode::<Vec<Term>>()
            .map_err(|_| format!("Field '{}' must be a list", key)),
        Err(_) => Err(format!("Missing field '{}'", key)),
    }
}

rustler::init!("Elixir.NeonFS.NFS.Native");

#[cfg(test)]
mod tests {
    #[test]
    fn test_module_compiles() {
        // Test that module compiles - existence of this test is sufficient
    }
}
