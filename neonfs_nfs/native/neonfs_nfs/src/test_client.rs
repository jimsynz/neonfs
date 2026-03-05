/// NFSv3 test client NIFs for end-to-end protocol testing.
///
/// Wraps `nfs3_client` in blocking dirty-scheduler NIFs so ExUnit tests can
/// exercise the full NFS protocol path: Rust client → TCP → nfs3_server →
/// filesystem.rs → channel → Handler → Elixir.
use nfs3_client::nfs3_types::nfs3::{
    cookieverf3, createhow3, createverf3, diropargs3, fattr3, filename3, ftype3, nfs_fh3, nfspath3,
    sattr3, stable_how, symlinkdata3, CREATE3args, GETATTR3args, LOOKUP3args, MKDIR3args,
    Nfs3Option, Nfs3Result, READ3args, READDIRPLUS3args, READLINK3args, REMOVE3args, RENAME3args,
    SETATTR3args, SYMLINK3args, WRITE3args,
};
use nfs3_client::nfs3_types::xdr_codec::Opaque;
use nfs3_client::tokio::{TokioConnector, TokioIo};
use nfs3_client::{Nfs3Connection, Nfs3ConnectionBuilder};
use rustler::{Binary, Encoder, Env, OwnedBinary, ResourceArc, Term};
use std::sync::Mutex;
use tokio::net::TcpStream;

type Connection = Nfs3Connection<TokioIo<TcpStream>>;

/// Rustler resource wrapping a tokio runtime and an NFS3 connection.
pub struct NfsTestClientResource {
    rt: tokio::runtime::Runtime,
    conn: Mutex<Option<Connection>>,
}

#[rustler::resource_impl]
impl rustler::Resource for NfsTestClientResource {}

/// Connect to an NFS server and mount the given export.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_connect(
    host: String,
    port: u16,
    export: String,
) -> Result<ResourceArc<NfsTestClientResource>, String> {
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("Runtime: {e}"))?;

    let conn = rt.block_on(async {
        Nfs3ConnectionBuilder::new(TokioConnector, &host, &export)
            .connect_from_privileged_port(false)
            .nfs3_port(port)
            .mount_port(port)
            .mount()
            .await
    });

    let conn = conn.map_err(|e| format!("Connect: {e}"))?;

    Ok(ResourceArc::new(NfsTestClientResource {
        rt,
        conn: Mutex::new(Some(conn)),
    }))
}

/// Disconnect from the NFS server.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_disconnect(resource: ResourceArc<NfsTestClientResource>) -> Result<(), String> {
    let conn = resource
        .conn
        .lock()
        .map_err(|e| format!("Lock: {e}"))?
        .take();

    if let Some(conn) = conn {
        resource
            .rt
            .block_on(async { conn.unmount().await })
            .map_err(|e| format!("Unmount: {e}"))?;
    }

    Ok(())
}

/// Get the root file handle from the mount.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_root_handle(
    env: Env,
    resource: ResourceArc<NfsTestClientResource>,
) -> Result<Term, String> {
    let guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_ref().ok_or("Not connected")?;
    let fh = conn.root_nfs_fh3();
    let bytes = fh.data.to_vec();
    Ok(make_binary(env, &bytes))
}

/// NFS NULL (ping) procedure.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_null(resource: ResourceArc<NfsTestClientResource>) -> Result<(), String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    resource
        .rt
        .block_on(async { conn.null().await })
        .map_err(|e| format!("Null: {e}"))
}

/// GETATTR — returns a map of file attributes.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_getattr<'a>(
    env: Env<'a>,
    resource: ResourceArc<NfsTestClientResource>,
    fh: Binary<'a>,
) -> Result<Term<'a>, String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = GETATTR3args {
        object: make_fh(fh.as_slice()),
    };

    let res = resource
        .rt
        .block_on(async { conn.getattr(&args).await })
        .map_err(|e| format!("Getattr: {e}"))?;

    match res {
        Nfs3Result::Ok(ok) => Ok(encode_fattr3(env, &ok.obj_attributes)),
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// LOOKUP — resolve a name in a directory.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_lookup<'a>(
    env: Env<'a>,
    resource: ResourceArc<NfsTestClientResource>,
    dir_fh: Binary<'a>,
    name: String,
) -> Result<Term<'a>, String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = LOOKUP3args {
        what: diropargs3 {
            dir: make_fh(dir_fh.as_slice()),
            name: filename3(Opaque::owned(name.into_bytes())),
        },
    };

    let res = resource
        .rt
        .block_on(async { conn.lookup(&args).await })
        .map_err(|e| format!("Lookup: {e}"))?;

    match res {
        Nfs3Result::Ok(ok) => {
            let handle_binary = make_binary(env, ok.object.data.as_ref());
            let attrs = match ok.obj_attributes {
                nfs3_client::nfs3_types::nfs3::Nfs3Option::Some(a) => encode_fattr3(env, &a),
                _ => rustler::types::atom::nil().encode(env),
            };
            Ok(make_map(
                env,
                &[("handle", handle_binary), ("attrs", attrs)],
            ))
        }
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// READDIRPLUS — list directory entries with attributes.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_readdirplus<'a>(
    env: Env<'a>,
    resource: ResourceArc<NfsTestClientResource>,
    dir_fh: Binary<'a>,
) -> Result<Term<'a>, String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = READDIRPLUS3args {
        dir: make_fh(dir_fh.as_slice()),
        cookie: 0,
        cookieverf: cookieverf3::default(),
        dircount: 8192,
        maxcount: 32768,
    };

    let res = resource
        .rt
        .block_on(async { conn.readdirplus(&args).await })
        .map_err(|e| format!("Readdirplus: {e}"))?;

    match res {
        Nfs3Result::Ok(ok) => {
            let mut entries_terms: Vec<Term> = Vec::new();

            for entry in &ok.reply.entries.0 {
                let name_str =
                    std::str::from_utf8(entry.name.0.as_ref()).unwrap_or("<invalid utf8>");
                let name_term = name_str.encode(env);

                let handle_term = match &entry.name_handle {
                    nfs3_client::nfs3_types::nfs3::Nfs3Option::Some(ref fh) => {
                        make_binary(env, fh.data.as_ref())
                    }
                    _ => rustler::types::atom::nil().encode(env),
                };

                let attrs_term = match &entry.name_attributes {
                    nfs3_client::nfs3_types::nfs3::Nfs3Option::Some(ref a) => encode_fattr3(env, a),
                    _ => rustler::types::atom::nil().encode(env),
                };

                let entry_map = make_map(
                    env,
                    &[
                        ("name", name_term),
                        ("handle", handle_term),
                        ("attrs", attrs_term),
                        ("fileid", entry.fileid.encode(env)),
                    ],
                );
                entries_terms.push(entry_map);
            }

            Ok(entries_terms.encode(env))
        }
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// CREATE — create a new file.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_create<'a>(
    env: Env<'a>,
    resource: ResourceArc<NfsTestClientResource>,
    dir_fh: Binary<'a>,
    name: String,
) -> Result<Term<'a>, String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = CREATE3args {
        where_: diropargs3 {
            dir: make_fh(dir_fh.as_slice()),
            name: filename3(Opaque::owned(name.into_bytes())),
        },
        how: createhow3::UNCHECKED(sattr3::default()),
    };

    let res = resource
        .rt
        .block_on(async { conn.create(&args).await })
        .map_err(|e| format!("Create: {e}"))?;

    match res {
        Nfs3Result::Ok(ok) => {
            let handle_term = match ok.obj {
                nfs3_client::nfs3_types::nfs3::Nfs3Option::Some(fh) => {
                    make_binary(env, fh.data.as_ref())
                }
                _ => rustler::types::atom::nil().encode(env),
            };
            let attrs_term = match ok.obj_attributes {
                nfs3_client::nfs3_types::nfs3::Nfs3Option::Some(a) => encode_fattr3(env, &a),
                _ => rustler::types::atom::nil().encode(env),
            };
            Ok(make_map(
                env,
                &[("handle", handle_term), ("attrs", attrs_term)],
            ))
        }
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// MKDIR — create a directory.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_mkdir<'a>(
    env: Env<'a>,
    resource: ResourceArc<NfsTestClientResource>,
    dir_fh: Binary<'a>,
    name: String,
) -> Result<Term<'a>, String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = MKDIR3args {
        where_: diropargs3 {
            dir: make_fh(dir_fh.as_slice()),
            name: filename3(Opaque::owned(name.into_bytes())),
        },
        attributes: sattr3::default(),
    };

    let res = resource
        .rt
        .block_on(async { conn.mkdir(&args).await })
        .map_err(|e| format!("Mkdir: {e}"))?;

    match res {
        Nfs3Result::Ok(ok) => {
            let handle_term = match ok.obj {
                nfs3_client::nfs3_types::nfs3::Nfs3Option::Some(fh) => {
                    make_binary(env, fh.data.as_ref())
                }
                _ => rustler::types::atom::nil().encode(env),
            };
            let attrs_term = match ok.obj_attributes {
                nfs3_client::nfs3_types::nfs3::Nfs3Option::Some(a) => encode_fattr3(env, &a),
                _ => rustler::types::atom::nil().encode(env),
            };
            Ok(make_map(
                env,
                &[("handle", handle_term), ("attrs", attrs_term)],
            ))
        }
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// WRITE — write data to a file.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_write<'a>(
    env: Env<'a>,
    resource: ResourceArc<NfsTestClientResource>,
    fh: Binary<'a>,
    offset: u64,
    data: Binary<'a>,
) -> Result<Term<'a>, String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = WRITE3args {
        file: make_fh(fh.as_slice()),
        offset,
        count: data.len() as u32,
        stable: stable_how::FILE_SYNC,
        data: Opaque::borrowed(data.as_slice()),
    };

    let res = resource
        .rt
        .block_on(async { conn.write(&args).await })
        .map_err(|e| format!("Write: {e}"))?;

    match res {
        Nfs3Result::Ok(ok) => Ok(ok.count.encode(env)),
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// READ — read data from a file.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_read<'a>(
    env: Env<'a>,
    resource: ResourceArc<NfsTestClientResource>,
    fh: Binary<'a>,
    offset: u64,
    count: u32,
) -> Result<Term<'a>, String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = READ3args {
        file: make_fh(fh.as_slice()),
        offset,
        count,
    };

    let res = resource
        .rt
        .block_on(async { conn.read(&args).await })
        .map_err(|e| format!("Read: {e}"))?;

    match res {
        Nfs3Result::Ok(ok) => {
            let data_term = make_binary(env, ok.data.as_ref());
            let eof_term = ok.eof.encode(env);
            Ok(make_map(env, &[("data", data_term), ("eof", eof_term)]))
        }
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// REMOVE — delete a file.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_remove<'a>(
    resource: ResourceArc<NfsTestClientResource>,
    dir_fh: Binary<'a>,
    name: String,
) -> Result<(), String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = REMOVE3args {
        object: diropargs3 {
            dir: make_fh(dir_fh.as_slice()),
            name: filename3(Opaque::owned(name.into_bytes())),
        },
    };

    let res = resource
        .rt
        .block_on(async { conn.remove(&args).await })
        .map_err(|e| format!("Remove: {e}"))?;

    match res {
        Nfs3Result::Ok(_) => Ok(()),
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// SETATTR — modify file attributes.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_setattr<'a>(
    env: Env<'a>,
    resource: ResourceArc<NfsTestClientResource>,
    fh: Binary<'a>,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
) -> Result<Term<'a>, String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;

    let to_opt_u32 = |v: Option<u32>| match v {
        Some(val) => Nfs3Option::Some(val),
        None => Nfs3Option::None,
    };
    let to_opt_u64 = |v: Option<u64>| match v {
        Some(val) => Nfs3Option::Some(val),
        None => Nfs3Option::None,
    };

    let new_attributes = sattr3 {
        mode: to_opt_u32(mode),
        uid: to_opt_u32(uid),
        gid: to_opt_u32(gid),
        size: to_opt_u64(size),
        ..sattr3::default()
    };

    let args = SETATTR3args {
        object: make_fh(fh.as_slice()),
        new_attributes,
        guard: Nfs3Option::None,
    };

    let res = resource
        .rt
        .block_on(async { conn.setattr(&args).await })
        .map_err(|e| format!("Setattr: {e}"))?;

    match res {
        Nfs3Result::Ok(ok) => match ok.obj_wcc.after {
            Nfs3Option::Some(attrs) => Ok(encode_fattr3(env, &attrs)),
            _ => Err("Setattr: no post-op attrs".to_string()),
        },
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// RENAME — move a file or directory.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_rename<'a>(
    resource: ResourceArc<NfsTestClientResource>,
    from_dir_fh: Binary<'a>,
    from_name: String,
    to_dir_fh: Binary<'a>,
    to_name: String,
) -> Result<(), String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = RENAME3args {
        from: diropargs3 {
            dir: make_fh(from_dir_fh.as_slice()),
            name: filename3(Opaque::owned(from_name.into_bytes())),
        },
        to: diropargs3 {
            dir: make_fh(to_dir_fh.as_slice()),
            name: filename3(Opaque::owned(to_name.into_bytes())),
        },
    };

    let res = resource
        .rt
        .block_on(async { conn.rename(&args).await })
        .map_err(|e| format!("Rename: {e}"))?;

    match res {
        Nfs3Result::Ok(_) => Ok(()),
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// SYMLINK — create a symbolic link.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_symlink<'a>(
    env: Env<'a>,
    resource: ResourceArc<NfsTestClientResource>,
    dir_fh: Binary<'a>,
    name: String,
    target: String,
) -> Result<Term<'a>, String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = SYMLINK3args {
        where_: diropargs3 {
            dir: make_fh(dir_fh.as_slice()),
            name: filename3(Opaque::owned(name.into_bytes())),
        },
        symlink: symlinkdata3 {
            symlink_attributes: sattr3::default(),
            symlink_data: nfspath3(Opaque::owned(target.into_bytes())),
        },
    };

    let res = resource
        .rt
        .block_on(async { conn.symlink(&args).await })
        .map_err(|e| format!("Symlink: {e}"))?;

    match res {
        Nfs3Result::Ok(ok) => {
            let handle_term = match ok.obj {
                Nfs3Option::Some(fh) => make_binary(env, fh.data.as_ref()),
                _ => rustler::types::atom::nil().encode(env),
            };
            let attrs_term = match ok.obj_attributes {
                Nfs3Option::Some(a) => encode_fattr3(env, &a),
                _ => rustler::types::atom::nil().encode(env),
            };
            Ok(make_map(
                env,
                &[("handle", handle_term), ("attrs", attrs_term)],
            ))
        }
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// READLINK — read symlink target.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_readlink<'a>(
    env: Env<'a>,
    resource: ResourceArc<NfsTestClientResource>,
    fh: Binary<'a>,
) -> Result<Term<'a>, String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = READLINK3args {
        symlink: make_fh(fh.as_slice()),
    };

    let res = resource
        .rt
        .block_on(async { conn.readlink(&args).await })
        .map_err(|e| format!("Readlink: {e}"))?;

    match res {
        Nfs3Result::Ok(ok) => {
            let target_str = std::str::from_utf8(ok.data.0.as_ref()).unwrap_or("<invalid utf8>");
            Ok(target_str.encode(env))
        }
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

/// CREATE_EXCLUSIVE — atomic create-if-absent.
#[rustler::nif(schedule = "DirtyCpu")]
fn test_client_create_exclusive<'a>(
    env: Env<'a>,
    resource: ResourceArc<NfsTestClientResource>,
    dir_fh: Binary<'a>,
    name: String,
) -> Result<Term<'a>, String> {
    let mut guard = resource.conn.lock().map_err(|e| format!("Lock: {e}"))?;
    let conn = guard.as_mut().ok_or("Not connected")?;
    let args = CREATE3args {
        where_: diropargs3 {
            dir: make_fh(dir_fh.as_slice()),
            name: filename3(Opaque::owned(name.into_bytes())),
        },
        how: createhow3::EXCLUSIVE(createverf3([0u8; 8])),
    };

    let res = resource
        .rt
        .block_on(async { conn.create(&args).await })
        .map_err(|e| format!("CreateExclusive: {e}"))?;

    match res {
        Nfs3Result::Ok(ok) => {
            let handle_term = match ok.obj {
                Nfs3Option::Some(fh) => make_binary(env, fh.data.as_ref()),
                _ => rustler::types::atom::nil().encode(env),
            };
            let attrs_term = match ok.obj_attributes {
                Nfs3Option::Some(a) => encode_fattr3(env, &a),
                _ => rustler::types::atom::nil().encode(env),
            };
            Ok(make_map(
                env,
                &[("handle", handle_term), ("attrs", attrs_term)],
            ))
        }
        Nfs3Result::Err((status, _)) => Err(format!("NFS error: {status}")),
    }
}

// -- Helpers --

fn make_fh(bytes: &[u8]) -> nfs_fh3 {
    nfs_fh3 {
        data: Opaque::owned(bytes.to_vec()),
    }
}

fn make_binary<'a>(env: Env<'a>, data: &[u8]) -> Term<'a> {
    let mut bin = OwnedBinary::new(data.len()).unwrap();
    bin.as_mut_slice().copy_from_slice(data);
    let binary: Binary = bin.release(env);
    binary.encode(env)
}

fn make_map<'a>(env: Env<'a>, pairs: &[(&str, Term<'a>)]) -> Term<'a> {
    let kv: Vec<(Term, Term)> = pairs.iter().map(|(k, v)| (k.encode(env), *v)).collect();
    Term::map_from_pairs(env, &kv).unwrap()
}

fn encode_fattr3<'a>(env: Env<'a>, attrs: &fattr3) -> Term<'a> {
    let file_type = match attrs.type_ {
        ftype3::NF3DIR => "directory",
        ftype3::NF3LNK => "symlink",
        _ => "file",
    };

    make_map(
        env,
        &[
            ("fileid", attrs.fileid.encode(env)),
            ("file_type", file_type.encode(env)),
            ("mode", attrs.mode.encode(env)),
            ("size", attrs.size.encode(env)),
            ("nlink", attrs.nlink.encode(env)),
            ("uid", attrs.uid.encode(env)),
            ("gid", attrs.gid.encode(env)),
        ],
    )
}
