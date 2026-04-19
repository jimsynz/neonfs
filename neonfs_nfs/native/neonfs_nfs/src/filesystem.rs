/// NeonFS NFS filesystem implementation
///
/// Implements the nfs3_server traits by forwarding all operations to an Elixir
/// GenServer via message passing. Each trait method:
/// 1. Registers a oneshot reply channel in the ReplyManager
/// 2. Sends `{:nfs_op, request_id, {op_name, params}}` to the Elixir Handler PID
/// 3. Awaits the oneshot with a timeout (returns NFS3ERR_JUKEBOX on timeout)
/// 4. Elixir replies via `Native.reply_nfs_operation/3` which resolves the oneshot
use crate::channel::{FileAttrs, FileKind, NfsReply, NfsReplyData, ReplyManager};
use crate::handle::NeonFileHandle;
use nfs3_server::nfs3_types::nfs3::{
    createverf3, fattr3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, set_atime,
    set_mtime, specdata3, Nfs3Option,
};
use nfs3_server::vfs::{
    DirEntryPlus, NextResult, NfsFileSystem, NfsReadFileSystem, ReadDirPlusIterator,
};
use rustler::{Encoder, LocalPid, NewBinary, OwnedEnv};
use tokio::time::{timeout, Duration};

/// Timeout for Elixir handler responses (10 seconds).
/// Returns NFS3ERR_JUKEBOX on timeout, telling the client to retry.
const HANDLER_TIMEOUT: Duration = Duration::from_secs(10);

/// The NeonFS filesystem accessible via NFSv3.
pub struct NeonFilesystem {
    callback_pid: LocalPid,
    reply_manager: ReplyManager,
}

impl NeonFilesystem {
    pub fn new(callback_pid: LocalPid, reply_manager: ReplyManager) -> Self {
        Self {
            callback_pid,
            reply_manager,
        }
    }

    /// Send an operation to the Elixir handler and await the reply.
    async fn call_elixir(&self, op_name: &str, params: Vec<(&str, ElixirValue)>) -> NfsReply {
        let (request_id, rx) = self.reply_manager.register();
        let pid = self.callback_pid;

        let op_name_owned = op_name.to_string();
        let params_owned: Vec<(String, ElixirValue)> = params
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        let _ = OwnedEnv::new().send_and_clear(&pid, move |env| {
            let nfs_op_atom = rustler::types::atom::Atom::from_str(env, "nfs_op").unwrap();

            // Build params map
            let mut map_pairs: Vec<(rustler::Term, rustler::Term)> = Vec::new();
            for (key, value) in &params_owned {
                let k = key.as_str().encode(env);
                let v = value.encode_term(env);
                map_pairs.push((k, v));
            }
            let params_map = rustler::Term::map_from_pairs(env, &map_pairs).unwrap();

            // Encode operation as {op_name, params_map}
            let operation = (op_name_owned.as_str().encode(env), params_map).encode(env);

            // Full message: {:nfs_op, request_id, {op_name, params}}
            (nfs_op_atom, request_id, operation).encode(env)
        });

        match timeout(HANDLER_TIMEOUT, rx).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => {
                eprintln!(
                    "[neonfs_nfs] Reply channel closed for request {}",
                    request_id
                );
                NfsReply::Error(nfsstat3::NFS3ERR_IO)
            }
            Err(_) => {
                eprintln!(
                    "[neonfs_nfs] Handler timeout for request {} (op: {})",
                    request_id, op_name
                );
                NfsReply::Error(nfsstat3::NFS3ERR_JUKEBOX)
            }
        }
    }
}

/// Elixir-encodable value for NIF message passing
#[derive(Debug, Clone)]
pub enum ElixirValue {
    U64(u64),
    U32(u32),
    String(String),
    Binary(Vec<u8>),
    OptionalU32(Option<u32>),
    OptionalU64(Option<u64>),
    OptionalTimestamp(Option<(i64, u32)>),
}

impl ElixirValue {
    fn encode_term<'a>(&self, env: rustler::Env<'a>) -> rustler::Term<'a> {
        let nil_atom = || {
            rustler::types::atom::Atom::from_str(env, "nil")
                .unwrap()
                .encode(env)
        };
        match self {
            ElixirValue::U64(v) => v.encode(env),
            ElixirValue::U32(v) => v.encode(env),
            ElixirValue::String(v) => v.as_str().encode(env),
            ElixirValue::Binary(v) => {
                let mut binary = NewBinary::new(env, v.len());
                binary.as_mut_slice().copy_from_slice(v);
                let bin: rustler::Binary = binary.into();
                bin.encode(env)
            }
            ElixirValue::OptionalU32(Some(v)) => v.encode(env),
            ElixirValue::OptionalU32(None) => nil_atom(),
            ElixirValue::OptionalU64(Some(v)) => v.encode(env),
            ElixirValue::OptionalU64(None) => nil_atom(),
            ElixirValue::OptionalTimestamp(Some((sec, nsec))) => (*sec, *nsec).encode(env),
            ElixirValue::OptionalTimestamp(None) => nil_atom(),
        }
    }
}

/// Derive a per-volume fsid from the 16-byte volume ID.
///
/// Each volume gets a distinct fsid so NFS clients treat files in different
/// volumes as belonging to different filesystems. Combined with deterministic
/// hash-based inodes, this makes file handles portable across NFS cluster nodes.
fn volume_id_to_fsid(volume_id: [u8; 16]) -> u64 {
    u64::from_le_bytes(volume_id[0..8].try_into().unwrap())
}

/// Convert FileAttrs to nfs3 fattr3, using the volume ID to derive fsid.
fn attrs_to_fattr3(attrs: &FileAttrs, volume_id: [u8; 16]) -> fattr3 {
    fattr3 {
        type_: match attrs.kind {
            FileKind::File => ftype3::NF3REG,
            FileKind::Directory => ftype3::NF3DIR,
            FileKind::Symlink => ftype3::NF3LNK,
        },
        mode: attrs.mode,
        nlink: attrs.nlink,
        uid: attrs.uid,
        gid: attrs.gid,
        size: attrs.size,
        used: attrs.size,
        rdev: specdata3 {
            specdata1: 0,
            specdata2: 0,
        },
        fsid: volume_id_to_fsid(volume_id),
        fileid: attrs.file_id,
        atime: nfstime3 {
            seconds: attrs.atime_secs as u32,
            nseconds: attrs.atime_nsecs,
        },
        mtime: nfstime3 {
            seconds: attrs.mtime_secs as u32,
            nseconds: attrs.mtime_nsecs,
        },
        ctime: nfstime3 {
            seconds: attrs.ctime_secs as u32,
            nseconds: attrs.ctime_nsecs,
        },
    }
}

/// Map Elixir errno integer to nfsstat3
pub fn errno_to_nfsstat(errno: i32) -> nfsstat3 {
    match errno {
        1 => nfsstat3::NFS3ERR_PERM,
        2 => nfsstat3::NFS3ERR_NOENT,
        5 => nfsstat3::NFS3ERR_IO,
        6 => nfsstat3::NFS3ERR_NXIO,
        13 => nfsstat3::NFS3ERR_ACCES,
        17 => nfsstat3::NFS3ERR_EXIST,
        18 => nfsstat3::NFS3ERR_XDEV,
        20 => nfsstat3::NFS3ERR_NOTDIR,
        21 => nfsstat3::NFS3ERR_ISDIR,
        22 => nfsstat3::NFS3ERR_INVAL,
        27 => nfsstat3::NFS3ERR_FBIG,
        28 => nfsstat3::NFS3ERR_NOSPC,
        30 => nfsstat3::NFS3ERR_ROFS,
        38 => nfsstat3::NFS3ERR_NOTSUPP,
        39 | 66 => nfsstat3::NFS3ERR_NOTEMPTY,
        63 => nfsstat3::NFS3ERR_NAMETOOLONG,
        70 => nfsstat3::NFS3ERR_STALE,
        10008 => nfsstat3::NFS3ERR_JUKEBOX,
        _ => nfsstat3::NFS3ERR_IO,
    }
}

// -- NfsReadFileSystem implementation --

impl NfsReadFileSystem for NeonFilesystem {
    type Handle = NeonFileHandle;

    fn root_dir(&self) -> Self::Handle {
        NeonFileHandle::root()
    }

    async fn lookup(
        &self,
        dirid: &Self::Handle,
        filename: &filename3<'_>,
    ) -> Result<Self::Handle, nfsstat3> {
        let name = std::str::from_utf8(filename.0.as_ref())
            .map_err(|_| nfsstat3::NFS3ERR_INVAL)?
            .to_string();

        let reply = self
            .call_elixir(
                "lookup",
                vec![
                    ("parent_inode", ElixirValue::U64(dirid.inode())),
                    (
                        "parent_volume_id",
                        ElixirValue::Binary(dirid.volume_id().to_vec()),
                    ),
                    ("name", ElixirValue::String(name)),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Lookup(file_id, _attrs, volume_id_override)) => {
                let volume_id = match &volume_id_override {
                    Some(vid) if vid.len() == 16 => {
                        let mut arr = [0u8; 16];
                        arr.copy_from_slice(vid);
                        arr
                    }
                    _ => dirid.volume_id(),
                };
                Ok(NeonFileHandle::new(file_id, volume_id))
            }
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }

    async fn getattr(&self, id: &Self::Handle) -> Result<fattr3, nfsstat3> {
        let reply = self
            .call_elixir(
                "getattr",
                vec![
                    ("inode", ElixirValue::U64(id.inode())),
                    ("volume_id", ElixirValue::Binary(id.volume_id().to_vec())),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Attrs(attrs)) => Ok(attrs_to_fattr3(&attrs, id.volume_id())),
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }

    async fn read(
        &self,
        id: &Self::Handle,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let reply = self
            .call_elixir(
                "read",
                vec![
                    ("inode", ElixirValue::U64(id.inode())),
                    ("volume_id", ElixirValue::Binary(id.volume_id().to_vec())),
                    ("offset", ElixirValue::U64(offset)),
                    ("count", ElixirValue::U32(count)),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Read(data, eof)) => Ok((data, eof)),
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }

    async fn readlink(&self, id: &Self::Handle) -> Result<nfspath3<'_>, nfsstat3> {
        let reply = self
            .call_elixir(
                "readlink",
                vec![
                    ("inode", ElixirValue::U64(id.inode())),
                    ("volume_id", ElixirValue::Binary(id.volume_id().to_vec())),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Readlink(target)) => {
                Ok(nfspath3(target.into_bytes().into()))
            }
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }

    async fn readdirplus(
        &self,
        dirid: &Self::Handle,
        cookie: u64,
    ) -> Result<impl ReadDirPlusIterator<Self::Handle>, nfsstat3> {
        let reply = self
            .call_elixir(
                "readdirplus",
                vec![
                    ("inode", ElixirValue::U64(dirid.inode())),
                    ("volume_id", ElixirValue::Binary(dirid.volume_id().to_vec())),
                    ("cookie", ElixirValue::U64(cookie)),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::DirEntries(entries)) => {
                let parent_volume_id = dirid.volume_id();

                let nfs_entries: Vec<DirEntryPlus<NeonFileHandle>> = entries
                    .into_iter()
                    .enumerate()
                    .map(|(i, entry)| {
                        let volume_id = match &entry.volume_id {
                            Some(vid) if vid.len() == 16 => {
                                let mut arr = [0u8; 16];
                                arr.copy_from_slice(vid);
                                arr
                            }
                            _ => parent_volume_id,
                        };
                        let handle = NeonFileHandle::new(entry.file_id, volume_id);
                        DirEntryPlus {
                            fileid: entry.file_id,
                            name: entry.name.into_bytes().into(),
                            cookie: cookie + i as u64 + 1,
                            name_attributes: Some(attrs_to_fattr3(&entry.attrs, volume_id)),
                            name_handle: Some(handle),
                        }
                    })
                    .collect();

                Ok(NeonDirIterator {
                    entries: nfs_entries,
                    pos: 0,
                })
            }
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }
}

// -- NfsFileSystem (write) implementation --

impl NfsFileSystem for NeonFilesystem {
    async fn setattr(&self, id: &Self::Handle, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        let reply = self
            .call_elixir(
                "setattr",
                vec![
                    ("inode", ElixirValue::U64(id.inode())),
                    ("volume_id", ElixirValue::Binary(id.volume_id().to_vec())),
                    ("mode", ElixirValue::OptionalU32(extract_set_mode(&setattr))),
                    ("uid", ElixirValue::OptionalU32(extract_set_uid(&setattr))),
                    ("gid", ElixirValue::OptionalU32(extract_set_gid(&setattr))),
                    ("size", ElixirValue::OptionalU64(extract_set_size(&setattr))),
                    (
                        "atime",
                        ElixirValue::OptionalTimestamp(extract_set_atime(&setattr)),
                    ),
                    (
                        "mtime",
                        ElixirValue::OptionalTimestamp(extract_set_mtime(&setattr)),
                    ),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Attrs(attrs)) => Ok(attrs_to_fattr3(&attrs, id.volume_id())),
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }

    async fn write(&self, id: &Self::Handle, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        let reply = self
            .call_elixir(
                "write",
                vec![
                    ("inode", ElixirValue::U64(id.inode())),
                    ("volume_id", ElixirValue::Binary(id.volume_id().to_vec())),
                    ("offset", ElixirValue::U64(offset)),
                    ("data", ElixirValue::Binary(data.to_vec())),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Write(_count, attrs)) => {
                Ok(attrs_to_fattr3(&attrs, id.volume_id()))
            }
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }

    async fn create(
        &self,
        dirid: &Self::Handle,
        filename: &filename3<'_>,
        setattr: sattr3,
    ) -> Result<(Self::Handle, fattr3), nfsstat3> {
        let name = std::str::from_utf8(filename.0.as_ref())
            .map_err(|_| nfsstat3::NFS3ERR_INVAL)?
            .to_string();

        let reply = self
            .call_elixir(
                "create",
                vec![
                    ("parent_inode", ElixirValue::U64(dirid.inode())),
                    (
                        "parent_volume_id",
                        ElixirValue::Binary(dirid.volume_id().to_vec()),
                    ),
                    ("name", ElixirValue::String(name)),
                    ("mode", ElixirValue::OptionalU32(extract_set_mode(&setattr))),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Create(file_id, attrs)) => {
                let handle = NeonFileHandle::new(file_id, dirid.volume_id());
                Ok((handle, attrs_to_fattr3(&attrs, dirid.volume_id())))
            }
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }

    async fn create_exclusive(
        &self,
        dirid: &Self::Handle,
        filename: &filename3<'_>,
        _createverf: createverf3,
    ) -> Result<Self::Handle, nfsstat3> {
        let name = std::str::from_utf8(filename.0.as_ref())
            .map_err(|_| nfsstat3::NFS3ERR_INVAL)?
            .to_string();

        let reply = self
            .call_elixir(
                "create_exclusive",
                vec![
                    ("parent_inode", ElixirValue::U64(dirid.inode())),
                    (
                        "parent_volume_id",
                        ElixirValue::Binary(dirid.volume_id().to_vec()),
                    ),
                    ("name", ElixirValue::String(name)),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Create(file_id, _attrs)) => {
                Ok(NeonFileHandle::new(file_id, dirid.volume_id()))
            }
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }

    async fn mkdir(
        &self,
        dirid: &Self::Handle,
        dirname: &filename3<'_>,
    ) -> Result<(Self::Handle, fattr3), nfsstat3> {
        let name = std::str::from_utf8(dirname.0.as_ref())
            .map_err(|_| nfsstat3::NFS3ERR_INVAL)?
            .to_string();

        let reply = self
            .call_elixir(
                "mkdir",
                vec![
                    ("parent_inode", ElixirValue::U64(dirid.inode())),
                    (
                        "parent_volume_id",
                        ElixirValue::Binary(dirid.volume_id().to_vec()),
                    ),
                    ("name", ElixirValue::String(name)),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Create(file_id, attrs)) => {
                let handle = NeonFileHandle::new(file_id, dirid.volume_id());
                Ok((handle, attrs_to_fattr3(&attrs, dirid.volume_id())))
            }
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }

    async fn remove(&self, dirid: &Self::Handle, filename: &filename3<'_>) -> Result<(), nfsstat3> {
        let name = std::str::from_utf8(filename.0.as_ref())
            .map_err(|_| nfsstat3::NFS3ERR_INVAL)?
            .to_string();

        let reply = self
            .call_elixir(
                "remove",
                vec![
                    ("parent_inode", ElixirValue::U64(dirid.inode())),
                    (
                        "parent_volume_id",
                        ElixirValue::Binary(dirid.volume_id().to_vec()),
                    ),
                    ("name", ElixirValue::String(name)),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Empty) => Ok(()),
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }

    async fn rename<'a>(
        &self,
        from_dirid: &Self::Handle,
        from_filename: &filename3<'a>,
        to_dirid: &Self::Handle,
        to_filename: &filename3<'a>,
    ) -> Result<(), nfsstat3> {
        let from_name = std::str::from_utf8(from_filename.0.as_ref())
            .map_err(|_| nfsstat3::NFS3ERR_INVAL)?
            .to_string();
        let to_name = std::str::from_utf8(to_filename.0.as_ref())
            .map_err(|_| nfsstat3::NFS3ERR_INVAL)?
            .to_string();

        let reply = self
            .call_elixir(
                "rename",
                vec![
                    ("from_parent_inode", ElixirValue::U64(from_dirid.inode())),
                    (
                        "from_parent_volume_id",
                        ElixirValue::Binary(from_dirid.volume_id().to_vec()),
                    ),
                    ("from_name", ElixirValue::String(from_name)),
                    ("to_parent_inode", ElixirValue::U64(to_dirid.inode())),
                    (
                        "to_parent_volume_id",
                        ElixirValue::Binary(to_dirid.volume_id().to_vec()),
                    ),
                    ("to_name", ElixirValue::String(to_name)),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Empty) => Ok(()),
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }

    async fn symlink<'a>(
        &self,
        dirid: &Self::Handle,
        linkname: &filename3<'a>,
        target: &nfspath3<'a>,
        _attr: &sattr3,
    ) -> Result<(Self::Handle, fattr3), nfsstat3> {
        let name = std::str::from_utf8(linkname.0.as_ref())
            .map_err(|_| nfsstat3::NFS3ERR_INVAL)?
            .to_string();
        let target_path = std::str::from_utf8(target.0.as_ref())
            .map_err(|_| nfsstat3::NFS3ERR_INVAL)?
            .to_string();

        let reply = self
            .call_elixir(
                "symlink",
                vec![
                    ("parent_inode", ElixirValue::U64(dirid.inode())),
                    (
                        "parent_volume_id",
                        ElixirValue::Binary(dirid.volume_id().to_vec()),
                    ),
                    ("name", ElixirValue::String(name)),
                    ("target", ElixirValue::String(target_path)),
                ],
            )
            .await;

        match reply {
            NfsReply::Ok(NfsReplyData::Create(file_id, attrs)) => {
                let handle = NeonFileHandle::new(file_id, dirid.volume_id());
                Ok((handle, attrs_to_fattr3(&attrs, dirid.volume_id())))
            }
            NfsReply::Error(stat) => Err(stat),
            _ => Err(nfsstat3::NFS3ERR_IO),
        }
    }
}

/// Iterator for readdirplus results
pub struct NeonDirIterator {
    entries: Vec<DirEntryPlus<NeonFileHandle>>,
    pos: usize,
}

impl ReadDirPlusIterator<NeonFileHandle> for NeonDirIterator {
    async fn next(&mut self) -> NextResult<DirEntryPlus<NeonFileHandle>> {
        if self.pos < self.entries.len() {
            let entry = self.entries[self.pos].clone();
            self.pos += 1;
            NextResult::Ok(entry)
        } else {
            NextResult::Eof
        }
    }
}

// -- sattr3 field extraction helpers --

fn extract_set_mode(attr: &sattr3) -> Option<u32> {
    match attr.mode {
        Nfs3Option::Some(m) => Some(m),
        Nfs3Option::None => None,
    }
}

fn extract_set_uid(attr: &sattr3) -> Option<u32> {
    match attr.uid {
        Nfs3Option::Some(u) => Some(u),
        Nfs3Option::None => None,
    }
}

fn extract_set_gid(attr: &sattr3) -> Option<u32> {
    match attr.gid {
        Nfs3Option::Some(g) => Some(g),
        Nfs3Option::None => None,
    }
}

fn extract_set_size(attr: &sattr3) -> Option<u64> {
    match attr.size {
        Nfs3Option::Some(s) => Some(s),
        Nfs3Option::None => None,
    }
}

fn extract_set_atime(attr: &sattr3) -> Option<(i64, u32)> {
    match &attr.atime {
        set_atime::SET_TO_CLIENT_TIME(t) => Some((t.seconds as i64, t.nseconds)),
        _ => None,
    }
}

fn extract_set_mtime(attr: &sattr3) -> Option<(i64, u32)> {
    match &attr.mtime {
        set_mtime::SET_TO_CLIENT_TIME(t) => Some((t.seconds as i64, t.nseconds)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attrs_to_fattr3_file() {
        let volume_id = [1u8; 16];
        let attrs = FileAttrs {
            file_id: 42,
            size: 1024,
            kind: FileKind::File,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            nlink: 1,
            atime_secs: 100,
            atime_nsecs: 0,
            mtime_secs: 200,
            mtime_nsecs: 0,
            ctime_secs: 300,
            ctime_nsecs: 0,
        };

        let fattr = attrs_to_fattr3(&attrs, volume_id);
        assert_eq!(fattr.type_, ftype3::NF3REG);
        assert_eq!(fattr.size, 1024);
        assert_eq!(fattr.fileid, 42);
        assert_eq!(fattr.fsid, volume_id_to_fsid(volume_id));
    }

    #[test]
    fn test_attrs_to_fattr3_dir() {
        let volume_id = [2u8; 16];
        let attrs = FileAttrs {
            kind: FileKind::Directory,
            mode: 0o755,
            ..Default::default()
        };

        let fattr = attrs_to_fattr3(&attrs, volume_id);
        assert_eq!(fattr.type_, ftype3::NF3DIR);
    }

    #[test]
    fn test_different_volumes_get_different_fsid() {
        let vol_a = [1u8; 16];
        let vol_b = [2u8; 16];
        assert_ne!(volume_id_to_fsid(vol_a), volume_id_to_fsid(vol_b));
    }

    #[test]
    fn test_null_volume_fsid_is_zero() {
        let null_vol = [0u8; 16];
        assert_eq!(volume_id_to_fsid(null_vol), 0);
    }

    #[test]
    fn test_errno_to_nfsstat() {
        assert_eq!(errno_to_nfsstat(2), nfsstat3::NFS3ERR_NOENT);
        assert_eq!(errno_to_nfsstat(5), nfsstat3::NFS3ERR_IO);
        assert_eq!(errno_to_nfsstat(13), nfsstat3::NFS3ERR_ACCES);
        assert_eq!(errno_to_nfsstat(999), nfsstat3::NFS3ERR_IO);
    }
}
