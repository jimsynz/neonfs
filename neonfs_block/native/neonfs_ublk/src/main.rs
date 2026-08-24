//! A ublk frontend for one NeonFS block device.
//!
//! One helper process per attached device, so its lifetime is the
//! attachment's and a fault in the io_uring binding takes one guest's
//! device rather than the node's whole data path. The BEAM keeps the IO
//! core and every per-IO decision; this owns io_uring framing and nothing
//! else.
//!
//! Multiqueue is one Unix socket per queue, so concurrency is real and a
//! slow IO blocks only its own queue.
//!
//! ## Where the device is not
//!
//! Nothing here reads or writes storage. Each IO is forwarded to the node
//! over `socket_prefix.<queue>` and completed with whatever the node
//! answers, which is what keeps one implementation of the device rather
//! than two.

mod protocol;

use std::io::{self, Write};
use std::os::unix::net::UnixStream;
use std::rc::Rc;
use std::sync::Arc;

use libublk::io::{BufDescList, UblkDev, UblkIOCtx, UblkQueue};
use libublk::{
    ctrl::{UblkCtrl, UblkCtrlBuilder},
    BufDesc, UblkError, UblkFlags, UblkIORes,
};

use protocol::{
    read_frame, write_frame, Reply, Request, OP_DISCARD, OP_FLUSH, OP_READ, OP_WRITE,
    OP_WRITE_ZEROES,
};

struct Config {
    socket_prefix: String,
    size_bytes: u64,
    logical_block_bytes: u32,
    device_id: i32,
    recover: bool,
    queues: u16,
    queue_depth: u16,
}

fn main() {
    match run(config_from_env()) {
        Ok(()) => {}
        Err(error) => {
            eprintln!("neonfs_ublk: {error}");
            std::process::exit(1);
        }
    }
}

// Configured by environment rather than by flags: the node spawns this and
// the values come from the device it resolved, so there is no operator in
// the middle to write a command line for.
fn config_from_env() -> Config {
    Config {
        socket_prefix: required("NEONFS_UBLK_SOCKET"),
        size_bytes: required("NEONFS_UBLK_SIZE_BYTES")
            .parse()
            .expect("size is a number"),
        logical_block_bytes: required("NEONFS_UBLK_BLOCK_BYTES")
            .parse()
            .expect("block size is a number"),
        // The node picks the id so it knows `/dev/ublkbN` without being told,
        // the way a CSI attach picks its own `/dev/nbdX`. `-1` asks the driver
        // to allocate, which leaves the path knowable only from here.
        device_id: optional("NEONFS_UBLK_ID", -1),
        // Set by the node when it is restarting a helper against a device
        // the kernel is holding quiesced, rather than creating a new one.
        recover: std::env::var("NEONFS_UBLK_RECOVER").is_ok(),
        queues: optional("NEONFS_UBLK_QUEUES", 1),
        queue_depth: optional("NEONFS_UBLK_QUEUE_DEPTH", 64),
    }
}

fn required(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{key} is required"))
}

fn optional<T: std::str::FromStr>(key: &str, fallback: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

fn run(config: Config) -> Result<(), UblkError> {
    let logical_block_bytes = config.logical_block_bytes;
    let size_bytes = config.size_bytes;
    let socket_prefix = Arc::new(config.socket_prefix);

    // Recovery is a different conversation with the driver, not a different
    // target: the device already exists and is quiesced, so this asks the
    // driver to reopen it rather than to make one. `START_USER_RECOVERY`
    // first, then a control handle flagged `RECOVER_DEV` — after which
    // `start_dev` sees the quiesced state and ends the recovery instead of
    // starting a device.
    if config.recover {
        if config.device_id < 0 {
            eprintln!("neonfs_ublk: recovery needs NEONFS_UBLK_ID");
            return Err(UblkError::OtherError(-libc::EINVAL));
        }

        UblkCtrl::new_simple(config.device_id)?.start_user_recover()?;
    }

    let control = UblkCtrlBuilder::default()
        .name("neonfs")
        .id(config.device_id)
        .nr_queues(config.queues)
        .depth(config.queue_depth)
        .io_buf_bytes(1 << 20)
        // `USER_RECOVERY` makes the driver quiesce this device when this
        // process dies, instead of failing every IO and taking it away.
        //
        // `REISSUE` decides what happens to the requests the dead process
        // had fetched but not completed: without it the driver fails them,
        // with it the new process is given them again. Failing them is what
        // recovery exists to avoid — a guest filesystem that gets EIO on a
        // journal write typically remounts read-only, which is the outcome
        // whether or not the device came back.
        //
        // The double-write REISSUE risks is safe here, and not by luck.
        // A re-issued request is by definition one the driver never
        // completed, so the guest was never told it finished and has issued
        // nothing that depends on it. Applying it twice converges: chunk
        // writes are content-addressed, so the same bytes produce the same
        // chunk, and a sub-extent write commits under a compare-and-swap
        // (`BlockIndex.commit/3`'s `:expect`) that re-reads and re-applies
        // if the first attempt did land. There is no ordering to violate and
        // no state to double-count.
        //
        // `UBLK_F_QUIESCE` is deliberately absent. It buys a *planned*
        // quiesce for an upgrade, which nothing asks for yet, and these
        // flags are fixed when the device is created — so adding it later
        // means recreating devices. That is the right trade in a project
        // that keeps no on-disk compatibility anyway.
        .ctrl_flags(
            (libublk::sys::UBLK_F_USER_RECOVERY | libublk::sys::UBLK_F_USER_RECOVERY_REISSUE)
                as u64,
        )
        .dev_flags(if config.recover {
            UblkFlags::UBLK_DEV_F_RECOVER_DEV
        } else {
            UblkFlags::UBLK_DEV_F_ADD_DEV
        })
        .build()?;

    let describe = move |device: &mut UblkDev| {
        device.set_default_params(size_bytes);
        device.tgt.params.basic.logical_bs_shift = shift_of(logical_block_bytes);
        device.tgt.params.basic.physical_bs_shift = shift_of(logical_block_bytes);
        Ok(())
    };

    let serve_queue = move |queue_id: u16, device: &UblkDev| {
        let prefix = Arc::clone(&socket_prefix);

        match serve(queue_id, device, &prefix) {
            Ok(()) => {}
            // A queue whose socket has gone cannot answer. Reporting it
            // lets the node act: since `USER_RECOVERY`, that means restarting
            // this process against the quiesced device rather than taking
            // the device away.
            Err(error) => eprintln!("neonfs_ublk: queue {queue_id} stopped: {error}"),
        }
    };

    // `run_target` calls this after `start_dev`, which is the only moment
    // `/dev/ublkbN` is known to exist — the queue sockets connect before it,
    // so a node waiting on those would hand out a path to nothing. One line,
    // one token, no value to misparse: the node already knows the id it asked
    // for.
    let announce = |control: &libublk::ctrl::UblkCtrl| {
        println!("neonfs_ublk: ready {}", control.dev_info().dev_id);
        let _ = io::stdout().flush();
    };

    control.run_target(describe, serve_queue, announce)?;
    Ok(())
}

fn shift_of(bytes: u32) -> u8 {
    bytes.trailing_zeros() as u8
}

fn serve(queue_id: u16, device: &UblkDev, socket_prefix: &str) -> Result<(), UblkError> {
    let path = format!("{socket_prefix}.{queue_id}");
    let mut socket = UnixStream::connect(&path)
        .map_err(|error| UblkError::OtherError(-(error.raw_os_error().unwrap_or(5))))?;

    let buffers = Rc::new(device.alloc_queue_io_bufs());
    let handler_buffers = buffers.clone();
    let mut outbound = Vec::new();
    let mut inbound = Vec::new();

    let handle = move |queue: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
        let descriptor = queue.get_iod(tag);
        let length = descriptor.nr_sectors << 9;
        let offset = descriptor.start_sector << 9;
        let buffer = &handler_buffers[tag as usize];
        let capacity = buffer.as_slice().len();

        // SAFETY: this queue owns `tag`'s buffer for the whole of that IO.
        // The kernel copied a write's payload into it before handing us the
        // command and does not touch it again until the completion below, so
        // nothing else reads or writes it here. `capacity` is the buffer's
        // own length, so the slice cannot run past it. `IoBuf::as_mut_slice`
        // would say this without `unsafe`, but it needs `&mut self` and the
        // queue's buffers are shared — which is the pattern the crate's own
        // synchronous example uses `as_mut_ptr` for.
        let slice = unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr(), capacity) };

        let outcome = match op_of(descriptor.op_flags & 0xff) {
            // An op the node has no answer for is refused as unsupported
            // rather than silently succeeding, which would tell the guest a
            // discard happened when nothing did.
            None => Err(UblkError::OtherError(-95)),

            Some(op) => {
                let payload: &[u8] = if op == OP_WRITE {
                    &slice[..length as usize]
                } else {
                    &[]
                };

                Request {
                    op,
                    tag,
                    offset,
                    length,
                    data: payload,
                }
                .encode(&mut outbound);

                let answered = write_frame(&mut socket, &outbound)
                    .and_then(|()| read_frame(&mut socket, &mut inbound))
                    .and_then(|()| Reply::decode(&inbound));

                settle(op, tag, length, slice, answered)
            }
        };

        queue
            .complete_io_cmd_unified(tag, BufDesc::Slice(buffer.as_slice()), outcome)
            .unwrap_or_else(|error| eprintln!("neonfs_ublk: completing tag {tag}: {error}"));
    };

    UblkQueue::new(queue_id, device)?
        .submit_fetch_commands_unified(BufDescList::Slices(Some(&buffers)))?
        .wait_and_handle_io(handle);

    Ok(())
}

// Turns the node's answer into the kernel's, copying a read's bytes into
// the queue's buffer on the way.
fn settle(
    op: u8,
    tag: u16,
    length: u32,
    slice: &mut [u8],
    answered: io::Result<Reply>,
) -> Result<UblkIORes, UblkError> {
    match answered {
        // The node never answered, so there is nothing to report but EIO —
        // and a transport failure is this device's failure.
        Err(error) => {
            eprintln!("neonfs_ublk: {error}");
            Err(UblkError::OtherError(-5))
        }

        // One socket per queue and one reply per request, so a tag that does
        // not match cannot be reordering — it is a logic error somewhere. The
        // consequence of trusting it would be completing this IO with another
        // one's bytes, so it fails instead of guessing.
        Ok(reply) if reply.tag != tag => {
            eprintln!("neonfs_ublk: tag {tag} answered with tag {}", reply.tag);
            Err(UblkError::OtherError(-5))
        }

        Ok(reply) if reply.status != 0 => Err(UblkError::OtherError(-(reply.status as i32))),

        Ok(reply) => {
            if op == OP_READ {
                let target = &mut slice[..length as usize];
                let served = reply.data.len().min(target.len());
                target[..served].copy_from_slice(&reply.data[..served]);
                // A short read is zero-filled rather than left holding
                // whatever was in the buffer, which would be another IO's
                // bytes.
                target[served..].fill(0);
            }

            Ok(UblkIORes::Result(length as i32))
        }
    }
}

fn op_of(code: u32) -> Option<u8> {
    match code {
        libublk::sys::UBLK_IO_OP_READ => Some(OP_READ),
        libublk::sys::UBLK_IO_OP_WRITE => Some(OP_WRITE),
        libublk::sys::UBLK_IO_OP_FLUSH => Some(OP_FLUSH),
        libublk::sys::UBLK_IO_OP_DISCARD => Some(OP_DISCARD),
        libublk::sys::UBLK_IO_OP_WRITE_ZEROES => Some(OP_WRITE_ZEROES),
        _ => None,
    }
}
