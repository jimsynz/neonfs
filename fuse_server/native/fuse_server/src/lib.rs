//! FUSE transport NIF.
//!
//! This crate owns a raw file descriptor (either `/dev/fuse` in production or
//! one end of a `pipe(2)` pair in tests) and exposes four operations to
//! Elixir via `FuseServer.Native`:
//!
//! * `open_dev_fuse/0` — open `/dev/fuse`.
//! * `pipe_pair/0` — allocate a pipe pair (test aid so the CI image does not
//!   need FUSE support).
//! * `select_read/1` — arm a single read-readiness notification via
//!   `enif_select`; the owning process receives a `{select, Resource, Ref,
//!   ready_input}` message when the fd is readable.
//! * `read_frame/1` / `write_frame/2` — non-blocking `read(2)` / `write(2)`
//!   against the fd. Each frame is bounded at 128 KiB (the FUSE protocol's
//!   `max_write`), so a single syscall always returns a complete
//!   request/response — matching the protocol-bounded case described in
//!   `CLAUDE.md` (not a whole-file-buffering violation).
//!
//! Rustler 0.37 does not wrap `enif_select`, and the safe `Resource` trait
//! does not let us install a stop callback. Without a stop callback we cannot
//! close the fd safely after tearing down a select registration. We therefore
//! register the resource type by hand via `enif_open_resource_type_x` at NIF
//! load time, installing both a destructor and a stop callback. All the
//! unsafe glue is localised to this file.

use rustler::sys::{
    enif_alloc_resource, enif_get_resource, enif_make_resource, enif_open_resource_type_x,
    enif_release_resource, enif_select, enif_self, ErlNifEnv, ErlNifEvent, ErlNifPid,
    ErlNifResourceFlags, ErlNifResourceType, ErlNifResourceTypeInit, ERL_NIF_SELECT_FAILED,
    ERL_NIF_SELECT_NOTSUP, ERL_NIF_SELECT_READ, ERL_NIF_SELECT_STOP,
};
use rustler::types::atom::{self, Atom};
use rustler::{Binary, Encoder, Env, Error as NifError, NewBinary, NifResult, Term};
use std::ffi::c_void;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::atomic::{AtomicI32, AtomicPtr, Ordering};

mod atoms {
    rustler::atoms! {
        eagain,
        eintr,
        einval,
        enodev,
        enoent,
        enosys,
        eperm,
        epipe,
        select_already_closed,
        select_failed,
        select_not_supported,
    }
}

/// Opaque resource holding an owned fd.
///
/// The fd is stored in an `AtomicI32` so the destructor and the stop callback
/// (which can run on different threads) can observe/transition it without a
/// data race. A value of `-1` means "closed".
#[repr(C)]
pub struct FuseFd {
    fd: AtomicI32,
}

/// Registered resource type, populated once in [`on_load`].
static RESOURCE_TYPE: AtomicPtr<ErlNifResourceType> = AtomicPtr::new(ptr::null_mut());

fn on_load(env: Env, _info: Term) -> bool {
    let init = ErlNifResourceTypeInit {
        dtor: fuse_fd_dtor as *const _,
        stop: fuse_fd_stop as *const _,
        down: ptr::null(),
        members: 2,
        dyncall: ptr::null(),
    };

    let name = c"FuseFd";
    let rt = unsafe {
        enif_open_resource_type_x(
            env.as_c_arg(),
            name.as_ptr() as *const c_char,
            &init,
            ErlNifResourceFlags::ERL_NIF_RT_CREATE,
            ptr::null_mut(),
        )
    };

    if rt.is_null() {
        return false;
    }

    RESOURCE_TYPE.store(rt as *mut _, Ordering::SeqCst);
    true
}

fn resource_type() -> *const ErlNifResourceType {
    RESOURCE_TYPE.load(Ordering::SeqCst) as *const _
}

/// Destructor invoked by the runtime when the last Erlang reference is
/// dropped. Schedules a stop via `enif_select`; the stop callback then closes
/// the fd.
unsafe extern "C" fn fuse_fd_dtor(env: *mut ErlNifEnv, obj: *mut c_void) {
    let resource = &*(obj as *const FuseFd);
    let fd = resource.fd.load(Ordering::SeqCst);
    if fd < 0 {
        return;
    }

    // `enif_select` with SELECT_STOP queues the stop callback. The stop
    // callback is responsible for the actual `close(2)`. Atoms are not
    // associated with an env, so we can pass `:undefined` by raw term here.
    let undefined = atom::undefined().as_c_arg();
    let _ = enif_select(
        env,
        fd as ErlNifEvent,
        ERL_NIF_SELECT_STOP,
        obj,
        ptr::null(),
        undefined,
    );
}

/// Stop callback invoked after the select registration is torn down. Safe to
/// close the fd now — no scheduler thread is still polling it.
unsafe extern "C" fn fuse_fd_stop(
    _env: *mut ErlNifEnv,
    obj: *mut c_void,
    _event: ErlNifEvent,
    _is_direct_call: c_int,
) {
    let resource = &*(obj as *const FuseFd);
    let fd = resource.fd.swap(-1, Ordering::SeqCst);
    if fd >= 0 {
        libc::close(fd);
    }
}

/// Return an `{:error, atom}` tuple from the NIF.
fn err_term(a: Atom) -> NifError {
    NifError::Term(Box::new(a))
}

/// Decode a resource term into a raw resource pointer and a reference to the
/// underlying `FuseFd`. We need both: the pointer is the `obj` argument
/// expected by `enif_select`; the reference lets us read the stored fd.
fn get_resource_raw<'a>(env: Env<'a>, term: Term<'a>) -> NifResult<(*const c_void, &'a FuseFd)> {
    let rt = resource_type();
    if rt.is_null() {
        return Err(err_term(atoms::enosys()));
    }

    let mut obj: *const c_void = ptr::null();
    let ok = unsafe { enif_get_resource(env.as_c_arg(), term.as_c_arg(), rt, &mut obj) };
    if ok == 0 || obj.is_null() {
        return Err(err_term(atoms::einval()));
    }

    let resource = unsafe { &*(obj as *const FuseFd) };
    Ok((obj, resource))
}

/// Allocate a new resource wrapping `fd`, hand ownership to the runtime, and
/// return a resource term usable from Elixir. On failure, closes `fd`.
fn make_resource(env: Env, fd: c_int) -> NifResult<Term> {
    let rt = resource_type();
    if rt.is_null() {
        unsafe { libc::close(fd) };
        return Err(err_term(atoms::enosys()));
    }

    let obj = unsafe { enif_alloc_resource(rt, std::mem::size_of::<FuseFd>()) };
    if obj.is_null() {
        unsafe { libc::close(fd) };
        return Err(err_term(atoms::enosys()));
    }

    // Initialise the struct in-place. `enif_alloc_resource` returns
    // uninitialised memory; `ptr::write` avoids dropping whatever happens to
    // be there.
    unsafe {
        ptr::write(
            obj as *mut FuseFd,
            FuseFd {
                fd: AtomicI32::new(fd),
            },
        );
    }

    let term = unsafe { enif_make_resource(env.as_c_arg(), obj) };
    // The term now owns a reference; drop our local refcount.
    unsafe { enif_release_resource(obj) };

    Ok(unsafe { Term::new(env, term) })
}

fn errno_to_atom(errno: c_int) -> Atom {
    match errno {
        libc::EAGAIN => atoms::eagain(),
        libc::EINTR => atoms::eintr(),
        libc::EINVAL => atoms::einval(),
        libc::ENODEV => atoms::enodev(),
        libc::ENOENT => atoms::enoent(),
        libc::EPERM | libc::EACCES => atoms::eperm(),
        libc::EPIPE => atoms::epipe(),
        _ => atoms::enosys(),
    }
}

fn last_errno() -> c_int {
    unsafe { *libc::__errno_location() }
}

#[rustler::nif]
pub fn open_dev_fuse(env: Env) -> NifResult<Term> {
    let path = c"/dev/fuse";
    let flags = libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC;
    let fd = unsafe { libc::open(path.as_ptr(), flags) };
    if fd < 0 {
        return Err(err_term(errno_to_atom(last_errno())));
    }

    let resource = make_resource(env, fd)?;
    Ok((atom::ok(), resource).encode(env))
}

#[rustler::nif]
pub fn pipe_pair(env: Env) -> NifResult<Term> {
    let mut fds: [c_int; 2] = [-1, -1];
    let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) };
    if rc != 0 {
        return Err(err_term(errno_to_atom(last_errno())));
    }

    let read_term = match make_resource(env, fds[0]) {
        Ok(t) => t,
        Err(e) => {
            unsafe { libc::close(fds[1]) };
            return Err(e);
        }
    };
    let write_term = make_resource(env, fds[1])?;
    Ok((atom::ok(), (read_term, write_term)).encode(env))
}

#[rustler::nif]
pub fn select_read<'a>(env: Env<'a>, resource: Term<'a>) -> NifResult<Atom> {
    let (obj, dev) = get_resource_raw(env, resource)?;
    let fd = dev.fd.load(Ordering::SeqCst);
    if fd < 0 {
        return Err(err_term(atoms::select_already_closed()));
    }

    let mut pid: ErlNifPid = unsafe { std::mem::zeroed() };
    let pid_ok = unsafe { enif_self(env.as_c_arg(), &mut pid) };
    if pid_ok.is_null() {
        return Err(err_term(atoms::einval()));
    }

    // Pass `:undefined` for the select ref so the default `{select, Resource,
    // Ref, ready_input}` tuple is delivered with Ref = undefined.
    let undefined = atom::undefined().as_c_arg();
    let rc = unsafe {
        enif_select(
            env.as_c_arg(),
            fd as ErlNifEvent,
            ERL_NIF_SELECT_READ,
            obj,
            &pid,
            undefined,
        )
    };

    if rc < 0 {
        return Err(err_term(atoms::select_failed()));
    }
    if rc & ERL_NIF_SELECT_FAILED != 0 {
        return Err(err_term(atoms::select_failed()));
    }
    if rc & ERL_NIF_SELECT_NOTSUP != 0 {
        return Err(err_term(atoms::select_not_supported()));
    }

    Ok(atom::ok())
}

#[rustler::nif]
pub fn read_frame<'a>(env: Env<'a>, resource: Term<'a>) -> NifResult<Term<'a>> {
    let (_obj, dev) = get_resource_raw(env, resource)?;
    let fd = dev.fd.load(Ordering::SeqCst);
    if fd < 0 {
        return Err(err_term(atoms::select_already_closed()));
    }

    // 128 KiB matches the FUSE kernel `max_write` — a single read(2) always
    // returns a whole frame.
    const FRAME_CAP: usize = 128 * 1024;
    let mut buf = vec![0u8; FRAME_CAP];

    let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut c_void, FRAME_CAP) };
    if n < 0 {
        return Err(err_term(errno_to_atom(last_errno())));
    }

    let n = n as usize;
    let mut bin = NewBinary::new(env, n);
    bin.as_mut_slice().copy_from_slice(&buf[..n]);
    let binary: Binary = bin.into();
    Ok((atom::ok(), binary.to_term(env)).encode(env))
}

#[rustler::nif]
pub fn write_frame<'a>(env: Env<'a>, resource: Term<'a>, frame: Binary<'a>) -> NifResult<Atom> {
    let (_obj, dev) = get_resource_raw(env, resource)?;
    let fd = dev.fd.load(Ordering::SeqCst);
    if fd < 0 {
        return Err(err_term(atoms::select_already_closed()));
    }

    let data = frame.as_slice();
    let n = unsafe { libc::write(fd, data.as_ptr() as *const c_void, data.len()) };
    if n < 0 {
        return Err(err_term(errno_to_atom(last_errno())));
    }
    if (n as usize) != data.len() {
        // Short writes shouldn't happen for bounded frames on `/dev/fuse` or
        // pipes with buffers ≥ 128 KiB. Surface it as EAGAIN so the caller
        // can re-arm a write-ready notification later.
        return Err(err_term(atoms::eagain()));
    }
    Ok(atom::ok())
}

rustler::init!("Elixir.FuseServer.Native", load = on_load);
