# FuseServer

A standalone Elixir library for building FUSE userspace filesystems on
the BEAM — without libfuse bindings or a native event loop. The only
native code is a minimal syscall NIF; everything above the file
descriptor (frame parsing, protocol encoding, your filesystem logic)
is ordinary supervised Elixir.

Two layers:

- **Transport** (`FuseServer.Native`, `FuseServer.Fusermount`) — opens
  `/dev/fuse`, mounts via the `fusermount3` userspace helper,
  `enif_select`-based readiness notifications, and a bounded
  read/write API for protocol frames. No `CAP_SYS_ADMIN` needed.
- **Codec** (`FuseServer.Protocol`) — a pure-Elixir codec for the
  Linux FUSE kernel protocol (FUSE_KERNEL_VERSION 7.31, as exposed by
  libfuse 3.10+ / Linux 5.4+). Operates on binaries only — no I/O, so
  it is testable without a kernel in sight.

The library is NeonFS-agnostic. The NeonFS filesystem built on it
lives in [`neonfs_fuse`](../neonfs_fuse/), whose
`NeonFS.FUSE.Session` GenServer is the reference for wiring transport
and codec together.

## Mount and serve

```elixir
{:ok, handle} =
  FuseServer.Fusermount.mount(
    "/tmp/my-mount",
    ["fsname=demo", "subtype=demo", "default_permissions"]
  )

:ok = FuseServer.Native.select_read(handle)

receive do
  {:select, ^handle, :undefined, :ready_input} ->
    {:ok, request_bytes} = FuseServer.Native.read_frame(handle)
    {:ok, op, header, request} = FuseServer.Protocol.decode_request(request_bytes)
    # ... build a reply struct for `op` ...
    response_bytes = FuseServer.Protocol.encode_response(header.unique, reply, 0)
    :ok = FuseServer.Native.write_frame(handle, response_bytes)
end

:ok = FuseServer.Fusermount.unmount("/tmp/my-mount")
```

`FuseServer.Fusermount.mount/2` calls into a NIF that uses
`posix_spawn(3)` to run `fusermount3` with one end of a `socketpair(2)`
inherited as fd 3, then receives the resulting `/dev/fuse` fd via
`SCM_RIGHTS`. `FuseServer.Fusermount.unmount/1` invokes
`fusermount3 -u` via an Erlang `Port` so the BEAM's child-process
management reaps the helper without colliding with `SIGCHLD = SIG_IGN`.

See `FuseServer.Native`, `FuseServer.Fusermount`, and
`FuseServer.Protocol` for full documentation.

## Tests without /dev/fuse

CI hosts that lack FUSE support can still exercise the transport:

```elixir
{:ok, {read_fd, write_fd}} = FuseServer.Native.pipe_pair()
```

returns a non-blocking pipe pair wrapped in the same resource type, so
the `select_read` / `read_frame` / `write_frame` path can be driven
end-to-end. Tests that exercise `FuseServer.Fusermount.mount/2` are
tagged `:fuse` and skipped on hosts where `/dev/fuse` is not available.

## Licence

Apache-2.0 — see [LICENSE](../LICENSE) for details.
