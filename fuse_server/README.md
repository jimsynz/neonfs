# FuseServer

A standalone Elixir library for building FUSE-based userspace filesystems.

`fuse_server` owns the low-level glue between the BEAM and the Linux FUSE
kernel ABI — opening `/dev/fuse`, mounting via `fusermount3`,
`enif_select`-based readiness notifications, and a bounded read/write
API for frames — and (in future sub-issues) will grow upward to cover
the FUSE protocol codec, INIT handshake, and a `FuseServer.Backend`
behaviour.

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
    # ... produce `response_bytes` ...
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

See `FuseServer.Native` and `FuseServer.Fusermount` for full
documentation.

## Tests without /dev/fuse

CI hosts that lack FUSE support can still exercise the transport:

```elixir
{:ok, {read_fd, write_fd}} = FuseServer.Native.pipe_pair()
```

returns a non-blocking pipe pair wrapped in the same resource type, so the
`select_read` / `read_frame` / `write_frame` path can be driven
end-to-end. Tests that exercise `FuseServer.Fusermount.mount/2` are
tagged `:fuse` and skipped on hosts where `/dev/fuse` is not available.
