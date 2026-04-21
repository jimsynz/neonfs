# FuseServer

A standalone Elixir library for building FUSE-based userspace filesystems.

`fuse_server` owns the low-level glue between the BEAM and the Linux FUSE
kernel ABI — opening `/dev/fuse`, `enif_select`-based readiness
notifications, and a bounded read/write API for frames — and (in future
sub-issues) will grow upward to cover the FUSE protocol codec, INIT
handshake, and a `FuseServer.Backend` behaviour.

Today (issue #274) it exposes only the transport layer:

```elixir
{:ok, fd} = FuseServer.Native.open_dev_fuse()
:ok = FuseServer.Native.select_read(fd)
receive do
  {:select, ^fd, :undefined, :ready_input} ->
    {:ok, request_bytes} = FuseServer.Native.read_frame(fd)
    # ... produce `response_bytes` ...
    :ok = FuseServer.Native.write_frame(fd, response_bytes)
end
```

See `FuseServer.Native` for full documentation.

## Tests without /dev/fuse

CI hosts that lack FUSE support can still exercise the transport:

```elixir
{:ok, {read_fd, write_fd}} = FuseServer.Native.pipe_pair()
```

returns a non-blocking pipe pair wrapped in the same resource type, so the
`select_read` / `read_frame` / `write_frame` path can be driven
end-to-end.
