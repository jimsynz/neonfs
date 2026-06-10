# neonfs_cifs

CIFS/SMB access to NeonFS — native Windows and macOS file shares
backed by the cluster, via a Samba VFS module. **In progress**: the
Elixir side is implemented; the Samba-side C shim and packaging are
still to come (see the
[#116 epic](https://harton.dev/project-neon/neonfs/issues/116)).

## How it will fit together

Samba handles the SMB protocol, authentication, and Windows semantics
— problems already solved well — while a thin VFS module
(`vfs_neonfs.so`) forwards filesystem operations to this package over
a Unix domain socket, and from there through `neonfs_client` to the
cluster. The framing is 4-byte big-endian length-prefixed ETF
(`:erlang.term_to_binary/1` end to end — no JSON, no protobuf).

## What's implemented (this package)

- `NeonFS.CIFS.Application` — owns the listener, per-connection state,
  and the cluster service-registry registration.
- A `ThousandIsland`-based UDS server with length-prefix framing.
- A handler dispatching each `{:op_atom, args}` tuple to a per-VFS-op
  handler, covering the ~20 "must implement" Samba VFS ops (lifecycle,
  metadata, file I/O, directory iteration, mutations, statvfs).
- A per-connection handle table — synthetic 64-bit tokens mapping to
  NeonFS volume + path, so the C shim can present POSIX-style fds.

## What's outstanding

- `vfs_neonfs.so` C shim and in-tree Samba build
  ([#384](https://harton.dev/project-neon/neonfs/issues/384))
- Container image, Debian package, omnibus integration
  ([#385](https://harton.dev/project-neon/neonfs/issues/385))
- End-to-end test against a real `smbd`
  ([#386](https://harton.dev/project-neon/neonfs/issues/386))

## Building and testing

```bash
mix deps.get
mix compile
mix test
mix check --no-retry
```

## Licence

Apache-2.0 — see [LICENSE](../LICENSE) for details.
