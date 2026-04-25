# neonfs_cifs

Samba VFS module backend for NeonFS — the Elixir side of the
`vfs_neonfs` plugin. Listens on a Unix domain socket for
length-prefixed ETF messages from Samba's process-per-connection
worker (`smbd`) and routes each VFS op through `neonfs_client` to a
NeonFS core cluster.

This package provides:

- An `OTP application` (`NeonFS.CIFS.Application`) that owns the
  listener, the per-connection state, and the registrar handle into
  the cluster's service registry.
- A `ThousandIsland`-based UDS server with 4-byte big-endian
  length-prefix framing and `:erlang.term_to_binary/1` ↔
  `binary_to_term/1` end-to-end (no JSON, no protobuf).
- A handler module that dispatches each incoming `{:op_atom, args}`
  tuple to a per-VFS-op handler. The first slice covers the ~20
  "Must implement" Samba VFS ops (lifecycle, metadata, file I/O,
  directory iteration, mutations, statvfs).
- A handle table per connection — synthetic 64-bit tokens that map
  to NeonFS volume + path so the C shim can pretend it has POSIX
  fds.

The C shim itself (`vfs_neonfs.so`), the in-tree Samba build, the
container packaging, and the end-to-end `smbd` test all live in
follow-up sub-issues — see #116 for the parent epic.
