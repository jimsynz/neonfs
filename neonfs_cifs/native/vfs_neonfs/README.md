# vfs_neonfs

The C side of the `vfs_neonfs.so` Samba VFS module for NeonFS.

This directory holds the **protocol half** (#1169): a wire client that speaks
the `neonfs_cifs` ETF protocol over a Unix domain socket, with no Samba
dependency. The Samba VFS glue (`vfs_*` op callbacks against a pinned in-tree
Samba) lands separately in #1170 and builds on this client.

## Wire contract

Mirrors `NeonFS.CIFS.Handler` / `NeonFS.CIFS.Listener`:

- **Framing**: 4-byte big-endian length prefix + ETF body. The Elixir listener
  runs its socket in `packet: 4` mode (auto-framing its side); this client adds
  the prefix on send and strips it on receive itself.
- **Request**: an ETF `{op_atom, args_map}` tuple. Argument-map keys are ETF
  binaries; values are binaries or integers.
- **Reply**: `{:ok, payload_map}` (atom-keyed payload) or `{:error, errno_atom}`.

See `wire.h` for the full op surface (the 20 "must implement" VFS ops) and the
decoded result structs. ETF encode/decode uses `erl_interface` (`ei`).

## Build + test

Needs a C compiler and Erlang on `PATH` (the `ei` headers/libs are discovered
from the running Erlang install — no Samba required):

```sh
make test   # builds the wire client + mock-responder harness and runs it
```

The harness (`test_wire.c`) pairs the client against a mock responder over a
`socketpair`, driving every op's encode → frame → decode round-trip against
canned ETF replies shaped like `NeonFS.CIFS.Handler`'s, asserting the decoded
C structs and errno translations.
