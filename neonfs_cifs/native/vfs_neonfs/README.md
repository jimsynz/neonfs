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
make test         # builds the wire client + mock-responder harness and runs it
make wire_probe   # builds the live-listener probe (driven by the ExUnit test)
```

The op-drive sequence lives once in `probe_ops.c` and is shared by both
binaries so their canned contracts can't drift:

- **`test_wire.c`** pairs the client against an in-process mock responder over a
  `socketpair`, encoding canned ETF replies with `ei` on both sides — a pure-C
  unit test of the client's own encode → frame → decode round-trip.
- **`wire_probe.c`** connects to a live `NeonFS.CIFS.Listener` Unix socket
  (passed as `argv[1]`) and runs the same sequence against the real Elixir
  `term_to_binary` / `binary_to_term` path. It is launched by
  `NeonFS.CIFS.LiveListenerTest`, which binds a listener with a canned-reply
  handler (#1400).
