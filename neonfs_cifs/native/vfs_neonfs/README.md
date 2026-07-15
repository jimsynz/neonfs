# vfs_neonfs

The C side of the `vfs_neonfs.so` Samba VFS module for NeonFS.

Two layers:

- **Protocol half** (#1169): `wire.c` / `wire.h` — a wire client that speaks
  the `neonfs_cifs` ETF protocol over a Unix domain socket, with no Samba
  dependency. Unit-tested standalone via `make test` (see below).
- **Samba VFS glue** (#1170): `vfs_neonfs.c` — implements Samba's
  `struct vfs_fn_pointers` (the 20 "must implement" ops), each marshalling to
  the wire client. Opaque per-open handles are held in the `files_struct` via
  Samba's FSP extension mechanism; paths come from `smb_fname->base_name`.

## Building the Samba module

Samba VFS modules cannot be built out-of-tree, and the module must ABI- **and
symbol-version-**match the host `smbd` (a bespoke `./configure` produces a
`SAMBA_<upstream>_PRIVATE_SAMBA` symbol version the distro's `samba-libs`
doesn't provide, so `smbd` refuses to load it — #1548). So
[`packaging/build-vfs-deb.sh`](../../../packaging/build-vfs-deb.sh) builds it
inside the distro's **own** Samba source (`apt-get source samba`) as the
**`samba-vfs-neonfs`** Debian package — the same way Debian builds
`samba-vfs-ceph`. It drops `vfs_neonfs.c` + `wire.c` into `source3/modules/`,
registers `erl_interface` (`ei`) as a link dependency, adds `vfs_neonfs` to
`--with-shared-modules`, adds a `samba-vfs-neonfs` binary package
(`Depends: samba (= exact version)`), and builds via the distro's
`debian/rules`:

```sh
OUT_DIR=./dist bash packaging/build-vfs-deb.sh   # → dist/samba-vfs-neonfs_<samba-ver>_<arch>.deb
```

Needs Erlang on `PATH` (the `ei` headers/libs come from the running install and
are linked statically) plus a C toolchain, and must run where apt resolves
`samba` to the target release's version. `neonfs-cifs` and `neonfs-omnibus`
`Depends: samba-vfs-neonfs`. CI builds it in the dedicated `vfs_neonfs` job with
the Samba source tree cached.

Runtime behaviour against a real `smbd` (mounting, round-trips) is #386.

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
