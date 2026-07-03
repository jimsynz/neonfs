# NeonFS FUSE

Mount a NeonFS volume as a local directory. `neonfs_fuse` turns
cluster-stored data into an ordinary POSIX filesystem — `ls`, `cp`,
your editor, your build — backed by replicated, deduplicated,
optionally encrypted storage.

What makes it unusual: the FUSE protocol is spoken **natively on the
BEAM**. There is no libfuse binding and no long-running native event
loop — a minimal syscall NIF (from the [`wick`](https://hex.pm/packages/wick)
library) obtains the `/dev/fuse` file descriptor via `fusermount3`, and
from there frame parsing, dispatch, and replies are all Elixir processes
under supervision.

This package depends on [`neonfs_client`](../neonfs_client/) only — it
has **no dependency on `neonfs_core`**. All communication with core
nodes happens via Erlang distribution and the TLS data plane, routed
through the client library.

## How it works

- `NeonFS.FUSE.Session` owns the `/dev/fuse` fd and dispatches incoming
  frames
- `NeonFS.FUSE.Handler` translates FUSE operations into core RPC calls
- `NeonFS.FUSE.MountManager` manages the mount/unmount lifecycle per volume
- File reads and writes stream chunk by chunk through
  `NeonFS.Client.ChunkReader` / `ChunkWriter` — memory use is bounded by
  chunk size, not file size

## Prerequisites

- FUSE support: `libfuse3-dev` (Debian/Ubuntu) or equivalent, with
  `fusermount3` on `$PATH`
- A running NeonFS core cluster to connect to

## Building and testing

```bash
mix deps.get
mix compile
mix test                          # unit tests (run without core)
mix check --no-retry              # full check suite
```

End-to-end FUSE tests (real mounts against a peer cluster) live in
`test/integration/`.

## Running

As an OTP release:

```bash
mix release neonfs_fuse
_build/prod/rel/neonfs_fuse/bin/neonfs_fuse start
```

Or as a container:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f ../containers/bake.hcl --load fuse
```

Mounting is driven from the CLI:

```bash
neonfs fuse mount my-volume /mnt/my-volume
```

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
