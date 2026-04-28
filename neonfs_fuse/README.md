# NeonFS FUSE

FUSE filesystem interface for NeonFS, providing local filesystem access to
cluster-stored data.

This package depends on `neonfs_client` only — it has **no dependency on
`neonfs_core`**. All communication with core nodes happens via Erlang
distribution, routed through `NeonFS.Client.Router`.

## How It Works

The FUSE node mounts volumes as local directories via a pure-BEAM
implementation built on top of `FuseServer.Fusermount` (which uses the
`fusermount3` userspace helper to obtain a `/dev/fuse` fd) and a
`NeonFS.FUSE.Session` GenServer that owns the fd and dispatches incoming
frames. Filesystem operations are translated into RPC calls to core nodes
through `neonfs_client`'s service discovery and routing.

### Key Modules

- `NeonFS.FUSE.Handler` — translates FUSE operations into core RPC calls
- `NeonFS.FUSE.Session` — owns the FUSE fd and dispatches frames
- `NeonFS.FUSE.MountManager` — manages mount/unmount lifecycle per volume
- `NeonFS.FUSE.Application` — OTP application and supervision tree

## Prerequisites

- FUSE support: `libfuse3-dev` (Debian/Ubuntu) or equivalent
- `fusermount3` available on `$PATH`
- A running NeonFS core cluster to connect to

## Building

```bash
mix deps.get
mix compile
```

The fuser-NIF Rust crate that this package used to ship was removed in
the cutover (#107). The minimal syscall NIF lives in the
`fuse_server` package now.

## Testing

```bash
mix test                          # unit tests (run without core)
mix check --no-retry              # full check suite
```

Integration tests that exercise the full FUSE-to-core path live in
`neonfs_integration`.

## Running

As an OTP release:

```bash
mix release neonfs_fuse
_build/prod/rel/neonfs_fuse/bin/neonfs_fuse start
```

Or as a container:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f ../bake.hcl --load fuse
```

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
