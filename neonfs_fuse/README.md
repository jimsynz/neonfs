# NeonFS FUSE

FUSE filesystem interface for NeonFS, providing local filesystem access to
cluster-stored data.

This package depends on `neonfs_client` only — it has **no dependency on
`neonfs_core`**. All communication with core nodes happens via Erlang
distribution, routed through `NeonFS.Client.Router`.

## How It Works

The FUSE node mounts volumes as local directories using the `fuser` Rust crate
(via Rustler NIFs). Filesystem operations (read, write, mkdir, etc.) are
translated into RPC calls to core nodes through the client's service discovery
and routing infrastructure.

### Key Modules

- `NeonFS.FUSE.Handler` — translates FUSE operations into core RPC calls
- `NeonFS.FUSE.MountManager` — manages mount/unmount lifecycle per volume
- `NeonFS.FUSE.Application` — OTP application and supervision tree

## Prerequisites

- FUSE support: `libfuse3-dev` (Debian/Ubuntu) or equivalent
- A running NeonFS core cluster to connect to

## Building

```bash
mix deps.get
mix compile    # compiles Elixir and Rust NIFs
```

Rust toolchain (1.93+) is required for the NIF crate in `native/`.

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
