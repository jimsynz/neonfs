# NeonFS NFS

NFSv3 server interface for NeonFS, providing network filesystem access to
cluster-stored data via the standard NFS protocol.

This package depends on `neonfs_client` only — it has **no dependency on
`neonfs_core`**. All communication with core nodes happens via Erlang
distribution, routed through `NeonFS.Client.Router`.

## How It Works

The NFS node exports volumes as NFSv3 shares using the `nfs3_server` Rust crate
(via Rustler NIFs). NFS operations (read, write, mkdir, etc.) are translated
into RPC calls to core nodes through the client's service discovery and routing
infrastructure.

### Key Modules

- `NeonFS.NFS.Handler` — translates NFS operations into core RPC calls
- `NeonFS.NFS.ExportManager` — manages volume export lifecycle
- `NeonFS.NFS.Application` — OTP application and supervision tree

## Prerequisites

- A running NeonFS core cluster to connect to
- NFS client on the accessing machine (built into Linux, macOS, Windows)

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

Integration tests that exercise the full NFS-to-core path live in
`neonfs_integration`.

## Running

As an OTP release:

```bash
mix release neonfs_nfs
_build/prod/rel/neonfs_nfs/bin/neonfs_nfs start
```

Or as a container:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f ../bake.hcl --load nfs
```

## Client Usage

Mount a volume:

```bash
mount -t nfs -o vers=3,nolock,tcp server:/volume_name /mnt/neonfs
```

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
