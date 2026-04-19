# NeonFS

A BEAM-orchestrated distributed filesystem combining Elixir's coordination
strengths with Rust's performance for storage operations.

NeonFS provides location-transparent storage where data is accessible from any
node regardless of where it's physically stored. It offers configurable
durability, tiered storage, and multiple access methods including FUSE, an
S3-compatible API, Docker volume plugin, and CIFS/SMB.

## Design Principles

- **Separation of concerns** — Elixir handles coordination, policy, and APIs;
  Rust handles I/O, chunking, and cryptography via Rustler NIFs
- **Content-addressed storage** — immutable, SHA-256-identified chunks enable
  deduplication, integrity verification, and data recovery
- **Location transparency** — all data flows through Elixir for a single code
  path regardless of whether access is local or remote
- **Per-volume isolation** — each volume has its own supervision tree with
  independent durability, tiering, compression, and encryption settings

## Architecture

```
                  ┌──────────────────────────────────────┐
                  │       Elixir Control Plane            │
                  │  Ra consensus · policy · APIs         │
                  │           │            │              │
                  │    ┌──────┴──┐   ┌─────┴───┐         │
                  │    │neonfs_  │   │neonfs_  │         │
                  │    │blob NIF │   │fuse NIF │         │
                  │    └─────────┘   └─────────┘         │
                  └──────────────────────────────────────┘
                            │                │
                     Chunk storage      FUSE mounts
```

## Packages

| Package | Description |
|---------|-------------|
| [`neonfs_client`](neonfs_client/) | Shared types and service discovery client library |
| [`neonfs_core`](neonfs_core/) | Storage engine, metadata, and cluster coordination |
| [`neonfs_fuse`](neonfs_fuse/) | FUSE filesystem interface |
| [`neonfs_integration`](neonfs_integration/) | Multi-node integration test suite |
| [`neonfs-cli`](neonfs-cli/) | Command-line interface for cluster management |

### Dependency Graph

```
neonfs_client  ← neonfs_core
neonfs_client  ← neonfs_fuse
neonfs_core    ← neonfs_integration
neonfs_fuse    ← neonfs_integration
```

`neonfs_fuse` has no dependency on `neonfs_core`. All communication between FUSE
and core nodes happens via Erlang distribution, routed through `NeonFS.Client.Router`.

## Features

- **Flexible durability** — choose between replication (factor N) for fast access
  or erasure coding (e.g. 10+4) for storage efficiency
- **Storage tiering** — hot (NVMe), warm (SATA SSD), and cold (HDD) tiers with
  automatic promotion/demotion based on access patterns
- **Compression** — per-volume Zstandard compression with configurable levels
- **Encryption** — server-side (AES-256-GCM) or envelope encryption with
  per-volume and per-chunk key management
- **Cluster CA** — self-signed ECDSA P-256 certificate authority for TLS on the
  data transfer plane
- **Event notification** — push-based cache invalidation and event dispatch
- **Multiple access methods** — FUSE mounts, S3-compatible API, Docker/Podman
  volumes, Kubernetes CSI, CIFS/SMB

## Prerequisites

- Elixir 1.19+ (OTP 28)
- Rust 1.93+
- FUSE (libfuse3-dev on Debian/Ubuntu)

## Getting Started

Build all packages from the repository root:

```bash
mix deps.get
mix compile
```

Run the test suite:

```bash
mix check --no-retry
```

Run tests for a specific package:

```bash
cd neonfs_core && mix test
```

## Container Builds

Build container images for local testing:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f bake.hcl --load core fuse cli
```

## Multi-Node Deployment

NeonFS core and FUSE nodes run as separate Erlang nodes communicating via
distribution. Bootstrap a cluster by initialising the first core node, then
join additional nodes using single-use invite tokens.

See the [wiki](https://harton.dev/project-neon/neonfs/wiki) for the full specification, patterns, and historical progress, and the [issue tracker](https://harton.dev/project-neon/neonfs/issues) for active work.

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
