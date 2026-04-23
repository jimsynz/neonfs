# NeonFS

A BEAM-orchestrated distributed filesystem combining Elixir's coordination strengths with Rust's performance for storage operations.

NeonFS provides location-transparent storage where data is accessible from any node regardless of where it's physically stored. It offers configurable durability, tiered storage, compression, encryption, and multiple access methods.

Current version: **v0.1.11**.

## Design Principles

- **Separation of concerns** — Elixir handles coordination, policy, and APIs; Rust handles I/O, chunking, and cryptography via Rustler NIFs.
- **Content-addressed storage** — immutable, SHA-256-identified chunks enable deduplication, integrity verification, and data recovery.
- **Location transparency** — metadata flows through Elixir for a single code path, while bulk chunk data moves over a dedicated TLS data plane.
- **Per-volume isolation** — each volume has its own supervision tree with independent durability, tiering, compression, encryption, and ACL settings.
- **No whole-file buffering** — read and write paths process data as streams of chunks; RAM usage is bounded by chunk size, not file size.

## Architecture

```
    ┌────────────────────────────────────────────────────────────┐
    │                 Elixir control plane (neonfs_core)         │
    │         Ra consensus · quorum metadata · policy · ACLs     │
    │                │              │              │             │
    │         neonfs_blob NIF   key mgmt    cluster CA (x509)    │
    └────────────────────────────────────────────────────────────┘
         ▲                                 ▲
         │ Erlang distribution             │ TLS data plane
         │ (metadata RPCs)                 │ (bulk chunks)
         │                                 │
    ┌────┴─────────────────────────────────┴─────────────────────┐
    │                    neonfs_client (shared)                  │
    │      Router · Discovery · ChunkReader · Transport pool     │
    └────────────────────────────────────────────────────────────┘
        │            │            │            │
  ┌─────┴───┐  ┌─────┴───┐  ┌─────┴───┐  ┌─────┴───┐
  │ FUSE    │  │ NFSv3 + │  │ S3-     │  │ WebDAV  │
  │ mount   │  │ NLM v4  │  │ compat  │  │ + locks │
  └─────────┘  └─────────┘  └─────────┘  └─────────┘
```

Interface packages (FUSE, NFS, S3, WebDAV) depend only on `neonfs_client`. They talk to core nodes via Erlang distribution for metadata and fetch chunk data directly over a dedicated TLS data plane, keeping bulk traffic off the BEAM distribution channel.

## Packages

| Package | Description |
|---------|-------------|
| [`neonfs_client`](neonfs_client/) | Shared types, service discovery, `Router`, `ChunkReader`, transport pooling |
| [`neonfs_core`](neonfs_core/) | Storage engine, metadata, Ra consensus, cluster CA, policy |
| [`neonfs_iam`](neonfs_iam/) | Identity and access management (users, groups, policies, identity mappings) |
| [`neonfs_fuse`](neonfs_fuse/) | FUSE filesystem interface |
| [`neonfs_nfs`](neonfs_nfs/) | NFSv3 server with NLM v4 advisory locking |
| [`neonfs_s3`](neonfs_s3/) | S3-compatible HTTP server |
| [`neonfs_webdav`](neonfs_webdav/) | WebDAV server with collection locking and dead properties |
| [`neonfs_docker`](neonfs_docker/) | Docker / Podman VolumeDriver plugin (HTTP over Unix socket) |
| [`neonfs_omnibus`](neonfs_omnibus/) | All-in-one bundle of core + all interface packages |
| [`neonfs_integration`](neonfs_integration/) | Peer-based multi-node integration test suite |
| [`neonfs-cli`](neonfs-cli/) | Rust command-line interface for cluster management |

### Dependency graph

```
neonfs_client  ← neonfs_core
neonfs_client  ← neonfs_iam
neonfs_client  ← neonfs_fuse
neonfs_client  ← neonfs_nfs
neonfs_client  ← neonfs_s3       (firkin)
neonfs_client  ← neonfs_webdav   (davy)
neonfs_client  ← neonfs_docker
neonfs_core, neonfs_fuse, neonfs_nfs, neonfs_s3, neonfs_webdav, neonfs_docker  ← neonfs_omnibus
all of the above                                                               ← neonfs_integration
```

Interface packages have **no dependency** on `neonfs_core`. All communication with core nodes goes through `NeonFS.Client.Router`.

## Features

### Storage
- **Flexible durability** — replication (factor N) or Reed–Solomon erasure coding (e.g. 10+4) per volume.
- **Storage tiering** — hot (NVMe), warm (SATA SSD), and cold (HDD) tiers with access-based promotion/demotion and HDD spin-down.
- **Content-addressed chunks** — FastCDC chunking, SHA-256 addressing, automatic deduplication.
- **Compression** — per-volume Zstandard with configurable levels.
- **Encryption** — server-side AES-256-GCM or envelope encryption, per-volume key management with background rotation.

### Cluster
- **Ra consensus** — metadata and service registry backed by Raft.
- **Leaderless quorum metadata** — chunk, file, and stripe indexes use R+W>N quorum reads/writes.
- **HLC-based conflict resolution** — hybrid logical clocks for ordering across nodes.
- **Self-signed cluster CA** — ECDSA P-256 via pure-Elixir `x509`, mTLS on the data plane, auto-renewing node certificates.
- **TLS Erlang distribution** — all BEAM distribution traffic encrypted.
- **Event notification** — `:pg`-based cache invalidation for interface nodes.
- **Distributed lock manager** — quorum-backed DLM shared across all interface protocols.

### Access methods

Shipped:

- FUSE mounts
- NFSv3 + NLM v4 advisory locking
- S3-compatible HTTP API (virtual-hosted-style, RFC 7232 conditional requests, multipart uploads)
- WebDAV with collection locking and dead property storage

Tracked for future work:

- Docker/Podman VolumeDriver plugin ([#243](https://harton.dev/project-neon/neonfs/issues/243))
- Kubernetes CSI driver ([#244](https://harton.dev/project-neon/neonfs/issues/244))
- CIFS/SMB via Samba VFS ([#116](https://harton.dev/project-neon/neonfs/issues/116))
- containerd content store ([#196](https://harton.dev/project-neon/neonfs/issues/196))

### Operations

- Prometheus metrics exporter with alerting rules.
- HTTP health endpoint for load balancers and orchestrators.
- Cluster-wide CLI queries (`drive list`, `volume list`, `gc status`, `scrub status`, etc.).
- Debian packages (amd64 + arm64) and multi-arch container images.
- systemd integration with `sd_notify` readiness.

## Prerequisites

- Elixir 1.19.5 (OTP 28)
- Erlang 28.3.1
- Rust 1.93.0
- FUSE (`libfuse3-dev` on Debian/Ubuntu)

All managed via `.tool-versions` — `asdf install` or `mise install` will fetch them.

## Getting started

Build everything from the repository root:

```bash
mix deps.get
mix compile
```

Run the full check suite (formatting, Credo, Dialyzer, Clippy, tests) across every subproject:

```bash
mix check --no-retry
```

Run tests for a specific package:

```bash
cd neonfs_core && mix test
```

## Container builds

Targets live in `containers/bake.hcl`:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f containers/bake.hcl --load \
  base core fuse nfs s3 webdav docker omnibus cli
```

`--load` is required for local testing; it loads images into the local Docker daemon rather than pushing to a registry.

## Deployment

Interface nodes (FUSE, NFS, S3, WebDAV) run as separate Erlang nodes and connect to core nodes via Erlang distribution. For small deployments, `neonfs_omnibus` bundles core + all interfaces into a single release.

Bootstrap a cluster by initialising the first core node, then join additional nodes using single-use invite tokens that carry a CSR for TLS certificate issuance.

See [`docs/operator-guide.md`](docs/operator-guide.md) for the end-to-end operator guide (installation, bootstrap, node join, volume/drive management, upgrades, troubleshooting), and [`docs/user-guide.md`](docs/user-guide.md) if you're consuming a running cluster (CLI, access methods, credentials, file operations, performance expectations). [`docs/deployment.md`](docs/deployment.md) and [`docs/orchestration.md`](docs/orchestration.md) cover Docker Compose, Swarm, and Kubernetes recipes in depth, [`docs/cli-reference.md`](docs/cli-reference.md) is the full `neonfs` command-line reference, and [`docs/docker-plugin.md`](docs/docker-plugin.md) covers the Docker VolumeDriver plugin specifically. Contributors should start with [`docs/developer-guide.md`](docs/developer-guide.md) for repo layout, subsystem pointers, Rustler conventions, testing strategy, and the contributing workflow.

## Project resources

- **[Wiki](https://harton.dev/project-neon/neonfs/wiki)** — full specification, architecture, codebase patterns, historical progress.
- **[Issues](https://harton.dev/project-neon/neonfs/issues)** — active work, feature requests, bug reports.
- **[Changelog](CHANGELOG.md)** — release notes (v0.1.0 onwards).

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
