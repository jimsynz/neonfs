# NeonFS

**Store it once. Serve it everywhere.**

NeonFS is a distributed filesystem that keeps one deduplicated,
content-addressed copy of your data and serves it over FUSE, NFSv3, S3,
WebDAV, Docker volumes, containerd, and Kubernetes CSI — simultaneously,
from any node in the cluster. Write a file over NFS from a legacy
application, read it from a Kubernetes pod, list it with `aws s3 ls`,
and browse it in Finder: same bytes, same namespace, no sync jobs.

It pairs the BEAM (Elixir/OTP) for coordination — Raft consensus,
supervision trees, failure recovery, policy — with Rust for the data
path — chunking, hashing, compression, encryption, and erasure coding
as Rustler NIFs. Each side does what it is best at.

## Why NeonFS?

- **Every protocol, one namespace.** Interface nodes are thin,
  independently deployable translators over a shared client library.
  Adding an access method never duplicates your data.
- **Per-volume policy, not per-cluster policy.** Each volume chooses its
  own durability (replication factor N, or Reed–Solomon erasure coding
  such as 10+4), storage tiering, Zstandard compression, and AES-256-GCM
  encryption with background key rotation. Each volume also gets its own
  supervision tree, so one volume's trouble doesn't take down the rest.
- **Content-addressed storage.** FastCDC chunking with SHA-256
  addressing gives you deduplication for free, verifiable integrity end
  to end, and chunk-level repair when a drive dies.
- **Secure by default.** The cluster operates its own ECDSA P-256
  certificate authority. Node certificates auto-renew, bulk data moves
  over an mTLS data plane, Erlang distribution itself runs over TLS, and
  nodes join with single-use invite tokens that carry a CSR.
- **Bounded memory, always.** Every read and write path streams data
  chunk by chunk. RAM usage is bounded by chunk size, never file size —
  a small node serves arbitrarily large files without buffering them.
- **Built to be operated.** Scrubbing, repair, anti-entropy, drive
  evacuation, garbage collection, tier promotion/demotion with HDD
  spin-down, DR snapshots, a Prometheus exporter with alerting rules,
  incident runbooks, and a CLI that covers the lot.

## Access methods

| Protocol | Good for | Status |
|----------|----------|--------|
| FUSE | Local POSIX mounts on Linux | Shipped |
| NFSv3 + NLM v4 | Network mounts — clients built into Linux, macOS, Windows | Shipped |
| S3-compatible HTTP | `aws` CLI and SDKs — Signature v4 auth, multipart uploads, conditional requests | Shipped |
| WebDAV | Finder, Explorer, rclone — collection locking, dead properties | Shipped |
| Docker / Podman volume plugin | Container volumes backed by the cluster | Shipped |
| containerd content store | Image layers stored as NeonFS objects | Shipped |
| Kubernetes CSI | Persistent volumes via Helm chart | Shipped — kind/k3d end-to-end test outstanding ([#319](https://harton.dev/project-neon/neonfs/issues/319)) |
| CIFS/SMB (Samba VFS) | Native Windows/macOS shares | In progress — `vfs_neonfs.so` C shim + Elixir bridge built and CI-tested; packaging ([#385](https://harton.dev/project-neon/neonfs/issues/385)) and end-to-end `smbd` test ([#386](https://harton.dev/project-neon/neonfs/issues/386)) outstanding ([#116](https://harton.dev/project-neon/neonfs/issues/116)) |

File locks taken over one protocol are honoured by the others — a
quorum-backed distributed lock manager is shared across all interfaces.

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
       │       │       │        │        │        │         │
    ┌──┴──┐ ┌──┴──┐ ┌──┴──┐ ┌───┴───┐ ┌──┴───┐ ┌──┴────┐ ┌──┴──┐
    │FUSE │ │NFSv3│ │ S3  │ │WebDAV │ │Docker│ │contai-│ │ CSI │
    │mount│ │+NLM │ │API  │ │+locks │ │volume│ │ nerd  │ │     │
    └─────┘ └─────┘ └─────┘ └───────┘ └──────┘ └───────┘ └─────┘
```

Interface packages depend only on `neonfs_client` — never on
`neonfs_core`. They reach core nodes over Erlang distribution for
metadata and fetch chunk data directly over a dedicated TLS data plane,
keeping bulk traffic off the BEAM distribution channel.

Cluster metadata uses two mechanisms: Ra (Raft) consensus for membership,
the service registry, and volume configuration; and leaderless
R+W>N quorum replication with hybrid-logical-clock conflict resolution
for the high-churn chunk, file, and stripe indexes.

### Packages

| Package | Description |
|---------|-------------|
| [`neonfs_core`](neonfs_core/) | Storage engine, metadata, Ra consensus, cluster CA, policy |
| [`neonfs_client`](neonfs_client/) | Shared types, service discovery, RPC routing, chunk streaming |
| [`neonfs_fuse`](neonfs_fuse/) | FUSE filesystem interface |
| [`neonfs_nfs`](neonfs_nfs/) | NFSv3 server with NLM v4 advisory locking |
| [`neonfs_s3`](neonfs_s3/) | S3-compatible HTTP server |
| [`neonfs_webdav`](neonfs_webdav/) | WebDAV server with collection locking and dead properties |
| [`neonfs_docker`](neonfs_docker/) | Docker / Podman VolumeDriver plugin |
| [`neonfs_containerd`](neonfs_containerd/) | containerd content-store gRPC plugin |
| [`neonfs_csi`](neonfs_csi/) | Kubernetes CSI driver (with [Helm chart](deploy/charts/neonfs-csi/)) |
| [`neonfs_cifs`](neonfs_cifs/) | Samba VFS module backend — `vfs_neonfs.so` C shim + Elixir bridge (packaging and end-to-end test in progress) |
| [`neonfs_iam`](neonfs_iam/) | Identity and access management domain (scaffold — resources land incrementally) |
| [`neonfs_omnibus`](neonfs_omnibus/) | All-in-one bundle: core + every interface in a single release |
| [`neonfs-cli`](neonfs-cli/) | Rust command-line interface for cluster administration |
| [`nfs_server`](nfs_server/) | Standalone library: pure-Elixir NFSv3/ONC-RPC server |
| [`neonfs_test_support`](neonfs_test_support/) | Shared peer-cluster test scaffolding |
| [`neonfs_integration`](neonfs_integration/) | Multi-node cluster-correctness test suite |

## Try it

The [QEMU test rig](test-rig/) brings up a real cluster — base OS, `.deb`
packages, systemd units, the works — with one command:

```bash
cd test-rig
./neonfs-rig up              # single-node cluster with a volume, end to end
./neonfs-rig cli 1 -- volume list
NODES=3 ./neonfs-rig up      # or a 3-node cluster with replicas=3
```

See [`test-rig/README.md`](test-rig/README.md) for requirements (QEMU,
nfpm) and the acceptance suite that drives every interface against the
running cluster.

## Installing

**Debian packages** (amd64 + arm64) from the project repository:

```bash
curl -fsSL https://harton.dev/api/packages/project-neon/debian/repository.key \
  | sudo tee /etc/apt/keyrings/neonfs.asc > /dev/null
echo "deb [signed-by=/etc/apt/keyrings/neonfs.asc] https://harton.dev/api/packages/project-neon/debian trixie main" \
  | sudo tee /etc/apt/sources.list.d/neonfs.list
sudo apt update
sudo apt install neonfs-omnibus        # everything in one node, or:
sudo apt install neonfs-core neonfs-fuse neonfs-nfs neonfs-s3   # split services
```

**Container images** are published per interface under
`ghcr.io/jimsynz/neonfs/` (`core`, `fuse`, `nfs`, `s3`, `webdav`,
`docker`, `containerd`, `csi`, `omnibus`, `cli`). The CSI driver
installs into Kubernetes via the [Helm chart](deploy/charts/neonfs-csi/).

See [`packaging/README.md`](packaging/README.md) for deployment
topologies and [`docs/deployment.md`](docs/deployment.md) for Docker
Compose, Swarm, and Kubernetes recipes.

## Your first cluster

```bash
neonfs cluster init --name production           # first core node: Ra, cluster CA, certs
neonfs drive add --path /data/nvme0 --tier hot --capacity 1T --id nvme0
neonfs volume create my-data --replicas 3 --compression zstd
neonfs fuse mount my-data /mnt/my-data
```

Additional nodes join with `neonfs cluster create-invite` on an existing
node and `neonfs cluster join` on the new one — the invite token is
single-use and carries the CSR for the node's TLS certificate.

The [operator guide](docs/operator-guide.md) covers the full lifecycle
(bootstrap, joins, drives, volumes, upgrades, troubleshooting); the
[user guide](docs/user-guide.md) covers consuming a running cluster.

## Documentation

| Document | Covers |
|----------|--------|
| [Operator guide](docs/operator-guide.md) | Installation, bootstrap, node join, drives, volumes, upgrades |
| [User guide](docs/user-guide.md) | Access methods, credentials, file operations, performance expectations |
| [CLI reference](docs/cli-reference.md) | Every `neonfs` command and flag |
| [Deployment](docs/deployment.md) / [Orchestration](docs/orchestration.md) | Docker Compose, Swarm, Kubernetes |
| [Docker plugin](docs/docker-plugin.md) / [containerd](docs/containerd.md) | Container-runtime integration |
| [API reference](docs/api-reference.md) | Programmatic interfaces |
| [Runbooks](docs/runbooks/) | Incident-shaped procedures: node down, drive failure, quorum loss, … |
| [Developer guide](docs/developer-guide.md) | Repo layout, subsystem pointers, testing strategy, contributing |
| [Wiki](https://harton.dev/project-neon/neonfs/wiki) | Full specification, architecture, codebase patterns |

## Project status

NeonFS is **experimental**. It is feature-rich and heavily tested —
unit, property, NIF-boundary, multi-node integration, and full
VM-based acceptance tests — but there are no production deployments
yet and no backwards-compatibility guarantees between releases:
on-disk formats and APIs may change without migration paths. Treat it
as something to evaluate and hack on, not somewhere to put the only
copy of your data.

## Issues and contributing

- **Bugs and feature requests** — [open an issue](https://harton.dev/project-neon/neonfs/issues).
- **Contributing** — start with the [developer guide](docs/developer-guide.md),
  then build with `mix deps.get && mix compile` and run the full check
  suite with `mix check --no-retry` (needs Elixir 1.19 / OTP 28 and
  Rust 1.93 — all pinned in `.tool-versions`, so `asdf install` or
  `mise install` fetches them).
- **Release notes** — [CHANGELOG.md](CHANGELOG.md), generated from
  conventional commits.

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
