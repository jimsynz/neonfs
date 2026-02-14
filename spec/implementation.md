# Implementation

This document describes the implementation phases, technical dependencies, and configuration reference.

## Implementation Phases

### Phase 1: Foundation

**Goal:** Basic single-node operation with local storage and CLI

Deliverables:
- neonfs_blob crate: content-addressed chunk storage
- neonfs_blob crate: FastCDC chunking, SHA-256 hashing
- neonfs_fuse crate: FUSE filesystem interface
- neonfs-cli crate: CLI tool using erl_dist/erl_rpc
- NeonFS.CLI.Handler module: RPC interface for CLI
- Basic Elixir supervision tree
- Rustler NIF integration for both crates
- All data flows through Elixir (single code path)
- systemd unit file and basic packaging

**Milestone:** Mount a directory, read/write files, data persists across restarts, CLI can query status

### Phase 2: Clustering

**Goal:** Multi-node cluster with metadata consensus

Deliverables:
- Ra integration for metadata storage
- WireGuard mesh setup (Headscale integration)
- Node discovery and join flow
- Basic replication (configurable factor)
- Quorum writes

**Milestone:** 3-node cluster, data replicated, survives single node failure

### Phase 3: Policies, Tiering, and Compression

**Goal:** Volume policies, tiered storage management, compression

Deliverables:
- Volume policy system
- Hot/warm/cold tier definitions
- Spin-down and spin-avoidance logic
- Compression (zstd) with hash-then-compress semantics
- Purpose-specific caching for transformed data
- Promotion/demotion background processes

**Milestone:** Multiple volumes with different policies, compression working, intelligent drive management

### Phase 4: Erasure Coding

**Goal:** Space-efficient durability for large files

Deliverables:
- Reed-Solomon encoding (Rust library integration)
- Stripe management in metadata
- Degraded read path (reconstruct from parity)
- Stripe repair process

**Milestone:** Volumes can use erasure coding, ~1.4x overhead with 4-fault tolerance

### Phase 5: Metadata Tiering

**Goal:** Scalable metadata architecture

Deliverables:
- Hybrid Logical Clocks for conflict resolution
- Consistent hashing ring for metadata segment assignment
- MetadataStateMachine v5 (segment assignments + intents, remove chunk/file/stripe maps from Ra)
- BlobStore metadata namespace (Rust NIFs + Elixir MetadataStore)
- Leaderless quorum coordinator (R+W>N reads/writes)
- Intent log for cross-segment atomicity and concurrent writer detection
- ChunkIndex, FileIndex, StripeIndex migration from Ra to quorum store
- DirectoryEntry type for efficient directory listings
- Read repair and anti-entropy for background consistency

**Milestone:** Metadata scales independently of cluster size, quorum-replicated with tunable consistency per volume

### Phase 6: Security

**Goal:** Production-ready security model

Deliverables:
- AES-256-GCM encryption in BlobStore NIFs (integrated into read/write pipeline)
- MetadataStateMachine v6 (encryption keys, volume ACLs — Tier 1 in Ra)
- Per-volume key management with wrapped keys
- Encrypted write and read paths (single NIF boundary crossing per chunk)
- Volume key rotation with background re-encryption
- UID/GID-based volume and file ACLs with POSIX enforcement
- Audit logging for security-relevant events
- CLI security commands

**Milestone:** Encrypted volumes with key rotation, UID/GID-based ACLs with POSIX enforcement, operational audit logging

### Phase 7: System Volume

**Goal:** Cluster-wide replicated storage for operational data

Deliverables:
- `_system` volume: auto-created at cluster init, replication factor equals cluster size
- VolumeRegistry guards: system volume cannot be deleted, renamed, or reconfigured by operators
- `NeonFS.Core.SystemVolume` access API: `read/1`, `write/2`, `append/2`, `list/1`
- Volume struct extension with `system: boolean()` field
- Replication factor auto-adjustment on node join/decommission
- Log retention: background pruning of append-only data (intent archives, audit logs)
- Startup ordering: system volume available before subsystems that depend on it

**Milestone:** `_system` volume created at cluster init, replicated to all nodes, accessible from any node, survives node failures, hidden from user volume listings

### Phase 8: Cluster CA

**Goal:** Self-signed certificate authority for inter-node mTLS

Deliverables:
- Cluster CA generation at cluster init: ECDSA P-256, stored in system volume
- Node certificate issuance via CSR during node join
- Certificate auto-renewal: `NeonFS.Transport.CertRenewal` GenServer (30 days before expiry)
- Certificate revocation: CRL stored in system volume, updated on node decommission
- Serial number allocation in system volume (`/tls/serial`)
- Local filesystem cache of CA cert for fast TLS startup
- CLI commands: `neonfs cluster ca info`, `ca list`, `ca revoke`, `ca rotate`
- Uses `x509` package (pure Elixir, zero deps)

**Milestone:** Every node has a valid certificate signed by the cluster CA, certificates auto-renew, revoked node certs are rejected, CA key stored durably in system volume

### Phase 9: Data Transfer

**Goal:** Out-of-band data plane separating bulk chunk traffic from the Erlang distribution control plane

Deliverables:
- TLS data plane: `:ssl` with `{packet, 4}` framing, mTLS using cluster CA certificates
- Connection pooling via `nimble_pool` (configurable connections per peer)
- Transport listener, handler, and pool manager modules in `neonfs_client`
- ServiceRegistry extension: data transfer endpoint advertisement and discovery
- `Router.data_call/4` for routing chunk operations over the data plane
- Chunk replication and retrieval migrated from distribution to data plane
- Configurable bind address and OS-assigned port with advertisement

**Milestone:** Chunk data separated from control plane, cluster stable under sustained bulk transfer, Ra consensus unaffected by large file writes

### Phase 10: Event Notification

**Goal:** Push-based cache invalidation for interface nodes

Deliverables:
- Event notification system via OTP `:pg` process groups and node-local `Registry`
- Event structs: file content, file attributes (chmod, chown, utimens, xattr), ACL changes, directory, and volume state changes
- Two-layer dispatch: `:pg` relay for cross-node (one message per node), `Registry` for local fan-out
- Subscription model: per-volume with local filtering
- `NeonFS.Events.Broadcaster` on core nodes after successful metadata writes
- `NeonFS.Events.Relay` for `:pg` membership management and local dispatch
- `NeonFS.Client.EventHandler` behaviour in `neonfs_client`
- Partition recovery: full cache invalidation on reconnect with debouncing
- Gap detection via per-source-node sequence counters
- FUSE metadata cache with event-driven invalidation

**Milestone:** Interface nodes receive push-based metadata invalidation, reduced RPC round-trips for cached metadata, correct behaviour across partitions

### Phase 11: APIs and Integration

**Goal:** External access methods

Deliverables:
- S3-compatible HTTP API (Bandit + Plug)
- Docker volume plugin
- CIFS via Samba VFS module
- CSI driver for Kubernetes

**Milestone:** Access from containers, S3 clients, Windows/macOS mounts

### Phase 12: Operations

**Goal:** Production operations support

Deliverables:
- Prometheus metrics exporter
- Decision escalation system (CLI + webhook)
- Capacity planning tools
- DR snapshots to system volume
- Backup/restore procedures
- Comprehensive documentation

**Milestone:** Operable production system with monitoring and alerting

---

## Technical Dependencies

### Elixir

| Dependency | Purpose |
|------------|---------|
| Ra | Raft consensus for metadata |
| Reactor | Saga orchestration for cross-segment operations |
| Rustler | NIF integration with Rust |
| NimblePool | Connection pooling for TLS data plane |
| X509 | Certificate authority operations (pure Elixir, zero deps) |
| Plug + Bandit | HTTP server for S3 API |
| GRPC | CSI driver protocol |
| Phoenix (optional) | Web UI for operations |

### Rust

**neonfs_fuse crate**

| Dependency | Purpose |
|------------|---------|
| fuser | FUSE filesystem implementation |
| tokio | Async runtime, channels to Elixir |
| rustler | NIF bindings |

**neonfs_blob crate**

| Dependency | Purpose |
|------------|---------|
| fastcdc | Content-defined chunking |
| reed-solomon-erasure | Erasure coding |
| ring | Cryptography (AES-GCM, SHA-256) |
| zstd | Compression (primary algorithm) |
| lz4 | Compression (fast alternative) |
| tokio | Async runtime |
| rustler | NIF bindings |

**neonfs-cli crate**

| Dependency | Purpose |
|------------|---------|
| erl_rpc | RPC client for Erlang distribution protocol |
| erl_dist | Erlang distribution protocol implementation |
| clap | CLI argument parsing |
| tokio | Async runtime |
| thiserror | Error handling |

### External Services (Optional)

| Service | Purpose |
|---------|---------|
| Samba | CIFS/SMB protocol support |
| HashiCorp Vault | External secret management (optional) |
| Prometheus | Metrics collection |
| Jaeger/Zipkin | Distributed tracing |

---

## Configuration Reference

### Node Configuration

```yaml
# /etc/neonfs/node.yaml

node:
  name: node1
  data_dir: /var/lib/neonfs

  # Listen addresses
  api_listen: 0.0.0.0:8080        # HTTP API (S3, admin)
  cluster_listen: 0.0.0.0:4369    # BEAM distribution

cluster:
  name: my-cluster
  # Populated during join

storage:
  hash_prefix_depth: 2            # Directory sharding depth

  drives:
    - path: /mnt/nvme0
      tier: hot
      power_management: always_on

    - path: /mnt/hdd0
      tier: cold
      power_management: spin_down
      idle_timeout: 30m

# Application-level cache for transformed data only
# (Linux page cache handles raw chunks automatically)
cache:
  max_memory: 2GB                 # Total memory for all volume caches

telemetry:
  metrics:
    enabled: true
    endpoint: prometheus  # or statsd
  tracing:
    enabled: true
    endpoint: http://jaeger:14268/api/traces
```

### Volume Creation

Volumes are created via CLI or API with inline configuration:

```bash
$ neonfs volume create documents \
    --owner alice \
    --durability replicate:3 \
    --min-copies 2 \
    --write-ack quorum \
    --initial-tier warm \
    --compression zstd:3 \
    --encryption server-side \
    --cache-transformed \
    --repair-priority critical

$ neonfs volume create media \
    --owner alice \
    --durability erasure:10:4 \
    --write-ack local \
    --initial-tier cold \
    --compression none \
    --encryption none \
    --cache-reconstructed \
    --repair-priority low
```

Or via configuration file:

```yaml
# volumes.yaml
volumes:
  - name: documents
    owner: alice
    durability:
      type: replicate
      factor: 3
      min_copies: 2
    write_ack: quorum
    tiering:
      initial_tier: warm
      promotion_threshold: 1
      demotion_delay: 7d
    compression:
      algorithm: zstd
      level: 3
      min_size: 4096
    encryption:
      mode: server_side
    caching:
      transformed_chunks: true
      reconstructed_stripes: false
      remote_chunks: true
      max_memory: 512MB
    repair_priority: critical

  - name: media
    owner: alice
    durability:
      type: erasure
      data_chunks: 10
      parity_chunks: 4
    write_ack: local
    tiering:
      initial_tier: cold
      promotion_threshold: 3
      demotion_delay: 30d
    compression:
      algorithm: none
    encryption:
      mode: none
    caching:
      transformed_chunks: false
      reconstructed_stripes: true
      remote_chunks: true
      max_memory: 2GB
    repair_priority: low
```
