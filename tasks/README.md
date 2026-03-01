# NeonFS Development Tasks

This directory contains individual task specifications for developing NeonFS. Each task is designed to be small enough for an agent to complete within a single context window.

## Task Format

Each task file follows this structure:
- **Status**: Current state of the task (see below)
- **Phase**: Which implementation phase (1-12) this task belongs to
- **Description**: What needs to be built
- **Acceptance Criteria**: Checkboxes for completion verification
- **Testing Strategy**: How to verify the implementation
- **Dependencies**: Which tasks must be completed first
- **Files to Create/Modify**: What code changes are expected
- **Reference**: Links to relevant specification sections
- **Notes**: Additional context or gotchas

## Phase Overview

### Phase 1: Foundation (Tasks 0001-0030, 0040-0044)
**Goal:** Basic single-node operation with local storage and CLI

| Task Range | Component |
|------------|-----------|
| 0001-0009 | neonfs_blob Rust crate (chunking, storage, compression) |
| 0010-0013 | neonfs_fuse Rust crate (FUSE filesystem) |
| 0014-0019 | Elixir core (metadata, read/write paths) |
| 0020-0021 | FUSE Elixir integration |
| 0022-0025 | CLI crate and commands |
| 0026-0029 | Supervision, deployment, releases |
| 0030 | Phase 1 integration test |
| 0040-0044 | Phase 1 addendum (fixes and gaps discovered in audit) |

#### Phase 1 Addendum Tasks (0040-0044)

| Task | Description | Blocks |
|------|-------------|--------|
| 0040 | Metadata persistence with DETS | Restart tests |
| 0041 | Fix FUSE Rust compilation errors | FUSE tests |
| 0042 | Implement CLI RPC server | CLI functionality |
| 0043 | Split systemd units (core + fuse) | Production deployment |
| 0044 | Acceptance testing and Forgejo CI | Phase 2 readiness |

**Milestone:** Mount a directory, read/write files, data persists across restarts, CLI can query status

### Phase 2: Clustering (Tasks 0031-0037)
**Goal:** Multi-node cluster with metadata consensus

| Task | Description |
|------|-------------|
| 0031 | Ra consensus integration |
| 0032 | Cluster bootstrap (init) |
| 0033 | Node join flow |
| 0034 | Distributed chunk metadata |
| 0035 | Basic replication |
| 0036 | Remote chunk reading |
| 0037 | Phase 2 integration test |

**Milestone:** 3-node cluster, data replicated, survives single node failure

### Phase 3: Policies, Tiering, and Compression (Tasks 0045-0056)
**Goal:** Volume policies, tiered storage management, compression

| Task | Description |
|------|-------------|
| 0045 | Volume policy configuration (tiering, caching, io_weight) |
| 0046 | Multi-drive BlobStore (one NIF handle per physical drive) |
| 0047 | Drive registry and tier discovery |
| 0048 | Write path tier and drive handling |
| 0049 | Chunk access tracking (sliding windows) |
| 0050 | Tier-aware read path (score-based replica selection) |
| 0051 | Drive power management (spin-down/spin-up state machine) |
| 0052 | Chunk cache for transformed data (LRU) |
| 0053 | Background work infrastructure (priority queues, rate limiting) |
| 0054 | Tiering manager (promotion/demotion decisions) |
| 0055 | Tier migration with Reactor (saga-based data movement) |
| 0056 | Phase 3 integration tests |

**Milestone:** Multiple volumes with different policies, compression working, intelligent drive management

### Phase 4: Erasure Coding (Tasks 0057-0066)
**Goal:** Space-efficient durability for large files

| Task | Description |
|------|-------------|
| 0057 | Reed-Solomon NIF (Rust encode/decode via solana-reed-solomon-erasure) |
| 0058 | Stripe struct and Volume durability extension |
| 0059 | MetadataStateMachine v4 and StripeIndex |
| 0060 | Erasure-coded write path (stripe batching, parity generation) |
| 0061 | Erasure-coded read path (healthy, degraded, multi-stripe) |
| 0062 | Stripe chunk placement (failure domain distribution) |
| 0063 | Stripe-aware garbage collection |
| 0064 | Stripe repair (background reconstruction) |
| 0065 | CLI and volume creation for erasure coding |
| 0066 | Phase 4 integration tests and full verification |

**Milestone:** Volumes can use erasure coding, ~1.4x overhead with 4-fault tolerance

### Phase 5: Metadata Tiering (Tasks 0080-0091)
**Goal:** Scalable metadata architecture — move large metadata out of Ra into leaderless quorum-replicated BlobStore

| Task | Description |
|------|-------------|
| 0080 | Hybrid Logical Clocks (HLC for conflict resolution) |
| 0081 | Consistent hashing ring (key → segment → replica set) |
| 0082 | MetadataStateMachine v5 (segment assignments + intents, remove chunk/file/stripe maps) |
| 0083 | BlobStore metadata namespace (Rust NIFs + MetadataStore Elixir module) |
| 0084 | Quorum coordinator (leaderless R+W>N reads/writes) |
| 0085 | Intent log (cross-segment atomicity, concurrent writer detection) |
| 0086 | ChunkIndex migration to quorum store |
| 0087 | FileIndex and directory entry migration |
| 0088 | StripeIndex migration to quorum store |
| 0089 | Read repair and write/read path adaptation |
| 0090 | Anti-entropy (periodic Merkle tree sync) |
| 0091 | Phase 5 integration tests and full verification |

**Milestone:** Metadata scales independently of cluster size, quorum-replicated with tunable consistency, directory entries for efficient listings

### Phase 6: Security (Tasks 0067-0079)
**Goal:** Production-ready security model

| Task | Description |
|------|-------------|
| 0067 | Encryption NIFs (AES-256-GCM in Rust) |
| 0068 | Encryption types and Volume config extension |
| 0069 | ACL types (UID/GID-based) |
| 0070 | MetadataStateMachine v6 (encryption keys, volume ACLs) |
| 0071 | Per-volume key management (KeyManager) |
| 0072 | Encrypted write path |
| 0073 | Encrypted read path and key version lookup |
| 0074 | Volume key rotation worker |
| 0075 | Volume ACLs and authorisation |
| 0076 | File/directory ACLs and POSIX enforcement |
| 0077 | Audit logging |
| 0078 | CLI security commands |
| 0079 | Phase 6 integration tests and full verification |

**Milestone:** Encrypted volumes with key rotation, UID/GID-based volume and file ACLs with POSIX enforcement, operational audit logging

### Phase 7: System Volume (Tasks 0092-0097)
**Goal:** Cluster-wide replicated storage for operational data

The `_system` volume stores cluster-wide operational data (CA keys, audit logs, DR snapshots). It is auto-created at cluster init with replication factor equal to cluster size.

| Task | Description |
|------|-------------|
| 0092 | Volume struct `system` field and VolumeRegistry guards |
| 0093 | SystemVolume access API (read, write, append, list, delete, exists?) |
| 0094 | Cluster init creates system volume and identity file |
| 0095 | Node join/decommission replication adjustment |
| 0096 | System volume log retention (background pruning) |
| 0097 | Phase 7 integration tests |

**Milestone:** System volume auto-created at cluster init, replicated to all nodes, protected from deletion/rename, with retention-managed audit log directories

See [spec/system-volume.md](../spec/system-volume.md).

### Phase 8: Cluster CA (Tasks 0098-0104)
**Goal:** Self-signed certificate authority for inter-node mTLS

ECDSA P-256 CA stored in the system volume. Nodes receive certificates via CSR during join. Auto-renewal, CRL revocation, CLI commands.

| Task | Description |
|------|-------------|
| 0098 | x509 dependency and NeonFS.Transport.TLS core module |
| 0099 | CertificateAuthority module and cluster init CA generation |
| 0100 | Node join certificate issuance |
| 0101 | Certificate auto-renewal (CertRenewal GenServer) |
| 0102 | Certificate revocation (CRL management) |
| 0103 | CLI CA commands (Elixir handler + Rust CLI) |
| 0104 | Phase 8 integration tests |

**Milestone:** Every node has a valid certificate signed by the cluster CA, certificates auto-renew, revoked node certs are tracked in CRL, CA key stored durably in system volume

See [spec/cluster-ca.md](../spec/cluster-ca.md).

### Phase 9: Data Transfer (Tasks 0105-0111)
**Goal:** Out-of-band data plane separating bulk chunk traffic from Erlang distribution

TLS data plane using `:ssl` with `{packet, 4}` framing and `nimble_pool` connection pooling. Chunk replication and retrieval migrated from distribution to the data plane.

| Task | Description |
|------|-------------|
| 0105 | nimble_pool dependency and Transport.ConnPool (NimblePool behaviour) |
| 0106 | Transport.Listener and Transport.Handler (inbound connection handling) |
| 0107 | Transport.PoolManager and endpoint advertisement (per-peer pool lifecycle) |
| 0108 | Router.data_call/4 (data plane routing + ServiceRegistry integration) |
| 0109 | Write path data plane migration (replication + stripe distribution) |
| 0110 | Read path data plane migration (remote chunk retrieval) |
| 0111 | Phase 9 integration tests |

**Milestone:** Chunk data separated from control plane, cluster stable under sustained bulk transfer, Ra consensus unaffected by large file writes

See [spec/data-transfer.md](../spec/data-transfer.md).

### Phase 10: Event Notification (Tasks 0112-0118)
**Goal:** Push-based cache invalidation for interface nodes

Two-layer dispatch (`:pg` cross-node relay + `Registry` local fan-out). Struct-based events for file content, attributes, ACLs, directories, and volumes. Partition recovery with debounced invalidation. No new dependencies — uses OTP built-in `:pg` and `Registry`.

| Task | Description |
|------|-------------|
| 0112 | Event structs and Envelope (14 event types + wrapper) |
| 0113 | Subscription API, Relay, and Registry (subscribe/unsubscribe + two-layer dispatch) |
| 0114 | Broadcaster and sequence counters (event emission + per-volume atomics counters) |
| 0115 | Core metadata event emission (instrument FileIndex, VolumeRegistry, ACLManager) |
| 0116 | Partition recovery (cache invalidation on reconnect with debouncing) |
| 0117 | FUSE metadata cache with event-driven invalidation |
| 0118 | Phase 10 integration tests |

**Milestone:** Interface nodes receive push-based metadata invalidation, reduced RPC round-trips for cached metadata, correct behaviour across partitions

See [spec/pubsub.md](../spec/pubsub.md).

### Gap Analysis: Quick Wins (Tasks 0119-0125)
**Goal:** Address low-effort gaps identified by spec-vs-implementation analysis

All tasks are independent unless noted. See [spec/gap-analysis.md](../spec/gap-analysis.md) for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0119 | FUSE `create`/`mkdir` honour mode parameter | None |
| 0120 | Add `changed_at` field to `FileMeta` for POSIX `ctime` | None |
| 0121 | Make `ChunkCache` memory limit configurable | None |
| 0122 | `ChunkFetcher` consults volume caching flags | 0121 (soft) |
| 0123 | Add `scrub_interval` to volume `verification_config` | None |
| 0124 | Add volume `atime_mode` setting and update on read | 0120 |
| 0125 | Truncate removes chunk references beyond new size | 0120 |

**Milestone:** POSIX timestamp and permission semantics correct, cache behaviour respects volume config, truncation works end-to-end

### Gap Analysis: M-1 — CLI Commands for GC and Scrubbing (Tasks 0126-0131)
**Goal:** Wire existing GarbageCollector into the CLI and add integrity scrubbing

Two parallel chains: GC (0126→0127→0128) and Scrub (0129→0130→0131). The chains are independent of each other. See [spec/gap-analysis.md](../spec/gap-analysis.md) M-1 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0126 | GC Job Runner — extend `GarbageCollector` + `Job.Runners.GarbageCollection` | None |
| 0127 | GC CLI handler — `handle_gc_collect/1`, `handle_gc_status/0` in handler.ex | 0126 |
| 0128 | GC Rust CLI — `gc collect`, `gc status` commands in neonfs-cli | 0127 |
| 0129 | Scrub Job Runner — `Job.Runners.Scrub` (chunk hash verification) | None |
| 0130 | Scrub CLI handler — `handle_scrub_start/1`, `handle_scrub_status/0` in handler.ex | 0129 |
| 0131 | Scrub Rust CLI — `scrub start`, `scrub status` commands in neonfs-cli | 0130 |

**Milestone:** `neonfs gc collect`, `neonfs gc status`, `neonfs scrub start`, and `neonfs scrub status` all functional end-to-end

### Gap Analysis: M-2 — Scheduled GC Execution (Tasks 0132-0134)
**Goal:** Automatic garbage collection on a schedule and under storage pressure

Sequential chain: 0132→0133→0134. See [spec/gap-analysis.md](../spec/gap-analysis.md) M-2 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0132 | GC Scheduler GenServer — periodic job creation via `JobTracker` | 0126 |
| 0133 | GC Storage Pressure Trigger — trigger GC when cluster usage exceeds threshold | 0132 |
| 0134 | GC Scheduler Supervision and Configuration — wire into supervisor, load from `cluster.json` | 0132, 0133 |

**Milestone:** GC runs automatically on a configurable interval and triggers when storage exceeds 85% capacity

### Gap Analysis: M-3 — Background Scrubbing Job (Tasks 0135-0137)
**Goal:** Periodic integrity verification of stored chunks

Sequential chain: 0135→0136, with 0137 independent after 0135. See [spec/gap-analysis.md](../spec/gap-analysis.md) M-3 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0135 | ScrubScheduler GenServer — periodic scrub job creation per volume | 0129 |
| 0136 | ScrubScheduler Supervision and Configuration — wire into supervisor, load from `cluster.json` | 0135 |
| 0137 | Encrypted Chunk Scrub Support — decrypt-then-verify via KeyManager | 0135 |

**Milestone:** Scrubbing runs automatically per volume's `scrub_interval`, encrypted chunks are verified, scheduler is supervised and configurable

### Gap Analysis: M-4 — Volume Configuration CLI (Tasks 0138-0139)
**Goal:** Expose all volume configuration fields via `volume update` CLI command

Sequential chain: 0138→0139. See [spec/gap-analysis.md](../spec/gap-analysis.md) M-4 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0138 | Volume update CLI handler — `handle_volume_update/2` in handler.ex | None |
| 0139 | Volume update Rust CLI — `volume update` command with flags for all updatable fields | 0138 |

**Milestone:** `neonfs volume update <name> --compression lz4 --write-ack quorum ...` functional end-to-end

### Gap Analysis: M-5 — Coldness Scoring Formula (Task 0140)
**Goal:** Replace simplistic daily-count sort with blended coldness score for eviction

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0140 | Coldness scoring formula — recency + frequency blended score in TieringManager | None |

**Milestone:** Eviction selects chunks based on access recency and frequency, not just daily count

### Gap Analysis: M-6 — Access Statistics Persistence (Task 0141)
**Goal:** Persist chunk access statistics across restarts

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0141 | ChunkAccessTracker DETS persistence — snapshot to DETS, restore on startup, table size management | None |

**Milestone:** Access statistics survive process and node restarts, table size is bounded

### Gap Analysis: M-7 — Structured JSON Logging (Tasks 0142-0144)
**Goal:** JSON log output with structured metadata fields

Sequential chain: 0142→0143→0144. See [spec/gap-analysis.md](../spec/gap-analysis.md) M-7 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0142 | JSON log formatter setup — `logger_json` dep, prod config, helper module | None |
| 0143 | Logger metadata at entry points — set component, volume_id, request_id at request boundaries | 0142 |
| 0144 | Convert Logger calls to structured format — replace string interpolation with metadata keyword lists | 0142, 0143 |

**Milestone:** Production logs are structured JSON with component, volume, and request context in every message

### Gap Analysis: M-8 — Property Tests (Tasks 0145-0148)
**Goal:** Property-based testing for core invariants

0145 is the foundation; 0146, 0147, 0148 are independent after 0145. See [spec/gap-analysis.md](../spec/gap-analysis.md) M-8 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0145 | stream_data dependencies and test generators — shared generators for common NeonFS types | None |
| 0146 | HLC and quorum coordinator property tests — monotonicity, antisymmetry, read-after-write | 0145 |
| 0147 | Compression and hashing property tests — round-trip, determinism, Rust proptest | 0145 |
| 0148 | Consistent hashing and path operation property tests — determinism, minimal disruption, path invariants | 0145 |

**Milestone:** Core invariants verified by property-based tests: HLC ordering, quorum consistency, compression round-trips, hash ring stability

### Gap Analysis: M-9 — FUSE setattr Integration Tests (Task 0149)
**Goal:** Integration test coverage for chmod, chown, utimens, truncate

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0149 | FUSE setattr integration tests — chmod, chown, truncate, utimens, permission enforcement | None |

**Milestone:** All setattr sub-operations have integration test coverage including permission edge cases

### Gap Analysis: M-10 — Network Partition Integration Tests (Tasks 0150-0151)
**Goal:** Test cluster behaviour under network splits

Sequential chain: 0150→0151. See [spec/gap-analysis.md](../spec/gap-analysis.md) M-10 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0150 | PeerCluster partition helpers — disconnect/reconnect/partition/heal functions | None |
| 0151 | Network partition integration tests — majority writes, minority read-only, partition healing | 0150 |

**Milestone:** Verified: majority partition continues writing, minority fails writes, data converges after healing

### Gap Analysis: M-11 — Worker Configuration CLI (Tasks 0152-0154)
**Goal:** Runtime BackgroundWorker configuration via CLI

Sequential chain: 0152→0153→0154. See [spec/gap-analysis.md](../spec/gap-analysis.md) M-11 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0152 | BackgroundWorker hot reconfiguration — `reconfigure/1` and `status/0` APIs | None |
| 0153 | Worker CLI handler — `handle_worker_status/0`, `handle_worker_configure/1` + cluster.json persistence | 0152 |
| 0154 | Worker Rust CLI — `worker status`, `worker configure` commands | 0153 |

**Milestone:** `neonfs worker status` and `neonfs worker configure --max-concurrent 8` functional end-to-end

### Gap Analysis: M-12 — cluster.json Validation and Missing Settings (Tasks 0155-0156)
**Goal:** Schema validation at startup and missing cluster-level settings

Sequential chain: 0155→0156. See [spec/gap-analysis.md](../spec/gap-analysis.md) M-12 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0155 | Cluster.State validator module — field-level validation with specific error messages | None |
| 0156 | Missing cluster settings and startup wiring — peer_sync_interval, peer_connect_timeout, etc. | 0155 |

**Milestone:** Malformed cluster.json produces actionable error messages, missing cluster-level settings have sensible defaults

### Gap Analysis: M-13 — Drive and Space Behaviour Tests (Tasks 0157-0158)
**Goal:** Integration tests using loopback devices for real space behaviour

Sequential chain: 0157→0158. See [spec/gap-analysis.md](../spec/gap-analysis.md) M-13 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0157 | Loopback device test helpers — create/mount/teardown small real filesystems | None |
| 0158 | Drive space integration tests — ENOSPC handling, capacity thresholds, drive evacuation | 0157, 0132 |

**Milestone:** Real I/O path tested with loopback devices: ENOSPC handling, GC pressure triggering, drive evacuation

### Gap Analysis: M-14 — Read Verification Wired into Read Path (Task 0159)
**Goal:** Verification failures trigger repair instead of just failing the read

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0159 | Read verification failure handling and repair — telemetry, remote retry, background repair | None |

**Milestone:** Corrupt chunks detected on read are transparently recovered from remote replicas and repaired in background

### Gap Analysis: M-15 — Cache Memory Limit Per-Node (Task 0160)
**Goal:** Single node-level cache budget with global LRU eviction

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0160 | Node-level cache budget — global LRU eviction, remove per-volume max_memory, enforce caching policy flags | None |

**Milestone:** Cache enforces a single node-level memory limit with LRU across all volumes, caching policy flags are honoured

### Gap Analysis: M-16 — Structured Errors Using splode (Tasks 0161-0164)
**Goal:** Consistent, classifiable error types across all packages

0161 is the foundation; 0162→0163 sequential; 0164 depends on 0162. See [spec/gap-analysis.md](../spec/gap-analysis.md) M-16 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0161 | NeonFS.Error struct and error catalogue — splode-based error hierarchy with from_legacy/1 | None |
| 0162 | Migrate CLI handler to structured errors — convert all handler error returns | 0161 |
| 0163 | Migrate core operations to structured errors — ReadOperation, WriteOperation, VolumeRegistry, Router | 0161, 0162 |
| 0164 | Rust CLI error struct parsing — parse NeonFS.Error structs, class-based exit codes | 0162 |

**Milestone:** Errors are structured types with classes, context, and human-readable messages from origin through CLI

### Gap Analysis: H-1 — GenStage I/O Scheduler (Tasks 0165-0172)
**Goal:** Replace/complement `BackgroundWorker` with a GenStage producer/consumer I/O scheduling architecture

0165 is the foundation (types + API); 0166 (producer) and 0167 (strategies) branch from 0165; 0168 (drive worker) merges 0166 + 0167; 0169 (priority adjustment) branches from 0166; 0170 (supervision) merges 0168 + 0169; 0171 → 0172 sequential. See [spec/gap-analysis.md](../spec/gap-analysis.md) H-1 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0165 | I/O operation types and scheduler API facade — `gen_stage` dep, Operation struct, Priority module | None |
| 0166 | I/O producer with weighted fair queuing — GenStage producer, per-volume WFQ dispatch | 0165 |
| 0167 | Drive strategy behaviour and implementations — HDD elevator, SSD parallel FIFO, drive type detection | 0165 |
| 0168 | Drive worker GenStage consumer — per-drive consumer, strategy-based dispatch, telemetry | 0166, 0167 |
| 0169 | Dynamic priority adjustment — storage pressure response, per-volume repair promotion | 0166 |
| 0170 | I/O scheduler supervision tree — supervisor, DynamicSupervisor for drive workers, DriveRegistry integration | 0168, 0169 |
| 0171 | Migrate existing callers to I/O scheduler — WriteOperation, ReadOperation, ChunkReplicator, etc. | 0170 |
| 0172 | I/O scheduler integration tests — end-to-end WFQ, drive strategies, priority adjustment, backpressure | 0171 |

**Milestone:** All disk I/O routed through GenStage pipeline with per-drive workers, WFQ fairness, and dynamic priority adjustment under storage pressure

### Gap Analysis: H-2 — Prometheus Metrics and Health HTTP Endpoints (Tasks 0173-0178)
**Goal:** Wire 41 existing telemetry sources to Prometheus exposition, add health aggregation, serve via HTTP

0173 (metric defs) and 0175 (health check) are independent starting points; 0174 follows 0173; 0176 merges 0173 + 0175; 0177 merges 0174 + 0176; 0178 follows 0177. See [spec/gap-analysis.md](../spec/gap-analysis.md) H-2 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0173 | Prometheus dependencies and metric definitions — `telemetry_metrics_prometheus_core` dep, central `Telemetry` module | None |
| 0174 | Telemetry poller for gauge emissions — periodic sampling of storage, cache, cluster, worker state | 0173 |
| 0175 | Health check aggregation module — per-subsystem health queries, overall status aggregation | None |
| 0176 | HTTP metrics endpoint (Bandit + Plug) — `/metrics` (Prometheus) and `/health` (JSON) on port 9568 | 0173, 0175 |
| 0177 | Metrics supervision and configuration — supervisor tree, enable/disable toggle, `cluster.json` integration | 0174, 0176 |
| 0178 | Alerting rules and FUSE metrics — Prometheus alerting rules file, FUSE node metrics endpoint on port 9569 | 0177 |

**Milestone:** `/metrics` serves Prometheus text format, `/health` serves JSON health report, alerting rules defined, FUSE node has its own metrics endpoint

### Gap Analysis: H-3 — Cargo-Fuzz Targets (Tasks 0179-0183)
**Goal:** Set up `cargo-fuzz` for both Rust crates with 3 fuzz targets

0179 (blob scaffolding) gates 0180 and 0181; 0182 (CLI) is independent; 0183 merges all three. See [spec/gap-analysis.md](../spec/gap-analysis.md) H-3 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0179 | neonfs_blob cargo-fuzz scaffolding — `libfuzzer-sys`, fuzz directory structure, placeholder target | None |
| 0180 | Chunk parsing fuzz target — chunk header parsing, hash verification, path derivation | 0179 |
| 0181 | Compression and encryption fuzz target — LZ4/Zstd round-trips, AES-256-GCM round-trips | 0179 |
| 0182 | CLI term parsing fuzz target — `eetf` term decoding, response extraction, error struct parsing | None |
| 0183 | CI integration for periodic fuzzing — per-PR 5-min runs (non-blocking), nightly 30-min runs | 0180, 0181, 0182 |

**Milestone:** Three fuzz targets running in CI, crashes automatically reported, seed corpora maintained

### Gap Analysis: H-4 — Container-Based Chaos Testing (Tasks 0184-0190)
**Goal:** Container-based test orchestration for true network partition and failure injection

0184 (runtime API) → 0185 (TestCluster) → 0186 (init helpers) → 0188 (ChaosCase) → 0189 (partition tests); 0187 (latency) branches from 0185 and merges with 0188 at 0190. See [spec/gap-analysis.md](../spec/gap-analysis.md) H-4 for full context.

| Task | Description | Dependencies |
|------|-------------|--------------|
| 0184 | Container runtime detection and basic API — Docker/Podman auto-detection, lifecycle operations | None |
| 0185 | TestCluster module: container lifecycle — Docker network, multi-container cluster management | 0184 |
| 0186 | TestCluster cluster initialisation and helpers — cluster init/join, CLI helpers, RPC support | 0185 |
| 0187 | Latency injection via tc/netem — per-peer and all-to-all latency, bidirectional, `NET_ADMIN` capability | 0185 |
| 0188 | ChaosCase ExUnit template — setup/teardown, `@tag :chaos`, `assert_eventually`, excluded by default | 0186 |
| 0189 | Container partition and failure tests — minority/majority partition, node kill, rolling restart, data integrity | 0188 |
| 0190 | Container latency and cascading failure tests — latency tolerance, cascading faults, degraded network + node kill | 0187, 0188 |

**Milestone:** Container-based chaos tests exercising real network partitions, latency injection, and cascading failures with verified data integrity

### Phase 11: S3-Compatible API (Future)
**Goal:** S3-compatible HTTP access to NeonFS volumes (Bandit + Plug, S3 Signature v4)

### Phase 12: Docker/Podman Volume Plugin (Future)
**Goal:** Container volume integration for Docker and Podman

### Phase 13: CIFS/SMB (Future)
**Goal:** Windows and macOS network share access via Samba VFS module

### Phase 14: CSI Driver (Future)
**Goal:** Kubernetes-native persistent volume support via gRPC CSI

### Phase 15: Operations (Future)
**Goal:** Production operations support (DR snapshots, monitoring, capacity planning)

### Phase 16: Documentation (Future)
**Goal:** Comprehensive documentation for operators, users, and developers

### Deferred: TLS Distribution (Future)
**Goal:** Mutual TLS for Erlang distribution (node-to-node control plane encryption). The cluster CA (Phase 8) provides the certificate infrastructure; the data plane (Phase 9) uses dedicated TLS connections for bulk transfer. Distribution-level TLS for the control plane remains deferred — relies on WireGuard/VPN for transport security in the interim.

## Task Dependencies Graph

```
Phase 1 Foundation:

0001 (blob scaffolding)
  └─▶ 0002 (check.exs) ─┬─▶ 0003 (hash) ─▶ 0004 (paths) ─▶ 0005 (store) ─▶ 0006 (verify)
                        │                        │                              │
                        │                        └─▶ 0008 (chunking)            ▼
                        │                                                  0007 (compress)
                        │                                                       │
                        └──────────────────────────────────────────────────────▶│
                                                                                ▼
                                                                           0009 (migrate)

0010 (fuse scaffolding) ─▶ 0011 (channels) ─▶ 0012 (mount ops) ─▶ 0013 (write ops)

0014 (blob wrapper)  ─┐
0015 (chunk meta)    ─┼─▶ 0018 (write path) ─┬─▶ 0020 (fuse handler) ─▶ 0021 (mount mgr)
0016 (file meta)     ─┤                      │
0017 (volume config) ─┘                      └─▶ 0019 (read path)

0022 (cli scaffolding) ─▶ 0023 (daemon conn) ─┬─▶ 0025 (cli commands)
                                              │
0024 (cli handler) ────────────────────────────┘

0026 (core supervision) ─┬─▶ 0028 (systemd)
0027 (fuse supervision) ─┘        │
                                  ▼
                             0029 (release)
                                  │
                                  ▼
                             0030 (integration)

Phase 1 Addendum (must complete before Phase 2):

0040 (persistence) ────────────────────────────────────────────┐
                                                               │
0041 (fuse rust fixes) ────────────────────────────────────────┤
                                                               │
0042 (cli rpc server) ─────────────────────────────────────────┼──▶ 0044 (acceptance/CI)
                                                               │
0043 (systemd split) ──────────────────────────────────────────┘

Phase 2 Clustering:

0031 (Ra setup) ─▶ 0032 (bootstrap) ─▶ 0033 (join) ─▶ 0034 (distributed meta)
                                                              │
                                                              ▼
                                                         0035 (replication)
                                                              │
                                                              ▼
                                                         0036 (remote read)
                                                              │
                                                              ▼
                                                         0037 (integration)

Phase 3 Policies, Tiering, and Compression:

Parallel starting points (4 independent streams):

Stream A — Multi-drive + drive infrastructure:
0046 (multi-drive BS) ─▶ 0047 (drive registry) ─┬─▶ 0050 (tier-aware reads) ─▶ 0051 (power mgmt)
                                                 ├─▶ 0048 (write path) ◀── 0045
                                                 └─▶ 0054 (tiering mgr) ◀── 0049, 0053

Stream B — Volume config + access tracking:
0045 (volume config) ─┬─▶ 0049 (access tracking)
                      └─▶ 0052 (chunk cache)

Stream C — Background infrastructure:
0053 (background worker)

Convergence:
0048 (write path)      ◀── 0045 + 0047
0054 (tiering mgr)     ◀── 0047 + 0049 + 0053
0055 (tier migration)  ◀── 0046 + 0047 + 0053 + 0054
0056 (integration)     ◀── all Phase 3 tasks (0045–0055)

Full dependency graph:

0045 (volume config) ──┬──▶ 0048 (write path) ─────────────────────────┐
                       ├──▶ 0049 (access tracking) ──▶ 0054 (tiering) ─┤
                       └──▶ 0052 (chunk cache)                         │
                                                                       │
0046 (multi-drive BS) ─▶ 0047 (drive registry) ──┬──▶ 0048            │
                                                  ├──▶ 0050 (read path)│
                                                  ├──▶ 0051 (power)    │
                                                  └──▶ 0054 ───────────┤
                                                                       │
0053 (background worker) ──▶ 0054 ──▶ 0055 (reactor migration) ───────┤
                                                                       │
0056 (integration tests) ◀────────────────────────────────────────────┘

Phase 4 Erasure Coding:

Parallel starting points (2 independent streams):

Stream A — Rust NIF:
0057 (Reed-Solomon NIF)

Stream B — Data structures:
0058 (Stripe struct + Volume durability) ──▶ 0059 (MSM v4 + StripeIndex)
                                           └──▶ 0065 (CLI erasure support)

Convergence:
0060 (write path)      ◀── 0057 + 0058 + 0059
0061 (read path)       ◀── 0057 + 0059 + 0060
0062 (placement)       ◀── 0060
0063 (GC)              ◀── 0059 + 0060
0064 (stripe repair)   ◀── 0057 + 0059 + 0061
0066 (integration)     ◀── all Phase 4 tasks (0057–0065)

Full dependency graph:

0057 (RS NIF) ──────────┬──▶ 0060 (write path) ──┬──▶ 0061 (read path) ──▶ 0064 (repair)
                         │                         ├──▶ 0062 (placement)
0058 (Stripe+Volume) ──┬─┤                         └──▶ 0063 (GC)
                        │ └──▶ 0059 (MSM v4) ─────┘
                        └──▶ 0065 (CLI)

0066 (integration tests) ◀── all tasks 0057–0065

Phase 5 Metadata Tiering:

Parallel starting points (2 independent streams):

Stream A — HLC:
0080 (Hybrid Logical Clocks)

Stream B — Hashing ring:
0081 (Consistent hashing ring)

Convergence:
0082 (MSM v5)               ◀── 0081
0083 (BlobStore metadata)    ◀── 0080 + 0082
0084 (Quorum coordinator)    ◀── 0080 + 0081 + 0083
0085 (Intent log)            ◀── 0082

Migration (parallel after 0084):
0086 (ChunkIndex)            ◀── 0084
0087 (FileIndex + DirEntry)  ◀── 0084 + 0085
0088 (StripeIndex)           ◀── 0084

Adaptation:
0089 (ReadRepair + paths)    ◀── 0086 + 0087 + 0088
0090 (Anti-entropy)          ◀── 0084
0091 (integration)           ◀── all Phase 5 tasks (0080–0090)

Full dependency graph:

0080 (HLC) ─────────────────────────┐
                                     │
0081 (Ring) ──▶ 0082 (MSM v5) ──────┼──▶ 0083 (BlobStore meta) ──▶ 0084 (Quorum)
                     │               │                                    │
                     └──▶ 0085 (IntentLog) ──────────────────────────────┤
                                                                         │
                     0086 (ChunkIndex) ◀─────────────────────────────────┤
                     0087 (FileIndex+Dir) ◀── 0084 + 0085 ──────────────┤
                     0088 (StripeIndex) ◀────────────────────────────────┘
                          │
                     0089 (ReadRepair+Paths) ◀── 0086 + 0087 + 0088
                     0090 (Anti-Entropy) ◀── 0084
                          │
                     0091 (Integration) ◀── all

Phase 6 Security:

Parallel starting points (3 independent streams):

Stream A — Encryption NIF:
0067 (AES-256-GCM NIFs)

Stream B — Encryption types:
0068 (encryption types + Volume config)

Stream C — ACL types:
0069 (UID/GID-based ACL types)

Convergence:
0070 (MSM v6)          ◀── 0068 + 0069
0071 (key management)  ◀── 0070
0072 (encrypted write) ◀── 0067 + 0071
0073 (encrypted read)  ◀── 0072
0074 (key rotation)    ◀── 0073

0075 (volume ACLs)     ◀── 0069 + 0070
0076 (file ACLs)       ◀── 0075

0077 (audit logging)   ◀── 0070

0078 (CLI commands)    ◀── 0071 + 0074 + 0075 + 0077
0079 (integration)     ◀── all Phase 6 tasks (0067–0078)

Full dependency graph:

0067 (encrypt NIF) ────────────────────────┐
                                            │
0068 (encrypt types) ──┬──▶ 0070 (MSM v6) ─┼──▶ 0071 (key mgmt) ──▶ 0072 (write) ──▶ 0073 (read) ──▶ 0074 (rotation)
                        │                   │                                                              │
0069 (ACL types) ──────┘                   ├──▶ 0075 (vol ACLs) ──▶ 0076 (file ACLs)                     │
                                            │                              │                               │
                                            └──▶ 0077 (audit) ────────────┤                               │
                                                                           │                               │
0078 (CLI commands) ◀──────────────────────────────────────────────────────┘───────────────────────────────┘

0079 (integration tests) ◀── all tasks 0067–0078

Phase 7 System Volume:

Single dependency chain (strictly sequential):

0092 (Volume struct + guards) ──▶ 0093 (SystemVolume API) ──▶ 0094 (cluster init)
                               └──▶ 0095 (join replication)
                               └──▶ 0096 (log retention) ◀── 0093

0097 (integration tests) ◀── all Phase 7 tasks (0092–0096)

Full dependency graph:

0092 (Volume struct) ──┬──▶ 0093 (SystemVolume API) ──┬──▶ 0094 (cluster init)
                       │                               └──▶ 0096 (log retention)
                       └──▶ 0095 (join replication)

0097 (integration tests) ◀── all tasks 0092–0096

Phase 8 Cluster CA:

Single starting point (0098), then 3 parallel streams:

Stream A — Init + Join:
0098 (TLS module) ──▶ 0099 (CA + init) ──▶ 0100 (join certs)

Stream B — Renewal:
0098 (TLS module) ──▶ 0101 (CertRenewal)

Stream C — Revocation:
0098 (TLS module) ──▶ 0102 (CRL revocation)

Convergence:
0103 (CLI commands) ◀── 0099 + 0100 + 0102
0104 (integration)  ◀── all Phase 8 tasks (0098–0103)

Full dependency graph:

0098 (TLS module) ──┬──▶ 0099 (CA + init) ──▶ 0100 (join certs) ──┐
                    ├──▶ 0101 (CertRenewal)                        │
                    └──▶ 0102 (CRL revocation) ────────────────────┤
                                                                    │
0103 (CLI commands) ◀── 0099 + 0100 + 0102 ───────────────────────┤
                                                                    │
0104 (integration tests) ◀────────────────────────────────────────┘

Phase 9 Data Transfer:

Parallel starting points (2 independent streams):

Stream A — Outbound connection pool:
0105 (ConnPool)

Stream B — Inbound connection handling:
0106 (Listener + Handler)

Convergence:
0107 (PoolManager)         ◀── 0105
0108 (Router.data_call)    ◀── 0107

Migration (parallel after 0108):
0109 (write path)          ◀── 0108
0110 (read path)           ◀── 0108

0111 (integration tests)   ◀── all Phase 9 tasks (0105–0110)

Full dependency graph:

0105 (ConnPool) ──▶ 0107 (PoolManager) ──▶ 0108 (data_call) ──┬──▶ 0109 (write path)
                                                                │
0106 (Listener + Handler)                                       └──▶ 0110 (read path)

0111 (integration tests) ◀── all tasks 0105–0110

Phase 9 → 10:

Phase 10 (Event Notification) — no hard dependency on 7-9, sequenced after for practical reasons

Phase 10 Event Notification:

Parallel starting points (2 independent streams after 0112):

Stream A — Subscription (receiving side):
0112 (event structs) ──▶ 0113 (subscription + Relay)

Stream B — Broadcasting (sending side):
0112 (event structs) ──▶ 0114 (Broadcaster)

Convergence:
0115 (event emission)    ◀── 0114
0116 (partition recovery) ◀── 0113
0117 (FUSE cache)        ◀── 0113 + 0115

0118 (integration tests) ◀── all Phase 10 tasks (0112–0117)

Full dependency graph:

0112 (event structs) ──┬──▶ 0113 (subscription + Relay) ──┬──▶ 0116 (partition recovery)
                       │                                    │
                       └──▶ 0114 (Broadcaster) ──▶ 0115 (event emission)
                                                           │
                                         0117 (FUSE cache) ◀── 0113 + 0115
                                                           │
                                         0118 (integration) ◀── all tasks 0112–0117

Gap Analysis H-1: GenStage I/O Scheduler (all four H chains are completely independent):

0165 (types + API)
  ├──▶ 0166 (producer + WFQ) ──▶ 0168 (drive worker) ──▶ 0170 (supervision) ──▶ 0171 (migrate callers) ──▶ 0172 (integration)
  │                                     ▲
  ├──▶ 0167 (drive strategies) ────────┘
  └──▶        0169 (priority adj) ──▶ 0170

Gap Analysis H-2: Prometheus Metrics:

0173 (metric defs) ──┬──▶ 0174 (poller) ──▶ 0177 (supervision) ──▶ 0178 (alerting + FUSE)
                     └──▶ 0176 (HTTP endpoint) ──▶ 0177
0175 (health check) ──▶ 0176

Gap Analysis H-3: Cargo-Fuzz:

0179 (blob scaffolding) ──▶ 0180 (chunk parsing) ──┐
                         └──▶ 0181 (compression) ──┼──▶ 0183 (CI integration)
0182 (cli term parsing) ───────────────────────────┘

Gap Analysis H-4: Container Chaos:

0184 (runtime API) ──▶ 0185 (TestCluster) ──┬──▶ 0186 (init helpers) ──▶ 0188 (ChaosCase) ──┬──▶ 0189 (partition tests)
                                             │                                                │
                                             └──▶ 0187 (latency) ──────────────────────────────┴──▶ 0190 (latency tests)
```

## Task Status Values

| Status | Meaning |
|--------|---------|
| Not Started | Task has not been picked up |
| In Progress | Work is actively being done |
| Blocked | Waiting on external factor or decision |
| Complete | All acceptance criteria met, tests pass |

When updating a task's status, edit the `## Status` line in the task file.

## Working on Tasks

1. **Check dependencies**: Ensure prerequisite tasks are complete
2. **Read the spec**: Review referenced specification documents
3. **Implement**: Write code following acceptance criteria
4. **Test**: Run the specified tests
5. **Verify**: Check all acceptance criteria boxes

## Parallel Work Streams

Some task chains can be worked on in parallel:

- **Blob store** (0001-0009) and **FUSE** (0010-0013) crates are independent
- **Elixir metadata** (0015-0017) can start once blob store scaffolding is done
- **CLI** (0022-0025) can be developed alongside core Elixir work
- **Phase 3** has 4 independent starting points: 0045 (volume config), 0046 (multi-drive), 0053 (background worker) can all start in parallel; 0047 follows 0046
- **Phase 4** has 2 independent starting points: 0057 (Rust NIF) and 0058 (Stripe struct) can start in parallel; critical path is 0058 → 0059 → 0060 → 0061 → 0064 → 0066
- **Phase 5** has 2 independent starting points: 0080 (HLC) and 0081 (hashing ring) can start in parallel; critical path is 0081 → 0082 → 0083 → 0084 → 0086/0087/0088 → 0089 → 0091
- **Phase 6** has 3 independent starting points: 0067 (encryption NIF), 0068 (encryption types), 0069 (ACL types) can all start in parallel; two main chains converge at CLI (0078): encryption chain (0067 → 0072 → 0073 → 0074) and ACL chain (0069 → 0075 → 0076)
- **Phase 7** is mostly sequential: 0092 (Volume struct) must come first, then 0093 (SystemVolume API) and 0095 (join replication) can start in parallel; 0094 (cluster init) and 0096 (log retention) follow 0093; critical path is 0092 → 0093 → 0094 → 0097
- **Phase 8** has 1 starting point: 0098 (TLS module) must come first, then 0099 (CA + init), 0101 (renewal), 0102 (revocation) can start in parallel; 0100 (join certs) follows 0099; 0103 (CLI) needs 0099 + 0100 + 0102; critical path is 0098 → 0099 → 0100 → 0103 → 0104
- **Phase 9** has 2 independent starting points: 0105 (ConnPool) and 0106 (Listener + Handler) can start in parallel; 0107 (PoolManager) follows 0105; 0108 (data_call) follows 0107; then 0109 (write path) and 0110 (read path) can be done in parallel; critical path is 0105 → 0107 → 0108 → 0109 → 0111
- **Phases 7 → 8 → 9** are strictly sequential: System Volume → Cluster CA → Data Transfer (each depends on the previous)
- **Phase 10** has 1 starting point: 0112 (event structs) must come first, then 0113 (subscription + Relay) and 0114 (Broadcaster) can start in parallel; 0115 (event emission) follows 0114; 0116 (partition recovery) follows 0113; 0117 (FUSE cache) needs 0113 + 0115; critical path is 0112 → 0114 → 0115 → 0117 → 0118. Phase 10 has no hard dependency on Phases 7-9 but is sequenced after them for practical reasons
- **H-1 (I/O Scheduler)** has 1 starting point: 0165 (types + API) must come first, then 0166 (producer), 0167 (strategies), and 0169 (priority adjustment) can start in parallel after their dependencies; critical path is 0165 → 0166 → 0168 → 0170 → 0171 → 0172
- **H-2 (Prometheus)** has 2 independent starting points: 0173 (metric defs) and 0175 (health check) can start in parallel; critical path is 0173 → 0174 → 0177 → 0178
- **H-3 (Cargo-Fuzz)** has 2 independent starting points: 0179 (blob scaffolding) and 0182 (CLI term parsing) can start in parallel; 0180 and 0181 are parallel after 0179; critical path is 0179 → 0180 → 0183
- **H-4 (Chaos Testing)** is mostly sequential: 0184 → 0185 → 0186 → 0188 → 0189; 0187 (latency) branches from 0185 and merges at 0190; critical path is 0184 → 0185 → 0186 → 0188 → 0189
- **All four H chains** (H-1 through H-4) are completely independent of each other and can be worked in parallel

## Adding New Tasks

When adding tasks:
1. Use the next available number (check existing files)
2. Follow the naming convention: `task_NNNN_brief_description.md`
3. Include all required sections
4. Update this README with the new task
5. Add to the dependency graph if relevant
