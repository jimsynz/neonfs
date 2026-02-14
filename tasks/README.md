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

### Phase 8: Cluster CA (Future)
**Goal:** Self-signed certificate authority for inter-node mTLS

ECDSA P-256 CA stored in the system volume. Nodes receive certificates via CSR during join. Auto-renewal, CRL revocation, CLI commands.

Task specifications not yet written. See [spec/cluster-ca.md](../spec/cluster-ca.md).

### Phase 9: Data Transfer (Future)
**Goal:** Out-of-band data plane separating bulk chunk traffic from Erlang distribution

TLS data plane using `:ssl` with `{packet, 4}` framing and `nimble_pool` connection pooling. Chunk replication and retrieval migrated from distribution to the data plane.

Task specifications not yet written. See [spec/data-transfer.md](../spec/data-transfer.md).

### Phase 10: Event Notification (Future)
**Goal:** Push-based cache invalidation for interface nodes

Two-layer dispatch (`:pg` cross-node relay + `Registry` local fan-out). Struct-based events for file content, attributes, ACLs, directories, and volumes. Partition recovery with debounced invalidation.

Task specifications not yet written. See [spec/pubsub.md](../spec/pubsub.md).

### Phase 11: APIs and Integration (Future)
**Goal:** S3, Docker, CIFS, CSI access methods

### Phase 12: Operations (Future)
**Goal:** Production operations support (DR snapshots, monitoring, capacity planning)

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

Phase 8 → 9 → 10 (linear chain, task numbers TBD):

Phase 8 (Cluster CA) ──▶ Phase 9 (Data Transfer)

Phase 10 (Event Notification) — no hard dependency on 7-9, sequenced after for practical reasons
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
- **Phases 7 → 8 → 9** are strictly sequential: System Volume → Cluster CA → Data Transfer (each depends on the previous)
- **Phase 10** (Event Notification) has no dependency on Phases 7-9 and could theoretically be developed in parallel, but is sequenced after them for practical reasons

## Adding New Tasks

When adding tasks:
1. Use the next available number (check existing files)
2. Follow the naming convention: `task_NNNN_brief_description.md`
3. Include all required sections
4. Update this README with the new task
5. Add to the dependency graph if relevant
