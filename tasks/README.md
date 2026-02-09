# NeonFS Development Tasks

This directory contains individual task specifications for developing NeonFS. Each task is designed to be small enough for an agent to complete within a single context window.

## Task Format

Each task file follows this structure:
- **Status**: Current state of the task (see below)
- **Phase**: Which implementation phase (1-7) this task belongs to
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

### Phase 5: Security (Future)
**Goal:** Production-ready security model

### Phase 6: APIs and Integration (Future)
**Goal:** S3, Docker, CIFS, CSI access methods

### Phase 7: Operations (Future)
**Goal:** Production operations support

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

## Adding New Tasks

When adding tasks:
1. Use the next available number (check existing files)
2. Follow the naming convention: `task_NNNN_brief_description.md`
3. Include all required sections
4. Update this README with the new task
5. Add to the dependency graph if relevant
