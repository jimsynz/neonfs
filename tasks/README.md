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

### Phase 1: Foundation (Tasks 0001-0030)
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

### Phase 3: Policies, Tiering, and Compression (Future)
**Goal:** Volume policies, tiered storage management, compression

### Phase 4: Erasure Coding (Future)
**Goal:** Space-efficient durability for large files

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

## Adding New Tasks

When adding tasks:
1. Use the next available number (check existing files)
2. Follow the naming convention: `task_NNNN_brief_description.md`
3. Include all required sections
4. Update this README with the new task
5. Add to the dependency graph if relevant
