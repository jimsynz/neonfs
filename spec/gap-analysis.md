# Gap Analysis: Specification vs Implementation

**Date:** 2026-02-21
**Scope:** Phases 1–10 (Foundation through Event Notification) plus foundational infrastructure
**Method:** Systematic comparison of all spec documents against the codebase

## Overview

Phases 1–10 and their associated tasks (0001–0118) are marked complete. This
analysis identifies features, behaviours, and infrastructure described in the
specification documents that are missing or incomplete in the current
implementation. Items that belong to future phases (11–16) are excluded unless
the spec describes them as foundational.

Each item is categorised by effort:

| Category   | Definition                                         | Count | Done |
|------------|----------------------------------------------------|-------|------|
| Quick Win  | Focused change, single file or a few lines, < 1 day | 7     | 7    |
| Medium     | Scoped work with clear boundaries, 1–5 days        | 16    | 2    |
| Hard       | New subsystem or architectural work, 1–4 weeks      | 4     | 0    |

---

## Quick Wins (all complete)

### QW-1: FUSE `create`/`mkdir` ignores mode parameter ✅

**Task:** 0119 — **Commit:** `a0b2454`

**Fix:** Pass the request's mode through to `FileMeta.new/3` in the `create`
and `mkdir` handlers.

---

### QW-2: `ctime` not updated on metadata changes ✅

**Task:** 0120 — **Commit:** `162cb13`

**Fix:** Added `changed_at` field to `FileMeta`, set in `update/2`, returned
in FUSE `getattr` responses.

---

### QW-3: `ChunkCache` memory limit not configurable ✅

**Task:** 0121 — **Commit:** `ea3cbd5`

**Fix:** Cache limit read from application config
(`config :neonfs_core, :chunk_cache_max_memory`) with fallback to 256 MB
default. See also M-15 for the broader per-node cache model change.

---

### QW-4: `ChunkFetcher` doesn't consult `volume.caching` flags ✅

**Task:** 0122 — **Commit:** `d767154`

**Fix:** `ChunkFetcher` now looks up the volume's caching config to decide
whether to cache.

---

### QW-5: Volume `verification_config` missing `scrub_interval` field ✅

**Task:** 0123 — **Commit:** `2786959`

**Fix:** Added `scrub_interval` to the `verification_config` type and
`default_verification/0`.

---

### QW-6: `atime` not auto-updated on read ✅

**Task:** 0124 — **Commit:** `f885ff1`

**Fix:** Added volume-level `atime_mode` setting (`:noatime` | `:relatime`)
with `relatime`-style updates on read.

---

### QW-7: Truncate is metadata-only ✅

**Task:** 0125 — **Commit:** `7e9a474`

**Fix:** Truncate now removes chunk references beyond the new size in
`FileIndex`.

---

## Medium

### M-1: CLI commands for GC and scrubbing

**Tasks:** 0126–0131

**Spec reference:** `spec/architecture.md` lines 370–388; `spec/replication.md` lines 619–630
**Location:** `neonfs_core/lib/neon_fs/cli/handler.ex`, `cli/src/commands/`

`GarbageCollector` exists but isn't wired into the CLI. Missing commands:

- `neonfs gc collect [--volume <id>]` — trigger collection
- `neonfs gc status` — show last run, next scheduled, protected chunks
- `neonfs scrub start [--volume <id>]` — start integrity scan
- `neonfs scrub status` — show scan progress

Requires handler functions in `handler.ex` and Rust CLI command modules.

---

### M-2: Scheduled GC execution

**Tasks:** 0132–0134

**Spec reference:** `spec/replication.md` lines 619–630
**Location:** `neonfs_core/lib/neon_fs/core/`

The spec describes GC running on a schedule (e.g. daily) and also triggering
when storage exceeds 85%. No scheduler invokes `GarbageCollector.collect/1`.

**Work:** Create `GCScheduler` GenServer with periodic job creation via
`JobTracker`, add a storage-pressure trigger using `StorageMetrics`, and wire
into the supervision tree with `cluster.json` configuration support.

---

### M-3: Background scrubbing job

**Tasks:** 0135–0137

**Spec reference:** `spec/architecture.md` lines 370–388
**Location:** `neonfs_core/lib/neon_fs/core/`

No periodic integrity verification of stored chunks exists. The spec
describes scrubbing as mandatory even when `on_read: :never`.

**Work:** Create `Job.Runners.Scrub` that walks all chunks on local drives,
verifies hashes, and reports corruption. Submit repairs via
`BackgroundWorker`. Integrate with `scrub_interval` from QW-5.

---

### M-4: Volume configuration not fully exposed in CLI

**Tasks:** 0138–0139

**Spec reference:** `spec/architecture.md` — volume configuration
**Location:** `cli/src/commands/volume.rs`, `neonfs_core/lib/neon_fs/cli/handler.ex`

Only `--replicas`, `--compression`, and `--encryption` are available on
`volume create`/`volume update`. The `Volume` struct has ~15 configurable
fields that cannot be set:

- Durability mode and EC params (`data_chunks`, `parity_chunks`)
- Tiering (`initial_tier`, `promotion_threshold`, `demotion_delay`)
- Caching (`transformed_chunks`, `reconstructed_stripes`, `remote_chunks`, `max_memory`)
- Verification (`on_read`, `sampling_rate`, `scrub_interval`)
- Metadata consistency (`metadata_replicas`, `read_quorum`, `write_quorum`)
- Write acknowledgement policy (`:local` | `:quorum` | `:all`)
- I/O weight

**Work:** Add CLI flags for each field group, update handler to accept them,
add validation.

---

### M-5: Coldness scoring formula

**Task:** 0140

**Spec reference:** `spec/storage-tiering.md` — eviction under pressure
**Location:** `neonfs_core/lib/neon_fs/core/tiering_manager.ex`

Eviction sorts by `stats.daily` only. The spec defines a blended score:

```
coldness = -hours_since(last_access) + (access_count_24h * 10)
```

**Work:** Implement the formula, replace the simple sort in
`evict_from_tier/4`.

---

### M-6: Access statistics not persistent across restarts

**Task:** 0141

**Spec reference:** `spec/storage-tiering.md` — access statistics
**Location:** `neonfs_core/lib/neon_fs/core/chunk_access_tracker.ex`

`ChunkAccessTracker` uses in-memory ETS. Restarting a node loses all history.
No cross-node aggregation exists either.

**Work:** Periodically snapshot access stats to the quorum BlobStore (or a
local file). On startup, restore from snapshot. Consider a lightweight
gossip protocol for cross-node stats.

---

### M-7: Structured JSON logging

**Tasks:** 0142–0144

**Spec reference:** `spec/operations.md` lines 200–218; `spec/observability.md`
**Location:** All 45+ modules using `Logger`

All logs are plain text with no structured fields, no `Logger.metadata`
context propagation, no trace or span IDs.

**Work:** Configure a JSON log formatter (e.g. `LoggerJSON`), define standard
metadata fields (`:component`, `:node`, `:volume_id`, `:trace_id`), add
`Logger.metadata/1` at request entry points.

---

### M-8: Property tests from spec not implemented

**Tasks:** 0145–0148

**Spec reference:** `spec/testing.md` lines 290–336
**Location:** Test suites across all packages

The spec defines HLC property tests (transitivity, antisymmetry, tick
advancement), quorum consistency properties, and consistent hash invariants.
Only `MetadataRing` has Elixir property tests. `stream_data` isn't even a
dependency in `neonfs_client`, `neonfs_fuse`, or `neonfs_integration`.

**Work:** Add `stream_data` dependency where needed. Implement property tests
for HLC, quorum coordinator, and consistent hashing. Add Rust `proptest`
coverage for encryption round-trips.

---

### M-9: FUSE `setattr` operations not integration-tested

**Task:** 0149

**Spec reference:** `spec/testing.md` — integration test strategy
**Location:** `neonfs_integration/test/integration/fuse_handler_test.exs`

`chmod`, `chown`, `utimens`, and `truncate` via `setattr` have no integration
test coverage. Current handler tests only cover core CRUD operations.

**Work:** Add integration tests exercising each `setattr` sub-operation
including permission enforcement edge cases.

---

### M-10: Network partition integration tests

**Tasks:** 0150–0151

**Spec reference:** `spec/testing.md` lines 608–828; `spec/node-management.md`
**Location:** `neonfs_integration/test/integration/failure_test.exs`

Only node crash is tested (3 tests). No tests simulate actual network splits
where the minority partition becomes read-only and the majority continues
writing.

**Work:** Use `:erlang.set_cookie/2` or `:net_kernel` controls to isolate
node subsets. Verify minority read-only behaviour, majority writes, and
partition healing.

---

### M-11: Worker configuration CLI commands

**Tasks:** 0152–0154

**Spec reference:** `spec/operations.md` — runtime configuration
**Location:** `neonfs_core/lib/neon_fs/cli/handler.ex`

Background worker settings (`max_concurrent`, `max_per_minute`,
`drive_concurrency`) can only be changed by editing `cluster.json` manually.

**Work:** Add `neonfs cluster configure --worker-max-concurrent N` (etc.)
commands that update `cluster.json` and hot-reload the BackgroundWorker.

---

### M-12: `cluster.json` validation and missing settings

**Tasks:** 0155–0156

**Spec reference:** `spec/operations.md` — configuration management; `spec/node-management.md`
**Location:** `neonfs_core/lib/neon_fs/core/application.ex`, `neonfs_core/lib/neonfs/cluster/state.ex`

Two issues:

1. **No schema validation at startup.** Malformed or missing fields in
   `cluster.json` cause runtime errors rather than a clear startup failure
   with actionable messages.

2. **Missing settings.** The spec defines cluster-level settings that should
   live in `cluster.json` but don't exist yet:
   - `peer_sync_interval` — how often to sync the peer list
   - `peer_connect_timeout` — timeout for establishing node connections
   - `min_peers_for_operation` — write quorum requirement
   - `startup_peer_timeout` — how long to wait for peers on boot

**Work:**
- Add a validation pass in `Application.start/2` that checks all required
  fields, types, and value ranges before starting the supervision tree
- Add the missing cluster-level settings to `Cluster.State` with sensible
  defaults
- Wire the new settings into `Connection`, `Discovery`, and `Router`

---

### M-13: Drive and space behaviour tests using loopback devices

**Tasks:** 0157–0158

**Spec reference:** `spec/testing.md` — failure injection; `spec/node-management.md`
**Location:** `neonfs_integration/test/`

No tests exercise real space-related behaviours: drive filling up, capacity
thresholds triggering GC or eviction, drive evacuation under I/O, or
rebalance across drives with different free space.

Loopback devices (`dd` + `losetup` + `mkfs`) can create small real
filesystems (e.g. 50 MB) that behave exactly like real drives, including
returning `ENOSPC` when full. This avoids mock blob stores entirely and
tests the real I/O path.

**Work:**
- Add test helpers to create/mount/teardown loopback devices
- Test: write until drive is full, verify `ENOSPC` handling
- Test: capacity threshold triggers GC/eviction
- Test: drive evacuation moves data to other drives
- Test: rebalance redistributes chunks after drive add/remove
- Ensure cleanup on test teardown (unmount + `losetup -d`)

---

### M-14: Read verification not wired into read path

**Task:** 0159

**Spec reference:** `spec/architecture.md` lines 353–368
**Location:** `neonfs_core/lib/neon_fs/core/read_operation.ex`

`verification_config.on_read` exists in `Volume` (`:always` | `:never` |
`:sampling`) but the read path doesn't call chunk hash verification based on
this setting.

**Work:** After reading a chunk, optionally verify its SHA-256 hash according
to the volume's verification config. On mismatch, trigger read repair.

---

### M-15: Cache memory limit should be per-node, not per-volume

**Task:** 0160

**Spec reference:** `spec/storage-tiering.md` — application-level caching
**Location:** `neonfs_core/lib/neon_fs/core/chunk_cache.ex`, `neonfs_client/lib/neon_fs/core/volume.ex`

The spec defines `max_memory` as a per-volume setting in `caching_config`,
but this doesn't make practical sense — operators care about total memory
consumption on a node, not per-volume budgets that silently compound as
volumes are added. A node with 10 volumes each configured for 256 MB would
consume 2.5 GB of cache.

**Work:**
- Move `max_memory` from `Volume.caching_config` to a node-level
  application config setting (e.g. `config :neonfs_core, :chunk_cache_max_memory`)
- `ChunkCache` should enforce a single global limit with LRU eviction across
  all volumes
- Remove the per-volume `max_memory` field from the Volume struct
- Update the spec to reflect the per-node model

---

### M-16: Structured errors using `splode`

**Tasks:** 0161–0164

**Spec reference:** General — error handling throughout all spec documents
**Location:** All packages

Errors throughout the codebase are ad-hoc atoms and strings: `{:error, :not_found}`,
`{:error, :all_nodes_unreachable}`, `{:error, "invalid path"}`, etc. There is
no error hierarchy, no consistent classification, and no way to distinguish
user-facing errors from internal failures programmatically.

The `splode` hex package provides structured, classifiable error types with
error classes (e.g. `:invalid`, `:framework`, `:unknown`) and composable
error modules. This would give:

- Consistent error types across all packages
- Machine-readable error classes for API consumers (S3, Docker plugin, CSI)
- Clear separation of user errors vs internal failures
- Better error messages with contextual breadcrumbs
- Foundation for structured error responses in future HTTP APIs

**Work:**
- Add `splode` dependency to `neonfs_client` (shared error types)
- Define error class hierarchy: `NeonFS.Error` with classes for
  `:invalid` (bad input), `:forbidden` (ACL), `:not_found`,
  `:unavailable` (quorum loss), `:internal` (unexpected failures)
- Migrate error returns incrementally, starting with the boundaries:
  CLI handler, FUSE handler, and `NeonFS.Client.Router`
- Update `with` blocks to pattern-match on error structs

---

## Hard

### H-1: GenStage I/O Scheduler

**Tasks:** 0165–0172

**Spec reference:** `spec/architecture.md` lines 501–740; `spec/storage-tiering.md` lines 637–790
**Location:** New subsystem in `neonfs_core/lib/neon_fs/io/`

The spec describes a full producer/consumer I/O scheduling architecture:

- **GenStage producer** with Weighted Fair Queuing (WFQ) per-volume
- **Per-drive worker processes** with separate concurrency limits
- **6 priority classes**: user read, user write, replication, read repair,
  repair/resilver, scrubbing
- **HDD elevator scheduling**: batch reads/writes, minimise seeking, sort by
  chunk hash prefix
- **SSD strategy**: parallel I/O, FIFO, interleaved reads/writes
- **Dynamic priority adjustment** under storage pressure
- **Backpressure signalling** from workers to producers

Currently only `BackgroundWorker` exists: a GenServer with 3-level priority
FIFO. No `gen_stage` dependency, no per-drive workers, no WFQ, no
drive-type-specific strategies.

**Design decisions needed:**
- GenStage vs Broadway vs custom demand-driven GenServer
- How to detect drive type (SSD vs HDD) at runtime
- How to integrate with existing `BackgroundWorker` consumers (read repair,
  stripe repair, tier migration)

**New modules:**
- `NeonFS.IO.Scheduler` — public API facade
- `NeonFS.IO.Producer` — GenStage producer with WFQ
- `NeonFS.IO.DriveWorker` — per-drive GenStage consumer
- `NeonFS.IO.DriveStrategy` — drive-type-specific scheduling

---

### H-2: Prometheus metrics and health HTTP endpoints

**Tasks:** 0173–0178

**Spec reference:** `spec/observability.md` (482 lines)
**Location:** New subsystem in `neonfs_core/`

The spec defines 40+ metrics across chunk operations, replication, repair,
storage, cluster, FUSE, and intent logs. Also requires:

- `GET /metrics` — Prometheus text format
- `GET /health` — JSON health status aggregated from all subsystems
- Telemetry poller for gauge emission (queue sizes, utilisation)
- Alerting rule definitions

Telemetry events are already emitted from 29 modules. The missing piece is
collection, aggregation, and HTTP exposure.

**Dependencies:** `telemetry_metrics_prometheus_core`, `bandit` or `plug_cowboy`

**Work:**
- Add `NeonFS.Core.Telemetry` module with metric definitions
- Add `NeonFS.Core.HealthCheck` aggregating status from `StorageMetrics`,
  `ClockMonitor`, `RaServer`, `ServiceRegistry`, etc.
- Add HTTP endpoint (Bandit + Plug) serving `/metrics` and `/health`
- Add `TelemetryPoller` for periodic gauge emission

---

### H-3: Cargo-fuzz targets

**Tasks:** 0179–0183

**Spec reference:** `spec/testing.md` lines 419–487
**Location:** `neonfs_core/native/neonfs_blob/fuzz/`, `cli/fuzz/`

The spec defines three fuzzing targets:

- `neonfs_blob/fuzz/fuzz_targets/chunk_parsing.rs` — parsing robustness
- `neonfs_blob/fuzz/fuzz_targets/compression.rs` — malformed compression data
- `cli/fuzz/fuzz_targets/term_parsing.rs` — malformed Erlang terms

None exist. Requires `cargo-fuzz` setup, `libfuzzer_sys` integration,
nightly Rust toolchain in CI, seed corpus creation, and CI integration for
periodic fuzzing runs.

---

### H-4: Container-based chaos testing framework

**Tasks:** 0184–0190

**Spec reference:** `spec/testing.md` lines 608–828
**Location:** `neonfs_integration/`

The spec proposes a container-based testing framework with:

- `TestCluster.pause_node/2` — pause container (true network partition)
- `TestCluster.kill_node/2` — kill container (crash)
- `TestCluster.unpause_node/2` — resume after partition
- Latency injection between specific node pairs
- Drive failure simulation at the I/O level

Current `PeerCluster` uses `:peer.start_link` which only supports stop (no
pause/partition). True network partition simulation needs either container
control or a custom RPC intercept layer.

**Work:**
- `NeonFS.Integration.Chaos` module with fault injection primitives
- Either container-based (Docker/Podman) or BEAM-level (message interception)
  approach
- Integration with existing `ClusterCase` test helpers
- Tests for: minority read-only, majority writes, partition healing, latency
  tolerance, cascading failures

---

## Items Confirmed as Not Gaps

The following were checked and confirmed to be either intentionally deferred
to future phases or correctly out of scope:

| Item | Reason |
|------|--------|
| Hard links / symlinks | Deferred per `spec/appendix.md` |
| Extended attributes (xattr) | Deferred per `spec/api-surfaces.md` |
| Advisory locking (flock/fcntl) | Intent log provides exclusive writes instead |
| Stripe compaction | Explicitly deferred in `spec/replication.md` |
| YAML config file (`daemon.conf`) | `cluster.json` covers this; missing settings tracked in M-12 |
| S3-compatible API | Phase 11 |
| Docker/Podman volume plugin | Phase 12 |
| CIFS/SMB | Phase 13 |
| CSI driver | Phase 14 |
| Alerting rules (Prometheus AlertManager) | Phase 15 |
| DR snapshots / capacity planning | Phase 15 |
| Documentation suite | Phase 16 |
