# Task 0084: Quorum Coordinator

## Status
Not Started

## Phase
5 - Metadata Tiering

## Description
Implement the QuorumCoordinator, which routes metadata operations through the leaderless quorum system. It maps keys to segments via the consistent hashing ring, dispatches read/write/delete operations to the appropriate replica set, and enforces quorum requirements (R+W>N). Stale replicas detected during quorum reads trigger async read repair via the BackgroundWorker infrastructure.

## Acceptance Criteria
- [ ] New `NeonFS.Core.QuorumCoordinator` module
- [ ] `quorum_write(key, value, opts \\ [])` — hash key → segment via MetadataRing → send to W replicas (MetadataStore RPC) → await W acks → return `:ok` or `{:error, reason}`
- [ ] `quorum_read(key, opts \\ [])` — hash key → segment → read from R replicas → return value with highest HLC timestamp → trigger async read repair if stale replicas detected
- [ ] `quorum_delete(key, opts \\ [])` — tombstone write via quorum (delegates to quorum_write with tombstone value)
- [ ] Default quorum: N=3, R=2, W=2 (configurable per-volume via `volume.metadata_consistency`)
- [ ] Quorum falls back gracefully when cluster size < N: `effective_N = min(N, cluster_size)`, R and W adjusted proportionally
- [ ] Local writes go through MetadataStore directly; remote writes go via RPC (`Node.spawn/4` or `:erpc.call/4`)
- [ ] Quorum read returns `{:ok, value}` or `{:error, :not_found}` (majority of replicas report not_found) or `{:error, :quorum_unavailable}`
- [ ] Degraded read mode: when quorum is unreachable, fall back to local MetadataStore replica if available — returns `{:ok, value, :possibly_stale}` so callers can decide whether to serve stale data
- [ ] Degraded reads are configurable (default enabled): `metadata.degraded_reads: true`
- [ ] Quorum write returns `{:ok, :written}` or `{:error, :quorum_unavailable}` (no degraded mode for writes — writes always require quorum)
- [ ] Stale replica detection: compare HLC timestamps from R responses, replicas with older timestamps are stale
- [ ] Stale replicas trigger async read repair submission to BackgroundWorker at `:read_repair` priority
- [ ] Timeout handling: individual replica responses timeout after configurable period (default 5s)
- [ ] Checks ClockMonitor quarantine state: rejects writes from quarantined nodes
- [ ] Telemetry events: `[:neonfs, :quorum, :write]`, `[:neonfs, :quorum, :read]` with segment_id, latency_ms, quorum_size
- [ ] Telemetry events: `[:neonfs, :quorum, :stale_detected]` when read repair triggered
- [ ] Volume consistency field added to Volume struct: `metadata_consistency: %{replicas: N, read_quorum: R, write_quorum: W}`
- [ ] Unit tests: quorum write with all replicas responding
- [ ] Unit tests: quorum write with one replica failing (still meets quorum)
- [ ] Unit tests: quorum write with too many failures (quorum unavailable)
- [ ] Unit tests: quorum read with consistent responses
- [ ] Unit tests: quorum read with stale replica (triggers read repair)
- [ ] Unit tests: quorum read where key not found
- [ ] Unit tests: degraded read returns `{:ok, value, :possibly_stale}` when quorum unreachable but local replica exists
- [ ] Unit tests: degraded read returns `{:error, :quorum_unavailable}` when quorum unreachable and no local replica

## Testing Strategy
- ExUnit tests with mocked MetadataStore and RPC calls
- ExUnit tests for quorum success with various N/R/W configurations
- ExUnit tests for partial failure scenarios (one replica down)
- ExUnit tests for total failure scenarios (quorum unavailable)
- ExUnit tests for stale detection and read repair triggering
- ExUnit tests for graceful degradation with small clusters (N > cluster_size)
- ExUnit tests for quarantined node rejection
- ExUnit tests for degraded read fallback (local replica available vs not available)

## Dependencies
- task_0080 (HLC — timestamp comparison for stale detection)
- task_0081 (MetadataRing — key → segment → replica set routing)
- task_0083 (MetadataStore — local storage backend for metadata)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/quorum_coordinator.ex` (new — quorum routing and coordination)
- `neonfs_client/lib/neon_fs/core/volume.ex` (modify — add optional `metadata_consistency` field)
- `neonfs_core/test/neon_fs/core/quorum_coordinator_test.exs` (new)

## Reference
- spec/metadata.md — Leaderless Quorum Model
- spec/metadata.md — Write Flow, Read Flow with Read Repair
- spec/metadata.md — Quorum Configuration (R+W>N table)
- spec/metadata.md — Metadata Configuration (default_replicas, default_read_quorum, default_write_quorum)
- Existing pattern: BackgroundWorker infrastructure (Phase 3) for async read repair submission

## Notes
The QuorumCoordinator is the central routing layer — all metadata operations (ChunkIndex, FileIndex, StripeIndex) go through it. It never touches metadata directly; it delegates to MetadataStore on each replica. The ring is rebuilt when Ra notifies of membership changes (segment_assignments updated). For performance, the QuorumCoordinator should cache the current ring and segment assignments locally. The read repair path must be async — it should never block the read response. When R=1 (fast reads), no stale detection is possible since there's only one response; read repair depends on anti-entropy in that case.

**Degraded reads:** When the quorum is temporarily unreachable (e.g., during a partial network partition), the QuorumCoordinator can fall back to reading from a local MetadataStore replica. This is analogous to Ceph's capabilities model — once data has been resolved, stale metadata is almost always preferable to failing entirely, because data chunks are immutable and content-addressed. The `{:ok, value, :possibly_stale}` return value lets callers (FUSE handler, ReadOperation) decide whether to serve stale data. Degraded reads are never used for writes — writes always require quorum to prevent split-brain divergence.
