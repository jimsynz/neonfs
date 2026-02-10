# Task 0089: Read Repair and Write/Read Path Adaptation

## Status
Complete

## Phase
5 - Metadata Tiering

## Description
Implement the ReadRepair module for async repair of stale metadata replicas, a ResolvedLookupCache for metadata availability during partial outages, and adapt WriteOperation and ReadOperation to work with the new quorum-based metadata system. Also update the supervision tree to include new Phase 5 children (ClockMonitor, MetadataStore DynamicSupervisor) and remove DETS-based metadata snapshotting from the Persistence module (BlobStore handles durability now).

## Acceptance Criteria
- [ ] New `NeonFS.Core.ReadRepair` module
- [ ] `ReadRepair.submit/3` — `(key, latest_value, stale_replicas)` → submits async repair job to BackgroundWorker at `:read_repair` priority
- [ ] ReadRepair worker: for each stale replica, writes the latest value via MetadataStore RPC
- [ ] ReadRepair is coalesced: multiple repair requests for the same key within a short window are merged into a single repair
- [ ] ReadRepair failures are logged but don't block reads (fire-and-forget from the caller's perspective)
- [ ] Telemetry events: `[:neonfs, :read_repair, :submitted]`, `[:neonfs, :read_repair, :completed]`, `[:neonfs, :read_repair, :failed]`
- [ ] New `NeonFS.Core.ResolvedLookupCache` module — ETS-backed cache of fully resolved file→chunk mappings
- [ ] `ResolvedLookupCache.put/3` — `(file_id, resolved_metadata, ttl)` → caches the complete chunk list, stripe info, and POSIX attrs needed to serve reads
- [ ] `ResolvedLookupCache.get/1` — `(file_id)` → returns cached resolution or `:miss`
- [ ] `ResolvedLookupCache.evict/1` — `(file_id)` → evicts entry (called on local writes and pub/sub invalidation)
- [ ] TTL-based expiry: entries expire after configurable period (default 5 minutes, per-volume overridable)
- [ ] Max entries limit with LRU eviction when cache is full (default 100,000 entries)
- [ ] Telemetry events: `[:neonfs, :resolved_cache, :hit]`, `[:neonfs, :resolved_cache, :miss]`, `[:neonfs, :resolved_cache, :evict]`
- [ ] `ReadOperation.read_file/3` checks ResolvedLookupCache before quorum read; populates cache after successful quorum resolution
- [ ] `ReadOperation.read_file/3` falls back to ResolvedLookupCache when quorum read returns `{:ok, value, :possibly_stale}` or `{:error, :quorum_unavailable}`
- [ ] `WriteOperation.write_file/4` evicts the file's ResolvedLookupCache entry after successful write
- [ ] `WriteOperation.write_file/4` adapted: file creation uses IntentLog-backed path via FileIndex
- [ ] `WriteOperation` chunk metadata writes go through ChunkIndex (which now uses quorum internally)
- [ ] `ReadOperation.read_file/3` adapted: minimal changes needed (already uses Index APIs)
- [ ] `Persistence` module updated: remove DETS snapshotting for chunk/file/stripe metadata (BlobStore handles durability)
- [ ] `Persistence` module retains any non-metadata persistence (e.g., configuration state)
- [ ] `Supervisor` updated: add ClockMonitor child, add MetadataStore DynamicSupervisor child
- [ ] Supervisor startup order: MetadataStore DynamicSupervisor starts before ChunkIndex/FileIndex/StripeIndex (they need MetadataStore for loading)
- [ ] ChunkIndex, FileIndex, StripeIndex startup: call `load_from_local_store/0` to populate ETS from BlobStore
- [ ] Unit tests for ReadRepair: submit repair, verify stale replica updated
- [ ] Unit tests for ReadRepair: coalescing (multiple submits for same key → single repair)
- [ ] Unit tests for ResolvedLookupCache: put/get round-trip
- [ ] Unit tests for ResolvedLookupCache: TTL expiry (entry expires after TTL)
- [ ] Unit tests for ResolvedLookupCache: eviction on write (put entry, evict, verify miss)
- [ ] Unit tests for ResolvedLookupCache: LRU eviction when max entries exceeded
- [ ] Unit tests for ReadOperation: uses cached resolution when quorum unavailable
- [ ] Integration: WriteOperation tests pass with quorum-backed indexes
- [ ] Integration: ReadOperation tests pass with quorum-backed indexes
- [ ] All existing Phase 1-4 tests pass (no regressions)
- [ ] Skipped integration tests restored and passing — remove `@moduletag skip:` from: ReplicationTest, FailureTest, SingleNodeTest, Phase3Test, FuseHandlerTest, ErasureCodingTest (skipped in task 0087 after FileIndex Ra fallback removal)
- [ ] Supervisor passes `quorum_opts` to FileIndex (and ChunkIndex) so the production supervision tree uses quorum mode

## Testing Strategy
- ExUnit tests for ReadRepair module in isolation (mock BackgroundWorker)
- ExUnit tests for ReadRepair coalescing behaviour
- ExUnit tests for ResolvedLookupCache: put/get, TTL expiry, eviction, LRU overflow
- ExUnit tests for ReadOperation using cached resolution during degraded quorum
- ExUnit tests for WriteOperation with IntentLog-backed file creation
- ExUnit tests for ReadOperation with quorum-backed chunk lookups
- Verify Persistence module correctly removed metadata DETS code
- Verify supervisor starts all new children in correct order
- Run full `mix test` in neonfs_core to verify no regressions

## Dependencies
- task_0086 (ChunkIndex migration — quorum-backed chunk metadata)
- task_0087 (FileIndex migration — quorum-backed file + directory metadata)
- task_0088 (StripeIndex migration — quorum-backed stripe metadata)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/read_repair.ex` (new — async read repair via BackgroundWorker)
- `neonfs_core/lib/neon_fs/core/resolved_lookup_cache.ex` (new — ETS-backed cache of resolved file→chunk mappings)
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — file creation via IntentLog path, evict cache on write)
- `neonfs_core/lib/neon_fs/core/read_operation.ex` (modify — minimal, uses Index APIs which now use quorum internally)
- `neonfs_core/lib/neon_fs/core/persistence.ex` (modify — remove metadata DETS snapshotting)
- `neonfs_core/lib/neon_fs/core/supervisor.ex` (modify — add ClockMonitor, MetadataStore DynamicSupervisor)
- `neonfs_core/test/neon_fs/core/read_repair_test.exs` (new)

## Reference
- spec/metadata.md — Read Flow with Read Repair
- spec/metadata.md — Reactor-Based Orchestration (file creation flow)
- spec/metadata.md — Crash Recovery
- Existing pattern: BackgroundWorker infrastructure (Phase 3) for async job submission
- Existing: `neonfs_core/lib/neon_fs/core/supervisor.ex` — current supervision tree

## Notes
The key insight is that WriteOperation and ReadOperation are minimally affected — they already use Index module APIs (ChunkIndex.put, FileIndex.get_by_path, etc.). The quorum layer is invisible to them. The main change is that file creation in WriteOperation needs to go through the IntentLog-backed path in FileIndex.create rather than directly writing Ra commands. ReadRepair must be async and non-blocking: it's submitted to BackgroundWorker and forgotten. If repair fails (e.g., stale node is down), anti-entropy (task 0090) will catch it later. The Persistence module cleanup is important — without it, the system would try to snapshot metadata to DETS that no longer exists in the Ra state.

**ResolvedLookupCache:** This cache is the primary mechanism for metadata availability during partial outages. It caches the *composed result* of a full file resolution (complete chunk list needed to serve reads), not individual metadata records. This is analogous to Ceph's capabilities model — once a client has resolved a file's metadata, it can continue serving reads even if the metadata quorum is temporarily unreachable. Since data chunks are immutable and content-addressed, stale metadata points to older but still valid file versions. The cache must be evicted on writes (to prevent serving stale data after an update) and expires after a TTL (to bound staleness). The cache is separate from MetadataStore's per-segment ETS caches — MetadataStore caches raw records per segment, while ResolvedLookupCache caches the final composed result of a multi-step lookup (directory entry → file meta → chunk list).
