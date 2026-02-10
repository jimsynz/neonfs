# Task 0086: ChunkIndex Migration to Quorum Store

## Status
Not Started

## Phase
5 - Metadata Tiering

## Description
Migrate ChunkIndex from Ra-backed storage to the leaderless quorum system via QuorumCoordinator. Replace all `maybe_ra_command` calls with `QuorumCoordinator.quorum_write/3` and all `get_from_ra` calls with `QuorumCoordinator.quorum_read/2`. Chunk metadata is stored in the BlobStore metadata namespace with deterministic keys. The public API of ChunkIndex remains unchanged — callers (WriteOperation, ReadOperation, GC) are unaffected.

## Acceptance Criteria
- [ ] `ChunkIndex.put/2` uses `QuorumCoordinator.quorum_write/3` instead of Ra command
- [ ] `ChunkIndex.get/1` uses `QuorumCoordinator.quorum_read/2` instead of Ra query
- [ ] `ChunkIndex.delete/1` uses `QuorumCoordinator.quorum_delete/2` instead of Ra command
- [ ] `ChunkIndex.exists?/1` uses quorum read (returns boolean)
- [ ] Metadata key format: `"chunk:#{Base.encode16(chunk_hash)}"` — deterministic, not content-addressed
- [ ] Local ETS cache retained for fast lookups (populated from MetadataStore on startup, updated on writes)
- [ ] `restore_from_ra/0` replaced with `load_from_local_store/0` — loads chunk metadata from local BlobStore metadata directory walk into ETS
- [ ] `active_write_refs` tracking stays local (ephemeral, single-node — not replicated)
- [ ] DETS/Persistence integration for chunks removed (BlobStore handles durability)
- [ ] All public API functions (`put`, `get`, `delete`, `exists?`, `get_chunks_for_volume`, `list_all`) maintain their existing signatures and return types
- [ ] `get_chunks_for_volume/1` works via local ETS scan (not quorum read — would be too expensive)
- [ ] Unit tests: put chunk via quorum, read back, verify equal
- [ ] Unit tests: delete chunk, verify not_found on read
- [ ] Unit tests: local ETS cache updated on write/delete
- [ ] Unit tests: load_from_local_store populates ETS from BlobStore
- [ ] All existing ChunkIndex tests pass (adapted to new backend)

## Testing Strategy
- ExUnit tests with QuorumCoordinator (can use single-node setup where quorum of 1 is sufficient)
- ExUnit tests for write → read round-trip through quorum
- ExUnit tests for ETS cache behaviour (cache hit avoids quorum read)
- ExUnit tests for load_from_local_store (write records via BlobStore, call load, verify ETS populated)
- ExUnit tests for active_write_refs (unchanged, local-only)
- Verify all existing WriteOperation and ReadOperation tests pass (they use ChunkIndex API)

## Dependencies
- task_0084 (QuorumCoordinator — quorum read/write/delete operations)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_index.ex` (modify — replace Ra calls with QuorumCoordinator, replace restore_from_ra with load_from_local_store)
- `neonfs_core/test/neon_fs/core/chunk_index_test.exs` (modify — adapt tests to quorum backend)

## Reference
- spec/metadata.md — Tier 2: Chunk Metadata (Distributed Index)
- Existing: `neonfs_core/lib/neon_fs/core/chunk_index.ex` — current Ra-backed implementation

## Notes
The key insight is that ChunkIndex's public API doesn't change — only the storage backend moves from Ra to quorum-based BlobStore. The ETS cache serves the same purpose as before (fast local lookups), but is now populated from the local BlobStore metadata directory rather than Ra snapshots. The `get_chunks_for_volume/1` function is used by GC and must remain efficient; it scans local ETS rather than doing per-chunk quorum reads. This means it only sees chunks known to the local node, which is correct for GC (each node cleans up its own stored chunks). The `active_write_refs` map is intentionally not replicated — it tracks in-flight writes on this specific node and is meaningless on other nodes.
