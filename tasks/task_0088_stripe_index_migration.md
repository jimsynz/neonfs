# Task 0088: StripeIndex Migration to Quorum Store

## Status
Complete

## Phase
5 - Metadata Tiering

## Description
Migrate StripeIndex from Ra-backed storage to the leaderless quorum system via QuorumCoordinator. Same pattern as ChunkIndex migration (task 0086): replace Ra commands with quorum operations, use deterministic keys, retain local ETS cache. The public API of StripeIndex remains unchanged — callers (erasure-coded write/read paths, stripe repair, GC) are unaffected.

## Acceptance Criteria
- [ ] `StripeIndex.put/2` uses `QuorumCoordinator.quorum_write/3` instead of Ra command
- [ ] `StripeIndex.get/1` uses `QuorumCoordinator.quorum_read/2` instead of Ra query
- [ ] `StripeIndex.delete/1` uses `QuorumCoordinator.quorum_delete/2` instead of Ra command
- [ ] `StripeIndex.exists?/1` uses quorum read (returns boolean)
- [ ] Metadata key format: `"stripe:#{stripe_id}"` — deterministic, not content-addressed
- [ ] Local ETS cache retained for fast lookups (populated from MetadataStore on startup)
- [ ] `restore_from_ra/0` replaced with `load_from_local_store/0` — loads stripe metadata from local BlobStore metadata directory walk into ETS
- [ ] DETS/Persistence integration for stripes removed (BlobStore handles durability)
- [ ] All public API functions (`put`, `get`, `delete`, `exists?`, `get_stripes_for_volume`, `list_all`) maintain their existing signatures and return types
- [ ] `get_stripes_for_volume/1` works via local ETS scan
- [ ] Unit tests: put stripe via quorum, read back, verify equal
- [ ] Unit tests: delete stripe, verify not_found on read
- [ ] Unit tests: local ETS cache updated on write/delete
- [ ] Unit tests: load_from_local_store populates ETS from BlobStore
- [ ] All existing StripeIndex tests pass (adapted to new backend)

## Testing Strategy
- ExUnit tests with QuorumCoordinator (single-node setup)
- ExUnit tests for write → read round-trip through quorum
- ExUnit tests for ETS cache behaviour
- ExUnit tests for load_from_local_store
- Verify all existing erasure coding tests pass (they use StripeIndex API)

## Dependencies
- task_0084 (QuorumCoordinator — quorum read/write/delete operations)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/stripe_index.ex` (modify — replace Ra calls with QuorumCoordinator)
- `neonfs_core/test/neon_fs/core/stripe_index_test.exs` (modify — adapt tests to quorum backend)

## Reference
- spec/metadata.md — Tier 2: Chunk Metadata (same tier as stripe metadata)
- Existing: `neonfs_core/lib/neon_fs/core/stripe_index.ex` — current Ra-backed implementation
- task_0086 — ChunkIndex migration (same pattern)

## Notes
This task follows the exact same pattern as task 0086 (ChunkIndex migration). The key difference is the metadata key format (`"stripe:#{stripe_id}"` vs `"chunk:#{chunk_hash}"`). Stripe metadata is larger than chunk metadata (it includes the full list of chunk hashes in the stripe), but this doesn't affect the storage pattern. The `get_stripes_for_volume/1` function is used by stripe repair and GC — it scans local ETS, same as ChunkIndex's volume-scoped query.
