# Task 0090: Anti-Entropy

## Status
Not Started

## Phase
5 - Metadata Tiering

## Description
Implement periodic anti-entropy for metadata segments. While quorum reads with read repair handle consistency for actively accessed data, rarely-accessed metadata can drift between replicas. Anti-entropy periodically compares Merkle trees between segment replicas and repairs any differences, ensuring all replicas eventually converge.

## Acceptance Criteria
- [ ] New `NeonFS.Core.AntiEntropy` module
- [ ] `AntiEntropy.sync_segment/1` — compares Merkle trees between all replicas of a segment, repairs differences
- [ ] Merkle tree computation: hash all key-value pairs in a segment into a tree structure for efficient comparison
- [ ] `MetadataStore.merkle_tree/1` — computes Merkle tree for a local segment (hash of each record, then tree of hashes)
- [ ] Tree comparison: identify keys that differ between replicas, fetch the latest version (highest HLC timestamp), write to stale replicas
- [ ] Anti-entropy runs as a periodic BackgroundWorker job at configurable interval (default 6 hours)
- [ ] Anti-entropy jobs submitted at low priority to avoid impacting normal operations
- [ ] Only processes segments where this node is a replica (doesn't sync segments it's not responsible for)
- [ ] Tombstone cleanup: during anti-entropy, if all replicas agree a key is tombstoned, the tombstone can be removed
- [ ] Telemetry events: `[:neonfs, :anti_entropy, :started]` with segment_id
- [ ] Telemetry events: `[:neonfs, :anti_entropy, :completed]` with segment_id, keys_repaired, duration_ms
- [ ] Telemetry events: `[:neonfs, :anti_entropy, :tombstones_cleaned]` with count
- [ ] Unit tests: two replicas with identical data — no repairs needed
- [ ] Unit tests: one replica missing a key — key replicated to it
- [ ] Unit tests: one replica has older version — updated with latest
- [ ] Unit tests: tombstone cleanup when all replicas agree

## Testing Strategy
- ExUnit tests with two MetadataStore instances simulating different replicas
- ExUnit tests: write different data to each replica, run sync, verify both converge
- ExUnit tests: Merkle tree computation and comparison
- ExUnit tests: tombstone cleanup (write tombstone to all replicas, run sync, verify tombstone removed)
- ExUnit tests for BackgroundWorker integration (job submission, priority)
- Verify anti-entropy doesn't interfere with concurrent quorum operations

## Dependencies
- task_0084 (QuorumCoordinator — segment → replica set mapping, MetadataStore RPC)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/anti_entropy.ex` (new — periodic Merkle tree sync)
- `neonfs_core/lib/neon_fs/core/metadata_store.ex` (modify — add `merkle_tree/1` function)
- `neonfs_core/test/neon_fs/core/anti_entropy_test.exs` (new)

## Reference
- spec/metadata.md — Anti-Entropy (Background Consistency)
- spec/metadata.md — Metadata Configuration (anti_entropy.interval)
- Existing pattern: BackgroundWorker infrastructure (Phase 3) for periodic job scheduling

## Notes
The Merkle tree doesn't need to be sophisticated — a simple hash-of-hashes approach is sufficient for the number of keys per segment (likely thousands to tens of thousands, not millions). The tree is computed on demand during anti-entropy, not maintained incrementally. For large segments, the comparison can be done level-by-level: compare root hashes first (O(1)), then only descend into subtrees that differ. Anti-entropy is the last line of defence for consistency — it catches any divergence that read repair missed (e.g., keys that are never read, failed read repairs, network partitions that healed). The 6-hour default interval is conservative; in practice, read repair handles most divergence within seconds.
