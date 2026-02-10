# Task 0081: Consistent Hashing Ring

## Status
Not Started

## Phase
5 - Metadata Tiering

## Description
Implement a consistent hashing ring for distributing metadata across cluster nodes. The ring maps arbitrary keys (chunk hashes, file IDs, directory paths) to segments, and segments to replica sets. Uses SHA-256 for ring positions and virtual nodes (64 per physical node) for even distribution. The ring is a pure data structure module — no GenServer, no side effects.

## Acceptance Criteria
- [ ] New `NeonFS.Core.MetadataRing` module (pure data structure, no GenServer)
- [ ] `MetadataRing.new/2` — builds ring from node list and options (virtual_nodes_per_physical, default 64)
- [ ] `MetadataRing.locate/2` — key (binary) → `{segment_id, replica_set}` where replica_set is an ordered list of nodes
- [ ] `MetadataRing.add_node/2` — returns `{new_ring, affected_segments}` where affected_segments is the list of segments whose replica sets changed (~1/N of total)
- [ ] `MetadataRing.remove_node/2` — returns `{new_ring, affected_segments}`
- [ ] `MetadataRing.segments/1` — returns all segment IDs and their replica sets
- [ ] `MetadataRing.nodes/1` — returns all physical nodes in the ring
- [ ] `MetadataRing.segment_count/1` — returns total number of segments
- [ ] Ring positions computed via SHA-256 (consistent with existing hash usage across codebase)
- [ ] Virtual nodes ensure even distribution: standard deviation of segment counts per node < 15% of mean
- [ ] Replica set size configurable (default 3, capped at cluster size)
- [ ] Replica sets prefer distinct nodes (no node appears twice in a replica set)
- [ ] Deterministic: same inputs always produce same ring (no randomness)
- [ ] Unit tests: ring with 3 nodes, verify all keys map to valid segments with 3-node replica sets
- [ ] Unit tests: add node, verify ~1/N segments affected
- [ ] Unit tests: remove node, verify affected segments reassigned
- [ ] Unit tests: distribution evenness (virtual node spread)
- [ ] Unit tests: determinism (build ring twice, same result)

## Testing Strategy
- ExUnit tests: build ring with various node counts (1, 3, 5, 10), verify locate returns valid results
- ExUnit tests: add/remove nodes, verify minimal disruption (only ~1/N segments move)
- ExUnit tests: distribution evenness — hash 10,000 random keys, verify no node gets more than 2x its fair share
- ExUnit tests: single-node ring — replica set is just that one node
- ExUnit tests: determinism — same node list always produces identical ring
- StreamData property test: for any key, locate always returns a valid segment with correct replica count

## Dependencies
- None (parallel with task 0080)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/metadata_ring.ex` (new — consistent hashing ring data structure)
- `neonfs_core/test/neon_fs/core/metadata_ring_test.exs` (new)

## Reference
- spec/metadata.md — Consistent Hashing with Virtual Nodes
- spec/metadata.md — Metadata Configuration (virtual_nodes_per_physical, hash_algorithm)

## Notes
The ring is a pure data structure — it doesn't know about Ra or the cluster. The QuorumCoordinator (task 0084) will use it to determine routing. Segment IDs are derived from the ring position ranges, not UUIDs. The `locate/2` function walks clockwise from the key's ring position to find N distinct physical nodes. When a node is added, only segments at ring positions between the new node's virtual nodes and their predecessors are affected. The ring should be efficient to rebuild (it's rebuilt on every membership change) but doesn't need to be optimised for millions of nodes — NeonFS clusters are typically 3-20 nodes.
