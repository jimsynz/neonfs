# Task 0062: Stripe Chunk Placement

## Status
Complete

## Phase
4 - Erasure Coding

## Description
Implement a stripe-aware chunk placement policy that distributes erasure-coded stripe chunks across different failure domains. Unlike replication (where each chunk is copied to N nodes), erasure coding creates N unique chunks and places one copy of each on a different node/drive. The placement algorithm maximises failure domain separation so that no single node or drive failure loses more chunks than the parity count can tolerate.

## Acceptance Criteria
- [ ] New `NeonFS.Core.StripePlacement` module
- [ ] `select_targets/2` — given a stripe config (`%{data_chunks, parity_chunks}`) and available nodes/drives, returns an ordered list of `%{node, drive_id}` targets (one per chunk in the stripe)
- [ ] Placement maximises failure domain separation: different nodes preferred over different drives on the same node
- [ ] When more chunks than nodes: distributes evenly across available nodes (round-robin), logs a warning that fault tolerance is reduced
- [ ] When more chunks than total drives across cluster: same as above, uses available drives
- [ ] Single-node cluster: all chunks placed on different drives if available, single drive gets all chunks with warning
- [ ] Placement considers drive tier — stripe chunks placed on the tier specified by the volume's `tiering.initial_tier`
- [ ] Placement considers drive capacity — avoids placing on drives above 90% utilisation
- [ ] `validate_placement/2` — checks that a placement meets minimum fault tolerance (at least `parity_chunks + 1` distinct failure domains), returns `:ok` or `{:warning, reason}`
- [ ] Write path (task 0060) updated to use `StripePlacement.select_targets/2` instead of basic local storage
- [ ] Chunk distribution: each stripe chunk written to its assigned target via BlobStore (local) or RPC (remote)
- [ ] ChunkMeta locations updated after distribution
- [ ] Telemetry events for placement decisions (target count, distinct nodes, warning if reduced fault tolerance)
- [ ] Unit tests for placement with various cluster topologies
- [ ] All existing tests still pass

## Testing Strategy
- Unit test: 3 nodes, 10+4 stripe — verify 14 chunks distributed across all 3 nodes
- Unit test: 14+ nodes — verify each chunk on a different node
- Unit test: 1 node, 3 drives — verify chunks spread across drives
- Unit test: 1 node, 1 drive — verify all chunks placed with warning
- Unit test: drive capacity filtering — full drives excluded from placement
- Unit test: `validate_placement/2` — passes with good spread, warns with poor spread
- Integration: write to erasure volume in multi-node cluster, verify chunks on different nodes
- Run full `mix test` in neonfs_core

## Dependencies
- task_0060 (write path creates stripes — this task refines how stripe chunks are distributed)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/stripe_placement.ex` (new — placement algorithm)
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — use StripePlacement for erasure writes)
- `neonfs_core/lib/neon_fs/core/replication.ex` (modify — add stripe chunk distribution function)
- `neonfs_core/test/neon_fs/core/stripe_placement_test.exs` (new)

## Reference
- spec/replication.md — Replica Placement, placement policy
- spec/storage-tiering.md — Replica Placement Policy algorithm
- spec/node-management.md — Failure domains

## Notes
The placement algorithm is distinct from replication's `select_replication_targets/2`. Replication selects N nodes to send copies of the *same* chunk; stripe placement selects N *distinct* targets for N *different* chunks. The goal is spreading unique data across failure domains, not redundancy. A 10+4 stripe can tolerate losing any 4 chunks — so placing 5+ chunks on one node means that node's failure could cause data loss. The warning mechanism lets operators know when their cluster topology limits fault tolerance. Future enhancement: zone-aware placement for multi-site clusters (not needed for Phase 4).
