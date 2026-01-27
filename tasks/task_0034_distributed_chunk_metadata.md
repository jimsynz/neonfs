# Task 0034: Distribute Chunk Metadata via Ra

## Status
Not Started

## Phase
2 - Clustering

## Description
Migrate chunk metadata storage from local ETS to Ra-backed distributed storage. This enables chunk location tracking across the cluster and survives node restarts.

## Acceptance Criteria
- [ ] `NeonFS.Core.ChunkIndex` uses Ra for persistence
- [ ] Chunk metadata replicated across Ra cluster
- [ ] Reads can use local Ra state (no network hop for reads)
- [ ] Writes go through Ra consensus
- [ ] Location tracking includes node information
- [ ] Query chunks by location (which chunks on which node)
- [ ] Handle Ra leader changes gracefully
- [ ] Metrics for Ra operations

## State Machine Commands
```elixir
# Commands applied through Ra
{:put_chunk, chunk_meta}
{:update_chunk_locations, hash, locations}
{:delete_chunk, hash}
{:commit_chunk, hash}
```

## Testing Strategy
- Unit tests:
  - CRUD via Ra state machine
  - Query operations
- Integration tests (3-node cluster):
  - Write chunk metadata on node 1
  - Read from node 2 and 3
  - Kill leader, verify reads still work

## Dependencies
- task_0031_ra_integration_setup
- task_0015_elixir_chunk_metadata

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_index.ex` (refactor for Ra)
- `neonfs_core/lib/neon_fs/core/metadata_state_machine.ex` (add chunk operations)
- `neonfs_core/test/neon_fs/core/chunk_index_distributed_test.exs` (new)

## Reference
- spec/metadata.md - Distributed metadata architecture
- spec/architecture.md - Ra Cluster stores chunk metadata

## Notes
Keep the ChunkIndex API stable - callers shouldn't know whether storage is local or distributed.
