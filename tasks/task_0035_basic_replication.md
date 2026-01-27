# Task 0035: Implement Basic Chunk Replication

## Status
Not Started

## Phase
2 - Clustering

## Description
Implement chunk replication across nodes. When a chunk is written, it's replicated to other nodes based on the volume's replication factor. This provides data redundancy.

## Acceptance Criteria
- [ ] `NeonFS.Core.Replication` module
- [ ] `replicate_chunk/3` - replicate to target nodes
- [ ] Select target nodes based on volume replication factor
- [ ] Transfer chunk data via Erlang distribution
- [ ] Update chunk metadata with all locations
- [ ] Handle replication failures gracefully
- [ ] Configurable write acknowledgement (local, quorum, all)
- [ ] Background replication for local-ack volumes
- [ ] Telemetry for replication operations

## Replication Flow
```elixir
def write_with_replication(data, volume, write_id) do
  # Store locally first
  {:ok, chunk_info} = BlobStore.write_chunk(data, volume.initial_tier, opts)

  # Determine replication targets
  targets = select_replication_targets(volume.durability.factor - 1)

  # Replicate based on write_ack policy
  case volume.write_ack do
    :local ->
      spawn_background_replication(chunk_info, targets)
      {:ok, chunk_info}

    :quorum ->
      quorum_replicate(chunk_info, targets)

    :all ->
      sync_replicate(chunk_info, targets)
  end
end
```

## Testing Strategy
- Unit tests:
  - Target selection logic
  - Mock replication calls
- Integration tests (3-node cluster):
  - Write with replication factor 3
  - Verify chunk exists on all 3 nodes
  - Quorum write completes with 2/3 acks

## Dependencies
- task_0034_distributed_chunk_metadata
- task_0033_node_join_flow

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/replication.ex` (new)
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (integrate replication)
- `neonfs_core/test/neon_fs/core/replication_test.exs` (new)

## Reference
- spec/replication.md - Write Flow (Replicated Volume)
- spec/replication.md - Quorum Configurations

## Notes
Erasure coding is Phase 4. This task covers simple replication only.
