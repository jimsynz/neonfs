# Task 0031: Ra Consensus Integration Setup

## Status
Complete

## Phase
2 - Clustering

## Description
Integrate the Ra library for Raft-based consensus. This provides the foundation for distributed metadata storage across the cluster. Ra will manage cluster membership, volume definitions, and chunk location data.

## Acceptance Criteria
- [ ] Add Ra dependency to neonfs_core
- [ ] Configure Ra cluster for NeonFS metadata
- [ ] Start Ra as part of supervision tree
- [ ] Basic Ra state machine for testing
- [ ] Single-node Ra cluster works
- [ ] Ra data persisted to disk
- [ ] Ra metrics exposed via telemetry

## Ra Configuration
```elixir
# Ra machine configuration
config :ra,
  data_dir: "/var/lib/neonfs/ra"

# In supervision tree
{:ra, name: :neonfs_meta, machine: NeonFS.Core.MetadataStateMachine}
```

## Testing Strategy
- Unit tests:
  - Start Ra on single node
  - Apply commands to state machine
  - Read state back
  - Restart and verify persistence
- Multi-node tests deferred to task_0033

## Dependencies
- task_0026_elixir_supervision_tree

## Files to Create/Modify
- `neonfs_core/mix.exs` (add Ra dependency)
- `neonfs_core/lib/neon_fs/core/metadata_state_machine.ex` (new)
- `neonfs_core/lib/neon_fs/core/ra_supervisor.ex` (new)
- `neonfs_core/test/neon_fs/core/ra_test.exs` (new)

## Reference
- spec/metadata.md - Ra Cluster section
- spec/architecture.md - Ra Cluster (Cluster-Wide State)
- Ra documentation: https://github.com/rabbitmq/ra

## Notes
Ra is complex. This task focuses on getting it running; actual distributed operations come in subsequent tasks.
