# Task 0095: Node Join/Decommission Replication Adjustment

## Status
Not Started

## Phase
7 - System Volume

## Description
Update `Cluster.Join.accept_join/3` and the CLI handler's join flow to adjust the system volume's replication factor when core nodes join or are decommissioned. The system volume's replication factor must always equal the number of core nodes in the cluster, ensuring every core node holds a complete copy.

On node join, after Ra membership is updated, the accepting node calls `VolumeRegistry.adjust_system_volume_replication/1` with the new cluster size. On node decommission (future), the same function is called with the decremented size. Only core node joins affect the system volume — non-core nodes (FUSE, S3, etc.) are excluded.

## Acceptance Criteria
- [ ] `Cluster.Join.accept_join/3` calls `VolumeRegistry.adjust_system_volume_replication/1` after successfully adding a core node
- [ ] Replication adjustment only occurs for core node joins (non-core types like `:fuse` skip it)
- [ ] Replication factor is set to the count of core nodes in `updated_state.ra_cluster_members`
- [ ] If `adjust_system_volume_replication/1` fails, the join still succeeds (log a warning, do not abort — the system volume was already replicated at init time and will catch up via repair)
- [ ] The CLI handler's `rebuild_quorum_ring_on_all_nodes/0` call remains after the join (replication adjustment is a separate concern)
- [ ] Unit tests verify replication factor increases on core node join
- [ ] Unit tests verify replication factor is unchanged on non-core node join
- [ ] Unit tests verify join does not fail if replication adjustment fails

## Testing Strategy
- ExUnit test: mock `VolumeRegistry.adjust_system_volume_replication/1`, call `accept_join/3` with a core node, verify it was called with the correct cluster size
- ExUnit test: call `accept_join/3` with a non-core node (`:fuse`), verify `adjust_system_volume_replication/1` was not called
- ExUnit test: make `adjust_system_volume_replication/1` return an error, verify `accept_join/3` still returns `{:ok, cluster_info}`
- ExUnit test: verify the replication factor in the system volume after two sequential core node joins (1 → 2 → 3)

## Dependencies
- task_0092 (`VolumeRegistry.adjust_system_volume_replication/1`)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cluster/join.ex` (modify — add replication adjustment in `accept_join/3`)
- `neonfs_core/test/neon_fs/cluster/join_test.exs` (modify — add replication adjustment tests)

## Reference
- spec/system-volume.md — Replication Behaviour: Factor Equals Cluster Size
- spec/system-volume.md — Lifecycle: Node Join
- Existing pattern: `Cluster.Join.accept_join/3` current flow (validate, add peer, add to Ra, register service)

## Notes
Node decommission is not yet implemented (spec exists in `spec/node-management.md` but code is future work). This task only adds the join-time adjustment. When decommission is implemented in a later phase, it should follow the same pattern: call `adjust_system_volume_replication/1` with the decremented cluster size after migration completes.

The replication adjustment is intentionally non-fatal. At cluster init time the system volume has replication factor 1. If the adjustment fails on a subsequent join, the data is still safely stored on the init node and the repair system will eventually propagate copies. Aborting the join over a non-critical metadata update would be disproportionate.
