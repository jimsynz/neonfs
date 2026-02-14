# Task 0097: Phase 7 Integration Tests

## Status
Complete

## Phase
7 - System Volume

## Description
Write integration tests that verify the system volume works correctly across a multi-node cluster. These tests use the `PeerCluster` framework to spawn real peer nodes and exercise the full lifecycle: system volume creation at cluster init, replication factor adjustment on node join, data accessibility from all nodes, guard enforcement, and survival of node failure.

## Acceptance Criteria
- [ ] New integration test file in `neonfs_integration/test/integration/system_volume_test.exs`
- [ ] Test: cluster init creates the system volume (verify via `VolumeRegistry.get_system_volume/0` on each node)
- [ ] Test: identity file (`/cluster/identity.json`) is readable from any node after init
- [ ] Test: system volume replication factor equals cluster size after all nodes have joined
- [ ] Test: `SystemVolume.write/2` on one node is readable via `SystemVolume.read/1` from another node
- [ ] Test: system volume cannot be deleted (verify `{:error, :system_volume}` from any node)
- [ ] Test: system volume is excluded from default `VolumeRegistry.list/1` output
- [ ] Test: system volume survives single node failure (stop one node, verify data still accessible from remaining nodes)
- [ ] Test: log retention prunes old files across the cluster
- [ ] All tests pass with `mix test` in the `neonfs_integration` directory
- [ ] Tests use `@moduletag timeout: 300_000` for adequate multi-node test time

## Testing Strategy
- Use `PeerCluster.start_cluster!/3` to create a 3-node cluster
- Use `PeerCluster.rpc/6` to execute operations on specific nodes
- Use `assert_eventually` / `wait_until` for eventually-consistent assertions
- Follow existing patterns from `neonfs_integration/test/integration/replication_test.exs` and `failure_test.exs`

### Test Scenarios

1. **System volume lifecycle**: Init cluster on node1, verify system volume exists on node1, join node2 and node3, verify system volume visible on all nodes, verify replication factor is 3
2. **Cross-node read/write**: Write a file to the system volume from node1, read it from node2 and node3, verify content matches
3. **Guard enforcement**: Attempt to delete system volume from node2 (not the init node), verify rejection
4. **Identity file**: Read `/cluster/identity.json` from each node, verify all return identical content with correct fields
5. **Node failure resilience**: Stop node3, verify system volume data is still readable from node1 and node2
6. **List filtering**: Create a user volume and verify `list/1` returns only the user volume, `list(include_system: true)` returns both

## Dependencies
- task_0092 (Volume struct + VolumeRegistry guards)
- task_0093 (SystemVolume access API)
- task_0094 (Cluster init system volume creation)
- task_0095 (Node join replication adjustment)
- task_0096 (Log retention)

## Files to Create/Modify
- `neonfs_integration/test/integration/system_volume_test.exs` (new — integration tests)

## Reference
- spec/system-volume.md — full specification
- Existing pattern: `neonfs_integration/test/integration/replication_test.exs` for multi-node test structure
- Existing pattern: `neonfs_integration/test/integration/failure_test.exs` for node failure testing
- Existing pattern: `neonfs_integration/test/support/cluster_case.ex` for `wait_until`, `assert_eventually`

## Notes
These integration tests are the final verification that all Phase 7 components work together in a realistic multi-node environment. They complement the unit tests in individual task files (0092-0096) which test components in isolation.

The test for log retention should write files with dates in the past, call `Retention.prune/0` via RPC, and verify the old files are gone while recent files remain. This exercises the full path through SystemVolume and VolumeRegistry in a cluster context.

Node failure resilience is important because the system volume stores cluster-critical data (identity, and in future phases, CA keys). The test should verify that losing a minority of nodes does not impact system volume availability.
