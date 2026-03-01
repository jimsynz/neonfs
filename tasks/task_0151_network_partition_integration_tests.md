# Task 0151: Network Partition Integration Tests

## Status
Done (2026-02-22)

## Phase
Gap Analysis — M-10 (2/2)

## Description
Add integration tests that simulate network partitions and verify correct
cluster behaviour: majority continues writing, minority becomes read-only
(or fails writes), and data converges after the partition heals.

Currently only node crash tests exist (3 tests in `failure_test.exs`). No
tests exercise actual network splits where nodes remain running but cannot
communicate with each other.

## Acceptance Criteria
- [x] Test: majority partition (2 of 3 nodes) can still write data
- [x] Test: minority partition (1 of 3 nodes) cannot write (quorum loss)
- [x] Test: minority partition can still read previously written data (if local replicas exist)
- [x] Test: after partition heals, minority node receives data written during the partition
- [x] Test: after partition heals, full cluster is consistent (all nodes have same data)
- [x] Test: rolling restart — stop and restart each node sequentially, verify no data loss
- [x] Test: node restart recovery — stopped node catches up via anti-entropy after rejoin
- [x] Tests use the partition helpers from task 0150
- [x] Tests use `assert_eventually` with appropriate timeouts for async convergence
- [x] All tests tagged `@tag :partition` for selective execution
- [x] Existing failure tests still pass
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Integration tests in `neonfs_integration/test/integration/partition_test.exs` (new file):
  - Start a 3-node cluster via `init_multi_node_cluster`
  - Write initial data
  - Create partition using `PeerCluster.partition_cluster/2`
  - Verify majority/minority behaviour
  - Heal partition using `PeerCluster.heal_partition/1`
  - Wait for convergence using `assert_eventually`
  - Verify data consistency across all nodes

## Dependencies
- Task 0150 (PeerCluster partition helpers)

## Files to Create/Modify
- `neonfs_integration/test/integration/partition_test.exs` (create)

## Reference
- `spec/testing.md` lines 608–828
- `spec/node-management.md`
- `spec/gap-analysis.md` — M-10
- Existing: `neonfs_integration/test/integration/failure_test.exs` (crash test patterns)
- Existing: `neonfs_integration/test/support/cluster_case.ex` (test helpers)

## Implementation Notes
### Ra auto-restart after node restart
When a peer node is stopped (`:peer.stop`) and restarted, the RaServer GenServer
needs to auto-restart from persisted state. The auto-restart is gated by
`ClusterState.exists?()` to avoid picking up stale Ra data in test environments.

### DETS recovery after unclean shutdown
`:peer.stop` kills the VM without flushing DETS buffers, leaving Ra's `names.dets`
empty. The `try_recover_lost_directory/1` function detects when data files exist but
the directory entry is lost, pre-registers the UID (using the atom ServerName, NOT
the server_id tuple), and retries the restart.

### Anonymous functions and peer nodes
Anonymous functions defined in test modules cannot be executed on peer nodes (`:undef`
error). Always use named functions from modules loaded on all peers for Ra queries.

## Notes
Partition tests are inherently timing-sensitive. Use generous timeouts
(60+ seconds) for convergence assertions and `assert_eventually` with
polling. Ra leader election after a partition can take several seconds.

The 3-node cluster is the minimum for meaningful partition tests (2-node
clusters have no majority after a split). Consider adding a 5-node test
for more complex partition scenarios (e.g., 3-2 split) if the 3-node
tests pass reliably.

Tag all tests with `@tag :partition` so they can be run separately from
the regular test suite — partition tests are slow and may be flaky in CI
environments with resource constraints.

For the rolling restart test, use `PeerCluster.stop_node/2` then
`PeerCluster.start_node/2` (or equivalent restart function) in sequence
for each node, verifying data accessibility throughout.
