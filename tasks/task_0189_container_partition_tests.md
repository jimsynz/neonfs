# Task 0189: Container Partition and Failure Tests

## Status
Blocked — premature; deferring chaos testing to a later stage

## Phase
Gap Analysis — H-4 (6/7)

## Description
Write chaos tests that use container pause/unpause to simulate true network
partitions and container kill to simulate node crashes. These tests verify
the cluster's behaviour under real failure conditions.

## Acceptance Criteria
- [ ] Test: pause minority (1 of 3 nodes) — majority continues accepting writes
- [ ] Test: pause minority — paused node's reads are unavailable
- [ ] Test: unpause minority — partitioned node recovers and catches up
- [ ] Test: pause majority (2 of 3 nodes) — remaining node cannot write (no quorum)
- [ ] Test: unpause majority — writes resume, data is consistent
- [ ] Test: kill one node — cluster continues operating
- [ ] Test: killed node replaced (new container) — joins cluster and receives data
- [ ] Test: sequential node kills (rolling restart) — cluster remains available throughout
- [ ] Test: data written during partition is readable after healing
- [ ] Test: concurrent writes during partition — no data loss after healing
- [ ] All tests tagged `@tag :chaos`
- [ ] All tests use `ChaosCase` template
- [ ] Tests have generous timeouts (2+ minutes per test)
- [ ] Assertions use `assert_eventually` for convergence checks
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Tests in `neonfs_integration/test/integration/chaos_partition_test.exs`:
  - Use `ChaosCase` with default 3-node cluster
  - Each test creates a volume, writes initial data, then introduces failure
  - After recovery, verify data integrity by reading back all written data
  - Use `assert_eventually` with 30-second timeout for convergence

## Dependencies
- Task 0188 (ChaosCase template)

## Files to Create/Modify
- `neonfs_integration/test/integration/chaos_partition_test.exs` (create — partition tests)

## Reference
- `spec/gap-analysis.md` — H-4
- `spec/testing.md` lines 608–680 (partition test scenarios)
- `spec/node-management.md` (partition recovery behaviour)
- Existing: `neonfs_integration/test/integration/failure_test.exs` (peer-based failure tests)

## Notes
Container pause is a true network partition: the container's processes are
frozen via cgroups, so TCP connections time out and the node appears
completely unreachable. This is more realistic than `:net_kernel` disconnect
because it simulates real network failure (no clean disconnect message).

Test structure for a partition test:

```elixir
test "minority partition - majority continues writing", %{cluster: cluster} do
  # Setup: create volume and write initial file
  {:ok, _} = TestCluster.create_volume(cluster, "test-vol")
  {:ok, _} = TestCluster.write_file(cluster, "test-vol/file1.txt", "initial")

  # Partition: pause node 3 (minority)
  :ok = TestCluster.pause_node(cluster, 3)

  # Verify: majority (nodes 1, 2) can still write
  {:ok, _} = TestCluster.write_file(cluster, "test-vol/file2.txt", "during-partition")

  # Heal: unpause node 3
  :ok = TestCluster.unpause_node(cluster, 3)

  # Verify: all data readable from any node
  assert_eventually(fn ->
    {:ok, content} = TestCluster.read_file_from(cluster, 3, "test-vol/file2.txt")
    assert content == "during-partition"
  end, timeout: 30_000)
end
```

The "concurrent writes during partition" test is particularly important: it
verifies that writes accepted by the majority partition are not lost when
the minority rejoins.

The "rolling restart" test verifies zero-downtime upgrades: kill node 1,
wait for recovery, kill node 2, wait for recovery, kill node 3, wait for
recovery. At no point should the cluster become unavailable.
