# Task 0037: Phase 2 Integration Test - Multi-Node Cluster

## Status
Not Started

## Phase
2 - Clustering

## Description
Create an end-to-end integration test that validates the Phase 2 milestone: "3-node cluster, data replicated, survives single node failure."

## Acceptance Criteria
- [ ] Integration test using containerised cluster (TestCluster)
- [ ] Create 3-node cluster
- [ ] Init cluster on node 1
- [ ] Join nodes 2 and 3
- [ ] Create volume with replication factor 3
- [ ] Write file, verify replicated to all nodes
- [ ] Kill one node
- [ ] Verify file still readable from surviving nodes
- [ ] Restart killed node
- [ ] Verify node rejoins and syncs

## Test Implementation
```elixir
defmodule NeonFS.Integration.Phase2Test do
  use ExUnit.Case, async: false
  alias NeonFS.TestCluster

  @moduletag :integration
  @moduletag timeout: 300_000

  setup do
    {:ok, cluster} = TestCluster.start(nodes: 3)
    on_exit(fn -> TestCluster.stop(cluster) end)
    %{cluster: cluster}
  end

  test "cluster survives single node failure", %{cluster: cluster} do
    # Init and join
    TestCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])
    token = TestCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [])
    TestCluster.rpc(cluster, :node2, NeonFS.CLI.Handler, :cluster_join, [token, :node1])
    TestCluster.rpc(cluster, :node3, NeonFS.CLI.Handler, :cluster_join, [token, :node1])

    # Create volume
    TestCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume,
      ["test", %{durability: %{type: :replicate, factor: 3}}])

    # Write data
    test_data = :crypto.strong_rand_bytes(100_000)
    TestCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file,
      ["test", "/data.bin", test_data])

    # Verify on all nodes
    for node <- [:node1, :node2, :node3] do
      {:ok, data} = TestCluster.rpc(cluster, node, NeonFS.TestHelpers, :read_file,
        ["test", "/data.bin"])
      assert data == test_data
    end

    # Kill node 3
    {:ok, cluster} = TestCluster.kill_node(cluster, :node3)
    Process.sleep(5_000)

    # Still readable
    {:ok, data} = TestCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file,
      ["test", "/data.bin"])
    assert data == test_data

    # Restart node 3
    {:ok, _cluster} = TestCluster.restart_node(cluster, :node3)
    Process.sleep(10_000)

    # Verify node 3 can read
    {:ok, data} = TestCluster.rpc(cluster, :node3, NeonFS.TestHelpers, :read_file,
      ["test", "/data.bin"])
    assert data == test_data
  end
end
```

## Testing Strategy
- Run with `mix test --only integration`
- Requires Docker/Podman for containerised nodes
- May take several minutes due to cluster operations

## Dependencies
- All Phase 2 tasks complete
- TestCluster infrastructure (from spec/testing.md)

## Files to Create/Modify
- `test/integration/phase2_test.exs` (new)
- `test/support/test_cluster.ex` (implement from spec)

## Reference
- spec/implementation.md - Phase 2 Milestone
- spec/testing.md - Integration Testing with Containerised Clusters

## Notes
This is the gate for Phase 2 completion. TestCluster implementation is a significant sub-task.
