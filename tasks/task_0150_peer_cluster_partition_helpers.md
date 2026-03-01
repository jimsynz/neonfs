# Task 0150: PeerCluster Partition Test Helpers

## Status
Complete

## Phase
Gap Analysis — M-10 (1/2)

## Description
Add network partition simulation helpers to `PeerCluster` and `ClusterCase`
so that integration tests can create and heal partitions between nodes
without stopping them entirely.

Currently `PeerCluster.stop_node/2` is the only failure injection
primitive — it kills the node entirely. For partition tests, we need to
disconnect specific node pairs while keeping both nodes running (simulating
a network split).

Use `Node.disconnect/1` via RPC to sever the connection between specific
nodes, and `Node.connect/1` to reconnect them. Also add helpers for
verifying partition state (which nodes can see each other).

## Acceptance Criteria
- [ ] `PeerCluster.disconnect_nodes/3` disconnects node_a from node_b (bidirectional)
- [ ] `PeerCluster.reconnect_nodes/3` reconnects node_a to node_b
- [ ] `PeerCluster.partition_cluster/2` splits a cluster into two groups (disconnects all cross-group pairs)
- [ ] `PeerCluster.heal_partition/1` reconnects all nodes in the cluster
- [ ] `PeerCluster.visible_nodes/2` returns the list of connected nodes as seen from a given node
- [ ] `ClusterCase.assert_partitioned/3` verifies two groups cannot see each other
- [ ] `ClusterCase.assert_connected/2` verifies all nodes in a group can see each other
- [ ] `ClusterCase.wait_for_partition_healed/2` waits until all nodes are connected with timeout
- [ ] Partition helpers handle the case where nodes are already disconnected (idempotent)
- [ ] Reconnection helpers handle the case where nodes are already connected (idempotent)
- [ ] Unit test: disconnect and reconnect two nodes
- [ ] Unit test: partition a 3-node cluster into {1} and {2,3}, verify isolation
- [ ] Unit test: heal a partition, verify full mesh restored
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Tests in `neonfs_integration/test/integration/partition_helpers_test.exs`:
  - Start a 3-node cluster via PeerCluster
  - Disconnect node pairs, verify `visible_nodes` reflects the partition
  - Reconnect, verify full mesh
  - Test `partition_cluster` with various split configurations

## Dependencies
- None (PeerCluster already exists)

## Files to Create/Modify
- `neonfs_integration/lib/neonfs/integration/peer_cluster.ex` (modify — add partition functions)
- `neonfs_integration/test/support/cluster_case.ex` (modify — add partition assertion helpers)
- `neonfs_integration/test/integration/partition_helpers_test.exs` (create — tests)

## Reference
- `spec/testing.md` lines 608–828
- `spec/gap-analysis.md` — M-10
- Existing: `neonfs_integration/lib/neonfs/integration/peer_cluster.ex`
- Existing: `neonfs_integration/test/support/cluster_case.ex`

## Notes
`Node.disconnect/1` is unidirectional — calling it on node_a disconnects
from node_b, but node_b may not immediately notice. For a clean bidirectional
partition, call `disconnect` on both sides via RPC:

```elixir
PeerCluster.rpc(cluster, node_a, Node, :disconnect, [node_b])
PeerCluster.rpc(cluster, node_b, Node, :disconnect, [node_a])
```

After disconnection, Erlang distribution will not automatically reconnect.
Reconnection requires explicit `Node.connect/1` on at least one side.

Ra's behaviour under partition depends on Raft: the majority side elects
a leader and continues; the minority side cannot commit. After healing,
Ra's log replication brings the minority up to date. This is the behaviour
the partition tests in task 0151 will verify.
