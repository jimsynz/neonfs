# Task 0186: TestCluster Cluster Initialisation and Helpers

## Status
Blocked — premature; deferring chaos testing to a later stage

## Phase
Gap Analysis — H-4 (3/7)

## Description
Add cluster initialisation logic and test helper functions to `TestCluster`.
This includes running `cluster init` and `cluster join` via the CLI inside
containers, waiting for Ra consensus to form, and providing helper functions
for common test operations (write file, read file, list volumes, etc.)
through the CLI or RPC.

## Acceptance Criteria
- [ ] `TestCluster.init_cluster/1` runs `neonfs cluster init` in the first container
- [ ] `TestCluster.join_node/2` runs `neonfs cluster join` in subsequent containers
- [ ] `TestCluster.wait_for_consensus/2` waits until Ra leader is elected (with timeout)
- [ ] `TestCluster.create_volume/2` creates a volume via CLI exec in a container
- [ ] `TestCluster.write_file/3` writes a file via CLI exec
- [ ] `TestCluster.read_file/2` reads a file via CLI exec
- [ ] `TestCluster.cluster_status/1` returns cluster status from any node
- [ ] `TestCluster.rpc/3` executes an Erlang RPC on a container node (via `neonfs remote` or `erl_call`)
- [ ] All helpers return `{:ok, result}` or `{:error, reason}` with meaningful error messages
- [ ] Helpers include stdout/stderr in error messages for debugging
- [ ] `setup_cluster/1` convenience function: start + init + join all + wait for consensus
- [ ] Unit test: `setup_cluster/1` creates a working 3-node cluster
- [ ] Unit test: write and read a file through the cluster
- [ ] Tests tagged `@tag :chaos`
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_integration/test/neonfs/integration/test_cluster_init_test.exs`:
  - Use `setup_cluster/1` to create a 3-node cluster
  - Create a volume, write a file, read it back
  - Verify data integrity
  - Clean up via `TestCluster.stop/1` in `on_exit`

## Dependencies
- Task 0185 (TestCluster container lifecycle — cluster must start before init)

## Files to Create/Modify
- `neonfs_integration/lib/neonfs/integration/test_cluster.ex` (modify — add init, join, helpers)
- `neonfs_integration/test/neonfs/integration/test_cluster_init_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-4
- `spec/testing.md` lines 750–800 (cluster initialisation for chaos tests)
- Existing: `neonfs_integration/lib/neonfs/integration/peer_cluster.ex` (helper patterns)
- Existing CLI commands: `cluster init`, `cluster join`, `cluster status`

## Notes
CLI execution inside containers uses `Container.exec/2`:

```elixir
Container.exec(container_id, ["neonfs", "cluster", "init", "--name", "test-cluster"])
```

The `rpc/3` function is needed for direct Elixir/Erlang calls that aren't
exposed via CLI. Options:
1. Use `erl_call` if available in the container
2. Use a hidden Erlang node that connects to the container's node
3. Expose an RPC endpoint in the NeonFS application

Option 2 (hidden node) is most flexible: start a hidden node in the test
process, connect to the container node via distribution, then call
`:rpc.call/4`. This requires the test host to be on the Docker network
or use port mapping.

The `setup_cluster/1` convenience function is the primary entry point for
chaos tests. It should handle all the complexity of cluster formation and
return a ready-to-use cluster handle.
