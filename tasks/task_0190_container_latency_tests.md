# Task 0190: Container Latency and Cascading Failure Tests

## Status
Blocked — premature; deferring chaos testing to a later stage

## Phase
Gap Analysis — H-4 (7/7)

## Description
Write chaos tests that use `tc netem` latency injection to test cluster
behaviour under degraded network conditions, and cascading failure scenarios
that combine multiple fault types.

## Acceptance Criteria
- [ ] Test: 200ms latency between all nodes — reads and writes still succeed (with higher latency)
- [ ] Test: 200ms latency — Ra consensus still elects leader and processes commands
- [ ] Test: 500ms latency to one node — that node becomes slow but cluster stays healthy
- [ ] Test: asymmetric latency (fast one direction, slow the other) — cluster handles correctly
- [ ] Test: latency injection then removal — operations return to normal speed
- [ ] Test: cascading failure — inject latency, then kill the slowest node — cluster recovers
- [ ] Test: cascading failure — partition 1 node, write data, kill another node — majority still has data
- [ ] Test: cascading failure — all-to-all latency + node kill — cluster degrades gracefully (no data loss)
- [ ] Test: latency during replication — new writes still replicate (eventually) after latency removed
- [ ] All tests tagged `@tag :chaos`
- [ ] All tests use `ChaosCase` template
- [ ] Tests have generous timeouts (3+ minutes for cascading tests)
- [ ] Assertions use `assert_eventually` for convergence checks
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Tests in `neonfs_integration/test/integration/chaos_latency_test.exs`:
  - Use `ChaosCase` with 3 or 5 node clusters as appropriate
  - Inject latency, perform operations, verify correctness
  - For cascading tests: inject multiple faults sequentially, verify at each stage
  - Clean up all latency injection in `on_exit`

## Dependencies
- Task 0187 (Latency injection via tc/netem)
- Task 0188 (ChaosCase template)

## Files to Create/Modify
- `neonfs_integration/test/integration/chaos_latency_test.exs` (create — latency tests)

## Reference
- `spec/gap-analysis.md` — H-4
- `spec/testing.md` lines 770–828 (latency and cascading failure scenarios)
- `spec/node-management.md` (timeout and retry behaviour under latency)

## Notes
Latency tests are inherently timing-sensitive. Use generous tolerances:
- With 200ms injected latency, expect operations to take 200-500ms
  (not exactly 200ms)
- With 500ms latency, Ra election timeout may need to be longer than
  default — verify the cluster config handles this

Cascading failure tests are the most valuable chaos tests because they
simulate realistic production failures. A common pattern:
1. Network degradation (latency)
2. Operator reaction takes time
3. Additional failure occurs before first is resolved
4. Verify the cluster doesn't suffer total data loss

Test structure for a cascading failure:

```elixir
@tag cluster_size: 5
test "latency + node kill - cluster survives", %{cluster: cluster} do
  # Setup
  {:ok, _} = TestCluster.create_volume(cluster, "test-vol")
  {:ok, _} = TestCluster.write_file(cluster, "test-vol/file1.txt", "important data")

  # First fault: inject 200ms latency between all nodes
  :ok = TestCluster.inject_latency_all(cluster, delay_ms: 200)

  # Verify: cluster still works (slowly)
  {:ok, _} = TestCluster.write_file(cluster, "test-vol/file2.txt", "during latency")

  # Second fault: kill node 3
  :ok = TestCluster.kill_node(cluster, 3)

  # Verify: cluster still works (4 of 5 nodes, with latency)
  assert_eventually(fn ->
    {:ok, _} = TestCluster.write_file(cluster, "test-vol/file3.txt", "after kill")
  end)

  # Recovery: remove latency
  :ok = TestCluster.remove_latency_all(cluster)

  # Verify: all data readable
  {:ok, content} = TestCluster.read_file(cluster, "test-vol/file1.txt")
  assert content == "important data"
end
```

The 5-node cluster for cascading tests provides more fault tolerance:
it can survive 2 simultaneous failures while maintaining quorum.
