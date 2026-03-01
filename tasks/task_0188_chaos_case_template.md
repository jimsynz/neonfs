# Task 0188: ChaosCase ExUnit Template

## Status
Blocked — premature; deferring chaos testing to a later stage

## Phase
Gap Analysis — H-4 (5/7)

## Description
Create a `ChaosCase` ExUnit case template (analogous to `ClusterCase` for
peer-based tests) that provides standardised setup and teardown for
container-based chaos tests. The template handles cluster creation, health
verification, and guaranteed cleanup.

## Acceptance Criteria
- [ ] `NeonFS.Integration.ChaosCase` module created using `ExUnit.CaseTemplate`
- [ ] `setup` callback creates a cluster via `TestCluster.setup_cluster/1`
- [ ] Cluster size configurable via `@tag cluster_size: 5` (default 3)
- [ ] `setup` waits for all nodes to be healthy before yielding to test
- [ ] `on_exit` callback guarantees cleanup: stops containers, removes network
- [ ] Cluster handle available in test context as `%{cluster: cluster}`
- [ ] Helper functions imported into test modules: `pause_node/2`, `unpause_node/2`, `kill_node/2`, `inject_latency/4`
- [ ] `assert_eventually/2` helper: retries an assertion with timeout (for eventually-consistent operations)
- [ ] `assert_healthy/1` helper: verifies all nodes report healthy
- [ ] `assert_degraded/1` helper: verifies at least one node is unhealthy/unreachable
- [ ] All chaos tests tagged `@tag :chaos` by default (via `@moduletag`)
- [ ] Tests excluded from default `mix test` run (require `--include chaos`)
- [ ] ExUnit configuration updated in `test_helper.exs` to exclude `:chaos` by default
- [ ] Unit test: ChaosCase setup creates a working cluster
- [ ] Unit test: ChaosCase teardown cleans up even if test fails
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_integration/test/neonfs/integration/chaos_case_test.exs`:
  - Write a minimal test using `ChaosCase` that starts a cluster and verifies health
  - Write a test that intentionally fails, verify cleanup still happens
  - Tag all tests with `@tag :chaos`

## Dependencies
- Task 0186 (TestCluster cluster initialisation — setup_cluster/1 must work)

## Files to Create/Modify
- `neonfs_integration/lib/neonfs/integration/chaos_case.ex` (create — case template)
- `neonfs_integration/test/test_helper.exs` (modify — exclude :chaos by default)
- `neonfs_integration/test/neonfs/integration/chaos_case_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-4
- `spec/testing.md` lines 800–828 (ChaosCase template)
- Existing: `neonfs_integration/lib/neonfs/integration/cluster_case.ex` (pattern reference)
- ExUnit.CaseTemplate documentation

## Notes
The `ChaosCase` template structure:

```elixir
defmodule NeonFS.Integration.ChaosCase do
  use ExUnit.CaseTemplate

  setup context do
    cluster_size = Map.get(context, :cluster_size, 3)
    {:ok, cluster} = TestCluster.setup_cluster(nodes: cluster_size)

    on_exit(fn ->
      TestCluster.stop(cluster)
    end)

    {:ok, cluster: cluster}
  end
end
```

Test modules use it:

```elixir
defmodule MyPartitionTest do
  use NeonFS.Integration.ChaosCase
  @moduletag :chaos

  @tag cluster_size: 5
  test "majority partition continues writing", %{cluster: cluster} do
    # ...
  end
end
```

The `assert_eventually/2` helper is critical for chaos tests because
operations like partition healing and re-election take time:

```elixir
def assert_eventually(assertion_fn, opts \\ []) do
  timeout = Keyword.get(opts, :timeout, 30_000)
  interval = Keyword.get(opts, :interval, 500)
  # retry until assertion passes or timeout
end
```

Excluding `:chaos` from default test runs is important because these tests
are slow (container startup) and require a container runtime. They should
be run explicitly with `mix test --include chaos`.
