# Task 0146: HLC and Quorum Coordinator Property Tests

## Status
Complete

## Phase
Gap Analysis — M-8 (2/4)

## Description
Add property-based tests for the Hybrid Logical Clock (HLC) and Quorum
Coordinator modules. The spec (`spec/testing.md` lines 290–336) defines
specific properties that must hold: HLC transitivity, antisymmetry, tick
advancement, and quorum consistency invariants.

The existing `hlc_test.exs` has property-like tests using manual loops but
doesn't use `ExUnitProperties`. This task replaces those with proper
`StreamData`-driven property tests and adds quorum coordinator properties.

## Acceptance Criteria
- [x] HLC property: timestamps are strictly monotonically increasing (`send/1` always advances)
- [x] HLC property: `compare/2` is antisymmetric (`compare(a, b) == :gt` implies `compare(b, a) == :lt`)
- [x] HLC property: `compare/2` is transitive (`a > b` and `b > c` implies `a > c`)
- [x] HLC property: `receive_event/2` produces a timestamp greater than both inputs
- [x] HLC property: physical clock component never exceeds wall clock by more than max drift
- [x] Quorum property: for any `n`, `r`, `w` where `r + w > n`, reads always see the latest write
- [x] Quorum property: concurrent writes with different HLC timestamps produce a deterministic winner
- [x] Quorum property: partial failure (up to `n - w` nodes down) doesn't prevent writes
- [x] Quorum property: partial failure (up to `n - r` nodes down) doesn't prevent reads
- [x] All property tests use `use ExUnitProperties` and `property` blocks
- [x] At least 100 iterations per property (StreamData default)
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Property tests in `neonfs_core/test/neon_fs/core/hlc_property_test.exs`:
  - Generate random HLC timestamps, verify ordering properties
  - Generate random event sequences, verify monotonicity
  - Generate pairs of timestamps, verify antisymmetry
- Property tests in `neonfs_core/test/neon_fs/core/quorum_coordinator_property_test.exs`:
  - Generate random quorum configurations (n, r, w with r+w>n)
  - Simulate write then read with various failure patterns
  - Verify read-after-write consistency holds for all valid configs

## Dependencies
- Task 0145 (stream_data dependency and generators)

## Files to Create/Modify
- `neonfs_core/test/neon_fs/core/hlc_property_test.exs` (create)
- `neonfs_core/test/neon_fs/core/quorum_coordinator_property_test.exs` (create)

## Reference
- `spec/testing.md` lines 290–336
- `spec/gap-analysis.md` — M-8
- Existing: `neonfs_core/lib/neon_fs/core/hlc.ex`
- Existing: `neonfs_core/lib/neon_fs/core/quorum_coordinator.ex`
- Existing: `neonfs_core/test/neon_fs/core/hlc_test.exs` (manual property-like tests)

## Notes
The quorum property tests need a way to simulate partial failures. Rather
than starting real nodes, mock the quorum coordinator's RPC calls to
simulate node responses and failures. Use generators for the response
patterns:

```elixir
def node_responses(n) do
  list_of(one_of([constant(:ok), constant({:error, :timeout})]), length: n)
end
```

For HLC tests, use `StreamData.bind/2` to generate sequences of operations
(send, receive_event) and verify invariants hold across the entire sequence.
