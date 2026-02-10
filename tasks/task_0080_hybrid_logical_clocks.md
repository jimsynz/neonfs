# Task 0080: Hybrid Logical Clocks

## Status
Complete

## Phase
5 - Metadata Tiering

## Description
Implement Hybrid Logical Clocks (HLC) for conflict resolution in the leaderless quorum metadata system. HLC combines wall clock time with a logical counter and node ID, ensuring monotonically increasing timestamps even under clock skew. Also implement a ClockMonitor GenServer that periodically probes cluster nodes for clock alignment and quarantines nodes with excessive skew.

## Acceptance Criteria
- [ ] New `NeonFS.Core.HLC` module (pure functional, no GenServer)
- [ ] HLC timestamp type: `{wall_ms, counter, node_id}` — wall clock in milliseconds, logical counter, originating node
- [ ] `HLC.new/1` — creates initial HLC state for a given node_id
- [ ] `HLC.now/1` — generates a new timestamp, advancing the state; bounds wall time to prevent runaway timestamps (max `wall + max_clock_skew_ms`)
- [ ] `HLC.receive_timestamp/2` — incorporates a remote timestamp; returns `{:error, :clock_skew_detected, skew_ms}` if remote wall time exceeds local by more than `max_clock_skew_ms` (default 1000ms)
- [ ] `HLC.compare/2` — compares two HLC timestamps: returns `:gt`, `:lt`, or `:eq` (wall time first, then counter, then node_id for deterministic tiebreak)
- [ ] `HLC.merge/2` — returns the higher of two timestamps (for read repair, anti-entropy)
- [ ] `HLC.to_binary/1` and `HLC.from_binary/1` — compact serialisation for storage
- [ ] New `NeonFS.Core.ClockMonitor` GenServer
- [ ] ClockMonitor probes all active cluster nodes periodically (default 30s interval)
- [ ] Probe uses round-trip compensation: `skew = abs(remote_time - (t1 + (t2 - t1) / 2))`
- [ ] Skew thresholds: warning (200ms), critical (500ms), quarantine (1000ms)
- [ ] Quarantined nodes excluded from quorum writes but can still serve reads
- [ ] Telemetry events: `[:neonfs, :clock, :skew]` with `%{skew_ms: N}` and `%{node: node}`
- [ ] Telemetry events: `[:neonfs, :clock, :quarantine]` when a node is quarantined
- [ ] ClockMonitor follows existing GenServer pattern in ChunkAccessTracker (trap_exit, ETS state)
- [ ] Unit tests for HLC: monotonicity, receive_timestamp advancement, skew rejection
- [ ] Unit tests for HLC: compare ordering, merge semantics, serialisation round-trip
- [ ] Unit tests for ClockMonitor: probe logic, threshold classification

## Testing Strategy
- ExUnit tests for HLC pure functions: generate timestamps, verify monotonicity across calls
- ExUnit tests: receive_timestamp with various skew values, verify correct advancement or rejection
- ExUnit tests: compare with identical wall times (counter tiebreak), identical counters (node_id tiebreak)
- ExUnit tests: merge always returns the higher timestamp
- ExUnit tests: binary serialisation round-trip
- ExUnit tests for ClockMonitor: mock RPC to simulate various skew levels, verify threshold classification
- StreamData property test: generating N timestamps always yields a sorted sequence

## Dependencies
- None (first Phase 5 task, independent)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/hlc.ex` (new — HLC timestamp module, pure functional)
- `neonfs_core/lib/neon_fs/core/clock_monitor.ex` (new — GenServer for cluster clock skew monitoring)
- `neonfs_core/test/neon_fs/core/hlc_test.exs` (new)
- `neonfs_core/test/neon_fs/core/clock_monitor_test.exs` (new)

## Reference
- spec/metadata.md — Conflict Resolution: Hybrid Logical Clocks
- spec/metadata.md — Clock Skew Bounds
- spec/metadata.md — Cluster-Wide Clock Monitoring
- spec/metadata.md — Metadata Configuration (HLC section)
- Existing pattern: `NeonFS.Core.ChunkAccessTracker` GenServer (ETS, trap_exit, periodic work)

## Notes
HLC correctness depends on bounded clock skew between nodes. The `max_clock_skew_ms` default of 1000ms is generous — with NTP, most nodes should be within 10-50ms. The `now/1` function must prevent runaway timestamps: if the last timestamp's wall component is far ahead of current wall time (e.g., after a clock correction), it bounds the effective wall time to `wall + max_clock_skew_ms`. The ClockMonitor should use `NeonFS.Core.ServiceRegistry` to discover active nodes. Quarantine state should be stored in a local ETS table and checked by the QuorumCoordinator before accepting writes from a node.
