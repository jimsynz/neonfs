# Task 0174: Telemetry Poller for Gauge Emissions

## Status
Complete

## Phase
Gap Analysis — H-2 (2/6)

## Description
Set up a `telemetry_poller` to periodically emit gauge-type metrics that
represent point-in-time system state. Unlike counters and histograms which
are emitted at event time, gauges (queue sizes, utilisation percentages,
connection counts) must be sampled periodically.

## Acceptance Criteria
- [x] `:telemetry_poller` added to `neonfs_core/mix.exs` dependencies (if not already present)
- [x] `NeonFS.Core.TelemetryPoller` module created with poller configuration
- [x] Poller emits storage utilisation gauges every 15 seconds: `neon_fs.storage.bytes_used`, `neon_fs.storage.bytes_free` per drive
- [x] Poller emits cache size gauge: `neon_fs.cache.size_bytes`, `neon_fs.cache.entry_count`
- [x] Poller emits cluster node count: `neon_fs.cluster.nodes`
- [x] Poller emits Ra state: `neon_fs.cluster.ra_term`, `neon_fs.cluster.ra_leader` (1 if this node is leader, 0 otherwise)
- [x] Poller emits background worker queue depth: `neon_fs.worker.queue_depth` per priority
- [x] Poller emits drive power state: `neon_fs.storage.drive_state` (0=standby, 1=active) per drive
- [x] Poll interval configurable via application config (default 15_000 ms)
- [x] Poller queries are lightweight (ETS lookups, not full scans)
- [x] Poller handles unavailable subsystems gracefully (e.g. DriveRegistry not started yet)
- [x] Unit test: poller emits expected telemetry events
- [x] Unit test: poller survives when queried subsystem is down
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/telemetry_poller_test.exs`:
  - Start poller with short interval (100ms)
  - Attach telemetry handler to capture events
  - Wait for at least one poll cycle
  - Verify expected events were emitted with correct measurements
  - Test with subsystem not started (should not crash)

## Dependencies
- Task 0173 (Metric definitions — gauge metrics must be defined to be collected)

## Files to Create/Modify
- `neonfs_core/mix.exs` (modify — add `:telemetry_poller` if needed)
- `neonfs_core/lib/neon_fs/core/telemetry_poller.ex` (create — poller configuration and measurement functions)
- `neonfs_core/test/neon_fs/core/telemetry_poller_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-2
- `spec/observability.md` — gauge metrics
- `:telemetry_poller` hex docs
- Existing: `neonfs_core/lib/neon_fs/core/storage_metrics.ex` (storage data source)
- Existing: `neonfs_core/lib/neon_fs/core/chunk_cache.ex` (cache data source)
- Existing: `neonfs_core/lib/neon_fs/core/drive_registry.ex` (drive data source)

## Notes
The poller measurement functions should be defined as module functions
(not anonymous) for clarity in telemetry handler output:

```elixir
:telemetry_poller.start_link(
  measurements: [
    {NeonFS.Core.TelemetryPoller, :measure_storage, []},
    {NeonFS.Core.TelemetryPoller, :measure_cache, []},
    {NeonFS.Core.TelemetryPoller, :measure_cluster, []}
  ],
  period: 15_000
)
```

Each measurement function calls `:telemetry.execute/3` directly. Keep each
function focused and resilient — wrap in `try/rescue` to avoid crashing the
poller if a subsystem is temporarily unavailable.
