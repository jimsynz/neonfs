# Task 0173: Prometheus Dependencies and Metric Definitions

## Status
Complete

## Phase
Gap Analysis — H-2 (1/6)

## Description
Add Prometheus metric exposition dependencies to `neonfs_core` and define
all metric specifications in a central `NeonFS.Core.Telemetry` module. This
module maps the 41 existing telemetry events to Prometheus metric types
(counters, histograms, gauges).

The spec defines metrics across 7 categories: chunk operations, replication,
repair, storage, cluster, FUSE, and intent logs. Most telemetry events are
already emitted — this task bridges them to Prometheus exposition format.

## Acceptance Criteria
- [x] `telemetry_metrics_prometheus_core` added to `neonfs_core/mix.exs` dependencies
- [x] `NeonFS.Core.Telemetry` module created with `metrics/0` function returning a list of `Telemetry.Metrics` specs
- [x] Chunk operation metrics defined: `neon_fs.blob.store_chunk.duration` (histogram), `neon_fs.blob.retrieve_chunk.duration` (histogram), `neon_fs.blob.store_chunk.count` (counter), `neon_fs.blob.retrieve_chunk.count` (counter)
- [x] Replication metrics: `neon_fs.replication.chunk.duration` (histogram), `neon_fs.replication.chunk.count` (counter by result)
- [x] Repair metrics: `neon_fs.repair.chunk.duration` (histogram), `neon_fs.repair.chunk.count` (counter by result)
- [x] Storage metrics: `neon_fs.storage.bytes_used` (gauge), `neon_fs.storage.bytes_free` (gauge), `neon_fs.storage.drive_count` (gauge)
- [x] Cluster metrics: `neon_fs.cluster.nodes` (gauge), `neon_fs.cluster.ra_term` (gauge), `neon_fs.ra.command.duration` (histogram)
- [x] FUSE metrics: `neon_fs.fuse.operation.duration` (histogram by op), `neon_fs.fuse.operation.count` (counter by op)
- [x] Cache metrics: `neon_fs.cache.hit` (counter), `neon_fs.cache.miss` (counter), `neon_fs.cache.eviction` (counter), `neon_fs.cache.size_bytes` (gauge)
- [x] I/O scheduler metrics (future): placeholders for queue depth, dispatch rate
- [x] All metrics have `:description` and appropriate `:tags` (`:volume_id`, `:drive_id`, `:node` where applicable)
- [x] Histogram buckets defined appropriately (e.g. `[1, 5, 10, 25, 50, 100, 250, 500, 1000]` ms for durations)
- [x] Unit test: `metrics/0` returns a non-empty list of valid metric specs
- [x] Unit test: no duplicate metric names
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/telemetry_test.exs`:
  - Call `metrics/0` and verify it returns a list of `Telemetry.Metrics` structs
  - Verify no duplicate metric names
  - Verify each metric has a description

## Dependencies
- None

## Files to Create/Modify
- `neonfs_core/mix.exs` (modify — add `telemetry_metrics_prometheus_core` dependency)
- `neonfs_core/lib/neon_fs/core/telemetry.ex` (create — metric definitions)
- `neonfs_core/test/neon_fs/core/telemetry_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-2
- `spec/observability.md` (all 482 lines — metric definitions)
- Existing telemetry events: grep for `:telemetry.execute` across the codebase
- `telemetry_metrics_prometheus_core` hex docs

## Notes
The `telemetry_metrics_prometheus_core` package handles the Prometheus text
format rendering. It does NOT include an HTTP server — that comes in task
0176 via Bandit + Plug.

The metric definitions should be organised by category using comments or
helper functions (e.g. `chunk_metrics/0`, `cluster_metrics/0`) for
maintainability.

Tag values like `:volume_id` come from the telemetry event metadata. Ensure
the existing telemetry calls include these metadata fields — if any are
missing, note them for the caller migration.
