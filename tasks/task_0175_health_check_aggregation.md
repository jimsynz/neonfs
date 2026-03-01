# Task 0175: Health Check Aggregation Module

## Status
Complete

## Phase
Gap Analysis — H-2 (3/6)

## Description
Create `NeonFS.Core.HealthCheck`, a module that aggregates health status
from all major subsystems into a single health report. This is used by the
`/health` HTTP endpoint (task 0176) and can be queried programmatically.

Health status is one of: `:healthy`, `:degraded`, or `:unhealthy`. The
overall status is the worst status among all checked subsystems.

## Acceptance Criteria
- [x] `NeonFS.Core.HealthCheck` module created with `check/0` returning a health report map
- [x] Report includes overall status: `:healthy`, `:degraded`, or `:unhealthy`
- [x] Report includes per-subsystem status with details:
  - `ra` — Ra cluster reachable, leader elected, this node is a member
  - `storage` — at least one drive writable, no drives in error state
  - `drives` — all registered drives accessible, per-drive status
  - `clock` — HLC clock within acceptable skew (from `ClockMonitor`)
  - `service_registry` — `ServiceRegistry` responding
  - `volumes` — volume count, any volumes with degraded redundancy
  - `cache` — `ChunkCache` running, within memory budget
- [x] Each subsystem check has a timeout (default 5 seconds) to avoid blocking
- [x] Subsystem checks run concurrently via `Task.async_stream`
- [x] If a subsystem check times out, it reports as `:unhealthy` with reason `:timeout`
- [x] Overall status logic: all healthy = `:healthy`, any degraded = `:degraded`, any unhealthy = `:unhealthy`
- [x] Report includes `checked_at` timestamp and `node` name
- [x] `check/0` is safe to call frequently (no expensive operations)
- [x] Unit test: all subsystems healthy returns `:healthy`
- [x] Unit test: one degraded subsystem returns `:degraded` overall
- [x] Unit test: one unhealthy subsystem returns `:unhealthy` overall
- [x] Unit test: timed-out subsystem reports `:unhealthy`
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/health_check_test.exs`:
  - Mock subsystem responses (start minimal GenServers that respond to health queries)
  - Test each degradation scenario
  - Test timeout handling with a slow mock

## Dependencies
- None (queries existing subsystems which already exist)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/health_check.ex` (create — health aggregation)
- `neonfs_core/test/neon_fs/core/health_check_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-2
- `spec/observability.md` — health endpoint requirements
- Existing: `neonfs_core/lib/neon_fs/core/storage_metrics.ex`
- Existing: `neonfs_core/lib/neon_fs/core/drive_registry.ex`
- Existing: `neonfs_core/lib/neon_fs/core/clock_monitor.ex`
- Existing: `neonfs_core/lib/neon_fs/core/service_registry.ex`

## Notes
The health check should be cheap enough to call every few seconds from a
load balancer or orchestrator. Avoid triggering expensive operations like
full drive scans.

Subsystem health queries should use existing status APIs where available:
- `DriveRegistry.list_drives/0` for drive status
- `StorageMetrics.get_metrics/0` for storage
- Ra's `ra:members/1` for cluster health
- `ClockMonitor` for clock skew

The health report is JSON-serialisable for the HTTP endpoint. Use plain maps
with atom keys — `Jason.encode!/1` handles these directly.

Consider degraded vs unhealthy thresholds:
- Storage 85-95% used → degraded; >95% → unhealthy
- Clock skew >100ms → degraded; >500ms → unhealthy
- One drive in error → degraded; all drives in error → unhealthy
