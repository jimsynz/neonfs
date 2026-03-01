# Task 0177: Metrics Supervision and Configuration

## Status
Complete

## Phase
Gap Analysis — H-2 (5/6)

## Description
Wire the metrics HTTP server, telemetry poller, and Prometheus core into the
`neonfs_core` supervision tree. Add application configuration for metrics
settings and ensure clean startup/shutdown.

## Acceptance Criteria
- [x] `NeonFS.Core.MetricsSupervisor` defined as a `Supervisor`
- [x] Children: Prometheus core reporter, telemetry poller, Bandit HTTP server
- [x] Prometheus core reporter started with metric definitions from `NeonFS.Core.Telemetry.metrics/0`
- [x] Telemetry poller started with measurement functions from `NeonFS.Core.TelemetryPoller`
- [x] Bandit started with `NeonFS.Core.MetricsPlug` on configured port
- [x] `MetricsSupervisor` added as a child of the core application supervisor
- [x] Metrics can be disabled entirely via `config :neonfs_core, :metrics_enabled, false`
- [x] When disabled, `MetricsSupervisor` is not started (no port binding)
- [x] Configuration documented in `config/config.exs` with commented examples
- [x] `cluster.json` integration: metrics port and enabled flag readable from cluster config
- [x] Clean shutdown: Bandit stops accepting connections before application stops
- [x] Unit test: supervisor starts all children
- [x] Unit test: metrics disabled — supervisor not started
- [x] Unit test: custom port configuration works
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/metrics_supervisor_test.exs`:
  - Start supervisor with default config, verify all children running
  - Start with `metrics_enabled: false`, verify supervisor not started
  - Start with custom port, verify Bandit binds to correct port
  - Verify clean shutdown (no lingering processes)

## Dependencies
- Task 0174 (Telemetry poller — supervised child)
- Task 0176 (HTTP metrics endpoint — supervised child)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/metrics_supervisor.ex` (create — supervision tree)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add MetricsSupervisor to children)
- `neonfs_core/config/config.exs` (modify — add metrics configuration)
- `neonfs_core/test/neon_fs/core/metrics_supervisor_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-2
- `spec/observability.md` — configuration requirements
- Existing: `neonfs_core/lib/neon_fs/core/application.ex` (supervision tree)
- `telemetry_metrics_prometheus_core` — reporter startup

## Notes
The supervision tree structure:

```
NeonFS.Core.MetricsSupervisor (:one_for_one)
├── TelemetryMetricsPrometheus.Core (reporter)
├── :telemetry_poller (gauge measurements)
└── Bandit (HTTP server for /metrics and /health)
```

The metrics supervisor should be one of the last children started in the
application supervisor, since it queries other subsystems for health and
gauge measurements.

In test configuration (`config/test.exs`), metrics should be disabled by
default to avoid port conflicts when running tests in parallel. Individual
test modules that need metrics can start the supervisor manually.
