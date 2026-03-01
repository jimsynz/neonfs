# Task 0178: Alerting Rules and FUSE Metrics

## Status
Complete

## Phase
Gap Analysis — H-2 (6/6)

## Description
Two final pieces for the observability stack:

1. **Alerting rule definitions**: Create a Prometheus alerting rules file
   that can be loaded into Prometheus AlertManager. Defines thresholds for
   storage pressure, clock skew, replication lag, and node health.

2. **FUSE node metrics**: Add a lightweight metrics endpoint to `neonfs_fuse`
   so Prometheus can scrape FUSE-specific metrics (operation latencies,
   cache hit rates) separately from the core node.

## Acceptance Criteria
- [ ] `deploy/prometheus/alerts.yml` created with alerting rules in Prometheus format
- [ ] Alert: `NeonFSStorageCritical` — fires when any node exceeds 95% storage for 5 minutes
- [ ] Alert: `NeonFSStorageWarning` — fires when any node exceeds 85% storage for 15 minutes
- [ ] Alert: `NeonFSClockSkew` — fires when clock skew exceeds 500ms
- [ ] Alert: `NeonFSNodeDown` — fires when a node's health endpoint is unreachable for 2 minutes
- [ ] Alert: `NeonFSReplicationLag` — fires when replication queue depth exceeds 1000 for 5 minutes
- [ ] Alert: `NeonFSDriveFailed` — fires when a drive reports error state
- [ ] Alert: `NeonFSCacheEvictionHigh` — fires when cache eviction rate exceeds 100/s for 10 minutes
- [ ] `telemetry_metrics_prometheus_core` added to `neonfs_fuse/mix.exs` dependencies
- [ ] `bandit` and `plug` added to `neonfs_fuse/mix.exs` dependencies
- [ ] `NeonFS.FUSE.MetricsPlug` serves `GET /metrics` on configurable port (default 9569)
- [ ] FUSE metrics include: operation durations, operation counts by type, cache hit/miss, metadata cache size
- [ ] FUSE metrics endpoint disabled by default (enabled via config)
- [ ] Unit test: alerts.yml is valid YAML
- [ ] Unit test: FUSE /metrics returns Prometheus format
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Parse `deploy/prometheus/alerts.yml` in a test to verify valid YAML structure
- Unit tests in `neonfs_fuse/test/neon_fs/fuse/metrics_plug_test.exs`:
  - Use `Plug.Test.conn/3` to test the FUSE metrics endpoint
  - Verify response format and content type

## Dependencies
- Task 0177 (Metrics supervision — core metrics infrastructure must be working first)

## Files to Create/Modify
- `deploy/prometheus/alerts.yml` (create — alerting rules)
- `neonfs_fuse/mix.exs` (modify — add metrics dependencies)
- `neonfs_fuse/lib/neon_fs/fuse/telemetry.ex` (create — FUSE metric definitions)
- `neonfs_fuse/lib/neon_fs/fuse/metrics_plug.ex` (create — HTTP endpoint)
- `neonfs_fuse/lib/neon_fs/fuse/application.ex` (modify — optionally start metrics)
- `neonfs_fuse/test/neon_fs/fuse/metrics_plug_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-2
- `spec/observability.md` — alerting rules, FUSE metrics
- Prometheus alerting rules documentation
- Existing: `neonfs_fuse/lib/neon_fs/fuse/` (FUSE telemetry events)

## Notes
The alerting rules file is a deployment artifact, not runtime code. It's
placed in `deploy/prometheus/` alongside any future Prometheus/Grafana
configuration. Users copy it to their Prometheus config directory.

The FUSE metrics port (9569) is one above the core metrics port (9568) to
avoid conflicts when core and FUSE run on the same machine.

FUSE-specific telemetry events should already be emitted from the FUSE
handler. If any are missing, add them as part of this task.

A `deploy/prometheus/prometheus.yml` example config can also be provided
showing scrape targets for core and FUSE nodes, but this is optional
documentation rather than a requirement.
