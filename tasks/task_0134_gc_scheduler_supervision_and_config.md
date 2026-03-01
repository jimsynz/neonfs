# Task 0134: GC Scheduler Supervision and Configuration

## Status
Complete

## Phase
Gap Analysis — M-2 (3/3)

## Description
Wire `NeonFS.Core.GCScheduler` into the core supervision tree and load its
configuration from `cluster.json`.

The scheduler must start after `JobTracker` and `StorageMetrics` (both of
which it depends on). Configuration should be loadable from `cluster.json`
under a `gc` key, following the same pattern used for `worker` config in
`Application.load_config_from_cluster_state/0`.

This task also adds the `gc` section to `Cluster.State` schema handling so
that `cluster.json` can contain:

```json
{
  "gc": {
    "interval_ms": 86400000,
    "pressure_threshold": 0.85,
    "pressure_check_interval_ms": 300000
  }
}
```

## Acceptance Criteria
- [ ] `GCScheduler` is added to `NeonFS.Core.Supervisor.build_children/0` after `StorageMetrics` and `JobTracker`
- [ ] `Application.load_config_from_cluster_state/0` loads `gc` config from `cluster.json` into app env
- [ ] `config :neonfs_core, :gc_interval_ms` configurable (default: 86_400_000)
- [ ] `config :neonfs_core, :gc_pressure_threshold` configurable (default: 0.85)
- [ ] `config :neonfs_core, :gc_pressure_check_interval_ms` configurable (default: 300_000)
- [ ] `GCScheduler` reads these config values at startup
- [ ] Missing or partial `gc` config in `cluster.json` falls back to defaults gracefully
- [ ] Existing tests still pass (no regression from supervisor change)
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Verify existing `neonfs_core` test suite passes with the new supervisor child
  (test env uses `start_children?: false` so the new child doesn't interfere)
- Unit test in `neonfs_core/test/neon_fs/core/gc_scheduler_test.exs` (extend):
  - Verify `GCScheduler` reads interval from app env
  - Verify `GCScheduler` reads pressure threshold from app env
  - Verify defaults are used when app env is not set
- Manual verification: `GCScheduler` appears in supervisor child list via
  `Supervisor.which_children(NeonFS.Core.Supervisor)`

## Dependencies
- Task 0132 (GC Scheduler GenServer)
- Task 0133 (GC Storage Pressure Trigger)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/supervisor.ex` (modify — add GCScheduler child)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — load GC config from cluster.json)
- `neonfs_core/lib/neon_fs/core/gc_scheduler.ex` (modify — read config from app env)
- `neonfs_core/test/neon_fs/core/gc_scheduler_test.exs` (modify — config tests)

## Reference
- `spec/replication.md` lines 619–630
- `spec/gap-analysis.md` — M-2
- Existing pattern: `neonfs_core/lib/neon_fs/core/application.ex` (`load_worker_config/1` for cluster.json → app env)
- Existing pattern: `neonfs_core/lib/neon_fs/core/supervisor.ex` (`build_children/0` ordering)
- Existing: `neonfs_core/lib/neon_fs/core/tiering_manager.ex` (reads config from opts/app env)

## Notes
The `GCScheduler` should be positioned in the supervisor child list after
`StorageMetrics` (which it queries for pressure) and after `JobTracker`
(which it uses to create jobs). A good position is between `TieringManager`
and `AntiEntropy`, since it's a periodic background service of similar
nature.

The `cluster.json` loading follows the existing `load_worker_config/1`
pattern: read the map from the parsed JSON, put individual values into
application env with `Application.put_env/3`. Missing keys use defaults.
This keeps the `GCScheduler` module itself clean — it reads from app env
at startup, same as `TieringManager`.

In test environment, `start_children?: false` means the scheduler won't
auto-start and interfere with tests. Tests that need it will start it
explicitly via `start_supervised!/1`.
