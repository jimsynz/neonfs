# Task 0136: ScrubScheduler Supervision and Configuration

## Status
Complete

## Phase
Gap Analysis — M-3 (2/3)

## Description
Wire `NeonFS.Core.ScrubScheduler` into the core supervision tree and load
its configuration from `cluster.json`.

The scheduler must start after `JobTracker` and `VolumeRegistry` (both of
which it depends on). Configuration should be loadable from `cluster.json`
under a `scrub` key, following the same pattern used for `gc` config in
`Application.load_config_from_cluster_state/0`.

This task also adds the `scrub` section to `Cluster.State` schema handling
so that `cluster.json` can contain:

```json
{
  "scrub": {
    "check_interval_ms": 3600000
  }
}
```

## Acceptance Criteria
- [ ] `ScrubScheduler` is added to `NeonFS.Core.Supervisor.build_children/0` after `VolumeRegistry` and `JobTracker`
- [ ] `Application.load_config_from_cluster_state/0` loads `scrub` config from `cluster.json` into app env
- [ ] `config :neonfs_core, :scrub_check_interval_ms` configurable (default: 3_600_000)
- [ ] `ScrubScheduler` reads config from app env at startup
- [ ] Missing or partial `scrub` config in `cluster.json` falls back to defaults gracefully
- [ ] Existing tests still pass (no regression from supervisor change)
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Verify existing `neonfs_core` test suite passes with the new supervisor child
  (test env uses `start_children?: false` so the new child doesn't interfere)
- Unit test in `neonfs_core/test/neon_fs/core/scrub_scheduler_test.exs` (extend):
  - Verify `ScrubScheduler` reads check_interval from app env
  - Verify defaults are used when app env is not set

## Dependencies
- Task 0135 (ScrubScheduler GenServer)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/supervisor.ex` (modify — add ScrubScheduler child)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — load scrub config from cluster.json)
- `neonfs_core/lib/neon_fs/core/scrub_scheduler.ex` (modify — read config from app env)
- `neonfs_core/test/neon_fs/core/scrub_scheduler_test.exs` (modify — config tests)

## Reference
- `spec/architecture.md` lines 370–388
- `spec/gap-analysis.md` — M-3
- Existing pattern: `neonfs_core/lib/neon_fs/core/application.ex` (GC config loading)
- Existing pattern: `neonfs_core/lib/neon_fs/core/supervisor.ex` (`build_children/0` ordering)
- Task 0134 (GCScheduler supervision — same pattern)

## Notes
Position ScrubScheduler near GCScheduler in the supervisor child list — they
are sibling periodic services. In test environment, `start_children?: false`
means the scheduler won't auto-start and interfere with tests.
