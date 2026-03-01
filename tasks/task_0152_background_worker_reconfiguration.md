# Task 0152: BackgroundWorker Hot Reconfiguration

## Status
Complete

## Phase
Gap Analysis — M-11 (1/3)

## Description
Add a `reconfigure/1` API to `BackgroundWorker` so that worker settings
(`max_concurrent`, `max_per_minute`, `drive_concurrency`) can be changed
at runtime without restarting the process.

Currently BackgroundWorker reads configuration from application env only
at init time. Changing settings requires a process restart, which cancels
in-flight work. This task adds a `GenServer.call/2`-based reconfigure
endpoint that updates the worker's internal state.

## Acceptance Criteria
- [ ] `BackgroundWorker.reconfigure/1` accepts a keyword list of settings to update
- [ ] Supported settings: `:max_concurrent`, `:max_per_minute`, `:drive_concurrency`
- [ ] Unsupported keys are ignored (no error, just filtered out)
- [ ] Settings take effect immediately for new work items (in-flight work continues with old settings)
- [ ] `BackgroundWorker.status/0` returns current configuration alongside queue/running stats
- [ ] Reconfigure also updates application env so the values persist if the process restarts
- [ ] Telemetry event `[:neonfs, :background_worker, :reconfigured]` emitted with old and new values
- [ ] Unit test: reconfigure max_concurrent, verify new limit is respected
- [ ] Unit test: reconfigure max_per_minute, verify rate limiting changes
- [ ] Unit test: status/0 reflects new configuration after reconfigure
- [ ] Unit test: unsupported keys are silently ignored
- [ ] Existing BackgroundWorker tests still pass
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/background_worker_test.exs` (extend):
  - Start worker with defaults, reconfigure, verify new settings in status
  - Submit work, reconfigure concurrency, verify new limit
  - Verify telemetry event emission

## Dependencies
- None (BackgroundWorker already exists)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/background_worker.ex` (modify — add reconfigure/1, status/0)
- `neonfs_core/test/neon_fs/core/background_worker_test.exs` (modify — add reconfigure tests)

## Reference
- `spec/operations.md` — runtime configuration
- `spec/gap-analysis.md` — M-11
- Existing: `neonfs_core/lib/neon_fs/core/background_worker.ex`

## Notes
The reconfigure implementation is a `handle_call` that updates the relevant
fields in the GenServer state and calls `Application.put_env/3` for each
changed setting. This dual update ensures the running process uses new
values immediately and any future restart picks them up from app env.

For `max_per_minute`, the rate limiter's token bucket needs to be reset or
adjusted. Check how the current rate limiting works — if it uses a sliding
window counter, the new rate should take effect at the next window reset.

The `status/0` function returns a map with both configuration and runtime
stats:
```elixir
%{
  max_concurrent: 4,
  max_per_minute: 50,
  drive_concurrency: 1,
  queued: 12,
  running: 3,
  completed_total: 456
}
```
