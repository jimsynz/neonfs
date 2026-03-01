# Task 0127: GC CLI Handler Functions

## Status
Complete

## Phase
Gap Analysis — M-1 (2/6)

## Description
Add `handle_gc_collect/1` and `handle_gc_status/0` handler functions to
`NeonFS.CLI.Handler` that the Rust CLI will call via Erlang distribution RPC.

`handle_gc_collect/1` accepts an options map (with optional `"volume"` key),
resolves the volume name to an ID, and creates a GC job via `JobTracker.create/2`.
It returns the job info map so the CLI can display the job ID and tell the user
to track progress with `neonfs job show <id>`.

`handle_gc_status/0` queries `JobTracker` for the most recent garbage-collection
jobs and returns their status. Since scheduled GC doesn't exist yet (see M-2),
this only shows previous and currently-running GC jobs.

## Acceptance Criteria
- [x] `handle_gc_collect/1` accepts a map with optional `"volume"` key (volume name string)
- [x] When `"volume"` is provided, resolves it via `VolumeRegistry.get_by_name/1` and passes `volume_id` in job params
- [x] When `"volume"` is not provided, creates a cluster-wide GC job (no volume_id in params)
- [x] Returns `{:ok, map}` with job info (id, type, status, etc.) using `job_to_map/1`
- [x] Returns `{:error, :not_found}` if the volume name doesn't exist
- [x] `handle_gc_status/0` returns `{:ok, [map]}` — list of recent GC jobs from JobTracker
- [x] GC runner (`NeonFS.Core.Job.Runners.GarbageCollection`) added to `known_runners` list in `parse_job_type_filter/2`
- [x] All functions have `@doc` and `@spec` annotations
- [x] Existing handler tests pass
- [x] `mix format` passes

## Testing Strategy
- The handler is tested indirectly through integration tests (Rust CLI → handler → JobTracker)
- Verify manually or via unit test that `handle_gc_collect(%{})` creates a job
- Verify `handle_gc_collect(%{"volume" => "nonexistent"})` returns `{:error, :not_found}`

## Dependencies
- task_0126 (GC Job Runner)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cli/handler.ex` (modify — add `handle_gc_collect/1`, `handle_gc_status/0`, update `known_runners`)

## Reference
- `spec/gap-analysis.md` — M-1
- Existing pattern: `rotate_volume_key/1` in handler.ex (job creation)
- Existing pattern: `handle_list_jobs/1` in handler.ex (job querying)
- Existing pattern: `handle_evacuate_drive/3` in handler.ex (returns job_to_map)

## Notes
The handler follows the established pattern: resolve names to IDs, delegate to
domain modules, convert results to serialisable maps.

The `known_runners` list in `parse_job_type_filter/2` must include the new
`GarbageCollection` runner so that `neonfs job list --type garbage-collection`
works.

`handle_gc_status/0` uses `JobTracker.list(type: NeonFS.Core.Job.Runners.GarbageCollection)`
to find GC jobs, sorted by most recent first.
