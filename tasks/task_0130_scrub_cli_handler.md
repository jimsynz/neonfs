# Task 0130: Scrub CLI Handler Functions

## Status
Complete

## Phase
Gap Analysis — M-1 (5/6)

## Description
Add `handle_scrub_start/1` and `handle_scrub_status/0` handler functions to
`NeonFS.CLI.Handler` that the Rust CLI will call via Erlang distribution RPC.

`handle_scrub_start/1` accepts an options map (with optional `"volume"` key),
resolves the volume name to an ID, and creates a scrub job via
`JobTracker.create/2`. It returns the job info map.

`handle_scrub_status/0` queries `JobTracker` for the most recent scrub jobs
and returns their status including corruption counts.

## Acceptance Criteria
- [ ] `handle_scrub_start/1` accepts a map with optional `"volume"` key (volume name string)
- [ ] When `"volume"` is provided, resolves via `VolumeRegistry.get_by_name/1` and passes `volume_id` in job params
- [ ] When `"volume"` is not provided, creates a full-node scrub job
- [ ] Returns `{:ok, map}` with job info using `job_to_map/1`
- [ ] Returns `{:error, :not_found}` if the volume name doesn't exist
- [ ] `handle_scrub_status/0` returns `{:ok, [map]}` — list of recent scrub jobs
- [ ] Scrub runner (`NeonFS.Core.Job.Runners.Scrub`) added to `known_runners` list in `parse_job_type_filter/2`
- [ ] All functions have `@doc` and `@spec` annotations
- [ ] Existing handler tests pass
- [ ] `mix format` passes

## Testing Strategy
- Verify manually or via unit test that `handle_scrub_start(%{})` creates a job
- Verify `handle_scrub_start(%{"volume" => "nonexistent"})` returns `{:error, :not_found}`

## Dependencies
- task_0129 (Scrub Job Runner)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cli/handler.ex` (modify — add `handle_scrub_start/1`, `handle_scrub_status/0`, update `known_runners`)

## Reference
- `spec/gap-analysis.md` — M-1
- Existing pattern: `handle_gc_collect/1` from task_0127 (same structure)
- Existing pattern: `handle_evacuate_drive/3` in handler.ex (job creation + map return)

## Notes
This follows the exact same pattern as the GC handler (task_0127). The only
differences are the runner module and the label string.

If task_0127 is implemented first, this task should follow the same structure
exactly — consistent patterns make the codebase easier to navigate.
