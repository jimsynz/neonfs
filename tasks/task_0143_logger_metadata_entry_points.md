# Task 0143: Logger Metadata at Request Entry Points

## Status
Complete

## Phase
Gap Analysis — M-7 (2/3)

## Description
Add `Logger.metadata/1` calls at key request entry points so that all
downstream log messages include context fields like `:component`,
`:volume_id`, `:request_id`, and `:node_name`.

Logger metadata in Elixir is process-local and inherited by spawned tasks.
By setting metadata at the entry point of each request (CLI handler call,
FUSE operation, background job start), all subsequent log messages in that
call chain automatically include the context without modifying each
individual log call.

## Acceptance Criteria
- [ ] CLI handler functions set `Logger.metadata(component: :cli, request_id: ...)` on entry
- [ ] FUSE handler operations set `Logger.metadata(component: :fuse, volume_id: ..., request_id: ...)` on entry
- [ ] Background worker job execution sets `Logger.metadata(component: :background, job_id: ..., job_type: ...)` on entry
- [ ] ReadOperation sets `Logger.metadata(component: :read, volume_id: ..., file_path: ...)` on entry
- [ ] WriteOperation sets `Logger.metadata(component: :write, volume_id: ..., file_path: ...)` on entry
- [ ] GCScheduler and ScrubScheduler set `Logger.metadata(component: :scheduler, scheduler: ...)` on tick
- [ ] Request IDs are generated using `:crypto.strong_rand_bytes(8) |> Base.encode16()`
- [ ] Node name is set once at application startup via `Logger.metadata(node_name: node())`
- [ ] No changes to individual log message strings — only metadata additions
- [ ] Existing tests still pass
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests verifying metadata is set correctly at entry points:
  - Call a handler function, capture Logger metadata, verify expected fields
  - Start a FUSE operation, verify volume_id and component are in metadata
- Verify existing test suites pass unchanged

## Dependencies
- Task 0142 (JSON log formatter setup)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cli/handler.ex` (modify — add metadata at function entry)
- `neonfs_fuse/lib/neon_fs/fuse/handler.ex` (modify — add metadata at operation entry)
- `neonfs_core/lib/neon_fs/core/background_worker.ex` (modify — add metadata in job execution)
- `neonfs_core/lib/neon_fs/core/read_operation.ex` (modify — add metadata)
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — add metadata)
- `neonfs_core/lib/neon_fs/core/gc_scheduler.ex` (modify — add metadata on tick)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — set node_name metadata at startup)

## Reference
- `spec/operations.md` lines 200–218
- `spec/observability.md`
- `spec/gap-analysis.md` — M-7
- Elixir Logger.metadata/1 documentation

## Notes
Logger metadata is per-process and automatically inherited by `Task.async`
and `Task.Supervisor.async` calls. This means metadata set in a handler
function will propagate to any tasks spawned during that request.

For GenServer callbacks (handle_call, handle_cast, handle_info), metadata
set in `init/1` persists for the lifetime of the process. For one-off
operations (handler functions called via RPC), set metadata at the
function entry and it will be available for all log calls in that
execution.

Don't worry about cleaning up metadata after each request — Logger
metadata doesn't leak between unrelated calls because each RPC/call runs
in its own process.
