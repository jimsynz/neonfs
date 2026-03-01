# Task 0119: FUSE `create`/`mkdir` Honour Mode Parameter

## Status
Complete

## Phase
Gap Analysis — QW-1

## Description
The FUSE handler's `create` and `mkdir` operations ignore the mode parameter sent by the kernel and hardcode `0o644` (files) and `0o755` (directories). The mode parameter is already present in `params["mode"]` but is prefixed with `_` and discarded.

Pass the kernel-supplied mode through to `FileMeta.new/3` so that applications using `open(path, O_CREAT, 0600)` or `mkdir(path, 0700)` get the permissions they requested.

## Acceptance Criteria
- [x] `create` handler reads `params["mode"]` and passes it to `write_file/4` via the `mode:` option
- [x] `mkdir` handler reads `params["mode"]` and passes it to `write_file/4` via the `mode:` option
- [x] The file-type bits (`S_IFREG` / `S_IFDIR`) are still OR'd in by the handler — callers supply permission bits only
- [x] Creating a file with mode `0o600` results in a `FileMeta` with mode `0o100600`
- [x] Creating a directory with mode `0o700` results in a `FileMeta` with mode `0o040700`
- [x] If no mode is supplied (nil), fall back to the current defaults (`0o644` / `0o755`)
- [x] Existing unit tests still pass
- [x] New unit tests verify mode passthrough for both `create` and `mkdir`

## Testing Strategy
- Unit tests in `neonfs_fuse/test/neon_fs/fuse/handler_test.exs`:
  - `create` with explicit mode `0o600` → verify `FileMeta.mode` is `0o100600`
  - `create` with nil mode → verify `FileMeta.mode` is `0o100644` (default)
  - `mkdir` with explicit mode `0o700` → verify `FileMeta.mode` is `0o040700`
  - `mkdir` with nil mode → verify `FileMeta.mode` is `0o040755` (default)

## Dependencies
None — standalone fix.

## Files to Create/Modify
- `neonfs_fuse/lib/neon_fs/fuse/handler.ex` (modify — `handle_operation` for `create` and `mkdir`)
- `neonfs_fuse/test/neon_fs/fuse/handler_test.exs` (modify — add mode passthrough tests)

## Reference
- `spec/architecture.md` — POSIX operations
- `spec/gap-analysis.md` — QW-1
- Existing pattern: `build_setattr_updates/1` in handler.ex already handles mode from `setattr`

## Notes
The kernel sends mode as an integer with permission bits only (e.g. `0o644`). The handler must OR in the file-type constant (`@s_ifreg = 0o100000` or `@s_ifdir = 0o040000`) before storing, exactly as the current hardcoded values do. The change is simply: use `params["mode"]` for the permission portion instead of the hardcoded value.
