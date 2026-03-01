# Task 0124: Add Volume `atime_mode` Setting and Update on Read

## Status
Complete

## Phase
Gap Analysis — QW-6

## Description
Read operations do not update `accessed_at` on files, effectively behaving as `noatime`. While this is good for performance, POSIX expects `atime` to be updated on read (or at least under `relatime` rules — update if `atime` < `mtime` or `atime` is > 24 hours old).

Add a per-volume `atime_mode` setting so operators can choose the behaviour, and implement `relatime` updates in the FUSE read handler.

## Acceptance Criteria
- [ ] `Volume` struct has a new `atime_mode` field of type `:noatime | :relatime`
- [ ] Default is `:noatime` (preserves current behaviour)
- [ ] FUSE `read` handler checks `atime_mode` on the volume
- [ ] When `:relatime`: update `accessed_at` if `accessed_at < modified_at` OR `accessed_at` is more than 24 hours old
- [ ] When `:noatime`: no `accessed_at` update on read (current behaviour)
- [ ] `relatime` updates use `FileMeta.touch/1` (no version bump, no `changed_at` update)
- [ ] CLI `volume create` and `volume update` accept `--atime-mode noatime|relatime`
- [ ] Existing tests pass
- [ ] `mix format` passes

## Testing Strategy
- Unit tests for `relatime` logic:
  - `atime` older than `mtime` → update
  - `atime` newer than `mtime` and < 24h old → no update
  - `atime` newer than `mtime` but > 24h old → update
- FUSE handler test:
  - Read on volume with `atime_mode: :noatime` → `accessed_at` unchanged
  - Read on volume with `atime_mode: :relatime` → `accessed_at` updated (when stale)

## Dependencies
- task_0120 (changed_at field) — to ensure `touch/1` doesn't accidentally update `changed_at`

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/core/volume.ex` (modify — add `atime_mode` field, default, type)
- `neonfs_fuse/lib/neon_fs/fuse/handler.ex` (modify — add relatime check in `read` handler)
- `neonfs_core/lib/neon_fs/cli/handler.ex` (modify — accept atime_mode in volume create/update)
- `cli/src/commands/volume.rs` (modify — add `--atime-mode` flag)
- `neonfs_fuse/test/neon_fs/fuse/handler_test.exs` (modify — add atime tests)

## Reference
- `spec/architecture.md` — POSIX timestamps
- `spec/gap-analysis.md` — QW-6
- Linux `relatime` mount option semantics: update atime if atime < mtime or atime > 24h old

## Notes
The `relatime` check should be fast — just compare two `DateTime` values. Avoid making an RPC call to update `accessed_at` on every read; instead, batch or coalesce atime updates. One approach: update the local `MetadataCache` immediately and flush to `FileIndex` asynchronously.

The `relatime` semantics match Linux's default mount behaviour since kernel 2.6.30, so this is the most commonly expected POSIX behaviour. Full `strictatime` (update on every read) is deliberately not offered due to the metadata write amplification it would cause in a distributed system.
