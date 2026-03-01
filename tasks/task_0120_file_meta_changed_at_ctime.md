# Task 0120: Add `changed_at` Field to `FileMeta` for POSIX `ctime`

## Status
Complete

## Phase
Gap Analysis — QW-2

## Description
POSIX requires a status change time (`ctime`) that updates whenever file metadata changes — permissions, ownership, ACLs, or content. NeonFS currently has `created_at` (set once, never updated), `modified_at` (updated on content and metadata changes), and `accessed_at` (updated by `touch/1`).

Add a `changed_at` field to `FileMeta` that is updated in `update/2` whenever any metadata field changes. This provides correct POSIX `ctime` semantics. Also update the FUSE `getattr` response to include all three POSIX timestamps.

## Acceptance Criteria
- [ ] `FileMeta` struct has a new `changed_at` field of type `DateTime.t()`
- [ ] `FileMeta.new/3` initialises `changed_at` to `DateTime.utc_now()` (same as other timestamps)
- [ ] `FileMeta.update/2` sets `changed_at` to `DateTime.utc_now()` on every call
- [ ] `FileMeta.touch/1` does NOT update `changed_at` (atime-only update is not a status change)
- [ ] `created_at` remains immutable — it is the file's birth time, not POSIX `ctime`
- [ ] FUSE `getattr` response includes `mtime`, `ctime` (mapped to `changed_at`), and `atime` fields
- [ ] MetadataCodec handles the new field in serialisation/deserialisation
- [ ] Existing tests pass; new tests verify `changed_at` behaviour
- [ ] `mix format` passes

## Testing Strategy
- Unit tests in `neonfs_client/test/neon_fs/core/file_meta_test.exs`:
  - `new/3` sets `changed_at` equal to `created_at`
  - `update/2` with mode change updates `changed_at`
  - `update/2` with uid/gid change updates `changed_at`
  - `touch/1` does NOT update `changed_at`
  - `changed_at` is always >= `created_at`

## Dependencies
None — standalone fix. Other tasks (QW-6, QW-7) that modify `FileMeta` behaviour will build on this.

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/core/file_meta.ex` (modify — add field, update `new/3`, `update/2`, typespec)
- `neonfs_fuse/lib/neon_fs/fuse/handler.ex` (modify — include timestamps in `getattr` response)
- `neonfs_core/lib/neon_fs/core/metadata_codec.ex` (modify — handle `changed_at` in pack/unpack)
- `neonfs_client/test/neon_fs/core/file_meta_test.exs` (modify — add `changed_at` tests)

## Reference
- `spec/architecture.md` — POSIX semantics
- `spec/appendix.md` — POSIX compliance
- `spec/gap-analysis.md` — QW-2
- Existing pattern: `modified_at` handling in `FileMeta.update/2`

## Notes
The FUSE `getattr` response currently only returns `size` and `kind`. Adding timestamps requires the FUSE native layer to accept and pass them through. Check how the Rust `neonfs_fuse` crate's `getattr` reply is structured — it likely already has fields for `mtime`, `ctime`, `atime` that just need populating.

Be careful with the naming: POSIX `ctime` is NOT creation time — it is status change time. Our `created_at` is the birth time (equivalent to `btime` / `st_birthtim` on systems that support it). The new `changed_at` maps to POSIX `st_ctime`.
