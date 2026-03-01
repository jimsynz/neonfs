# Task 0163: Migrate Core Operations to Structured Errors

## Status
Complete

## Phase
Gap Analysis — M-16 (3/4)

## Description
Migrate the core operation modules (`ReadOperation`, `WriteOperation`,
`VolumeRegistry`, and `Router`) to return `NeonFS.Error` structs instead
of ad-hoc atoms and strings. These modules are the critical path for data
operations and the most common source of errors seen by users.

This is a deeper migration than the handler (task 0162) — it converts the
internal modules themselves rather than just the boundary. After this task,
errors flow as structured types from origin to handler to CLI.

## Acceptance Criteria
- [x] `ReadOperation` error returns use appropriate error structs:
  - `:chunk_not_found` → `NeonFS.Error.ChunkNotFound`
  - `:chunk_corrupted` → `NeonFS.Error.Internal` with corruption details
  - `:all_nodes_unreachable` → `NeonFS.Error.Unavailable`
- [x] `WriteOperation` error returns use appropriate error structs:
  - `:no_drives_available` → `NeonFS.Error.Unavailable` with tier context
  - `:volume_not_found` → `NeonFS.Error.VolumeNotFound`
- [x] `VolumeRegistry` error returns use appropriate error structs:
  - `"volume with name '...' already exists"` → `NeonFS.Error.Invalid` with context
  - `"volume contains N file(s), cannot delete"` → `NeonFS.Error.Invalid` with context
  - `:system_volume` → `NeonFS.Error.Invalid` with context
  - `:reserved_name` → `NeonFS.Error.Invalid` with context
  - `:system_volume_protected` → `NeonFS.Error.Invalid` with context
- [x] `Router` error returns use appropriate error structs:
  - `:all_nodes_unreachable` → `NeonFS.Error.Unavailable`
- [x] All error structs include relevant context (volume_id, chunk_hash, node, etc.)
- [x] Modules still accept legacy error formats from lower-level dependencies
- [x] Unit tests updated to match new error structs
- [x] Existing test assertions updated from `{:error, :atom}` to `{:error, %NeonFS.Error{}}`
- [x] FUSE handler updated to match on error struct classes
- [x] Downstream consumers updated (SystemVolume, CertificateAuthority, integration tests)
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Update tests in:
  - `neonfs_core/test/neon_fs/core/read_operation_test.exs`
  - `neonfs_core/test/neon_fs/core/write_operation_test.exs`
  - `neonfs_core/test/neon_fs/core/volume_registry_test.exs`
  - `neonfs_client/test/neon_fs/client/router_test.exs`
- Verify error structs have correct class and context
- Run integration tests to verify end-to-end error propagation

## Dependencies
- Task 0161 (NeonFS.Error struct and error catalogue)
- Task 0162 (Handler error migration — to avoid double migration conflicts)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/read_operation.ex` (modify — error returns)
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — error returns)
- `neonfs_core/lib/neon_fs/core/volume_registry.ex` (modify — error returns)
- `neonfs_client/lib/neon_fs/client/router.ex` (modify — error returns)
- `neonfs_core/test/neon_fs/core/read_operation_test.exs` (modify — update assertions)
- `neonfs_core/test/neon_fs/core/write_operation_test.exs` (modify — update assertions)
- `neonfs_core/test/neon_fs/core/volume_registry_test.exs` (modify — update assertions)
- `neonfs_client/test/neon_fs/client/router_test.exs` (modify — update assertions)

## Reference
- `spec/gap-analysis.md` — M-16
- Existing error patterns in each module (see M-16 analysis in gap-analysis.md)
- Task 0161 (error struct definitions)
- Task 0162 (handler migration — complements this work)

## Notes
This migration touches the most frequently exercised code paths. Test
thoroughly after each module conversion — don't batch convert all four
modules at once.

The FUSE handler (`neonfs_fuse/lib/neon_fs/fuse/handler.ex`) also pattern
matches on error atoms from these modules. After this migration, the FUSE
handler will receive error structs instead. Update the FUSE handler's error
handling to match on error struct classes:

```elixir
# Before
{:error, :not_found} -> {:error, :enoent}

# After
{:error, %NeonFS.Error.NotFound{}} -> {:error, :enoent}
{:error, %NeonFS.Error{}} -> {:error, :eio}  # catch-all for structured errors
{:error, reason} -> {:error, :eio}            # catch-all for legacy errors
```

The catch-all ensures the FUSE handler never crashes on unexpected error
types during the migration period.
