# Task 0046: Multi-Drive BlobStore

## Status
Complete

## Phase
3 - Policies, Tiering, and Compression

## Description
Refactor the BlobStore GenServer to manage multiple NIF store handles — one per configured physical drive. A node with 2 NVMes and 8 SATA disks gets 10 separate `Native.store_open/2` calls and 10 handles. No Rust changes are needed. Drive configuration is required (no single-base_dir fallback). The state changes from a single handle to a `%{drive_id => handle}` map, and all operations gain an explicit `drive_id` parameter.

## Acceptance Criteria
- [x] BlobStore `init/1` reads drive config from application environment
- [x] Each configured drive gets its own `Native.store_open/2` call
- [x] State holds `%{stores: %{drive_id => handle}, drives: %{drive_id => drive_config}}`
- [x] `write_chunk/4` accepts `(data, drive_id, tier, opts)` — the old 3-arity form is removed
- [x] `read_chunk/3` accepts `(hash, drive_id, opts)`
- [x] `delete_chunk/2` accepts `(hash, drive_id)`
- [x] `migrate_chunk/4` accepts `(hash, drive_id, from_tier, to_tier)` — local migration within one drive
- [x] `list_drives/0` returns configured drives and their status
- [x] Drive config requires at least one drive; startup fails with clear error if no drives configured
- [x] Drive config shape: `%{id: string, path: string, tier: atom, capacity: integer}`
- [x] Tier subdirectories remain within drives (e.g. `{drive_path}/blobs/hot/...`)
- [x] All existing BlobStore callers updated to pass `drive_id`
- [x] All existing tests updated for new API
- [x] New tests for multi-drive scenarios (2+ drives, operations routed correctly)
- [x] Telemetry events include `drive_id` in metadata

## Testing Strategy
- Unit tests with multiple tmp dirs simulating multiple drives
- Test that writes to different drive_ids end up in correct directories
- Test startup failure when no drives configured
- Test that each drive gets its own independent NIF handle
- Integration: existing write/read path tests pass with single-drive config

## Dependencies
- None (can run in parallel with task_0045)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/blob_store.ex` (modify — multi-handle state, new API)
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — pass drive_id)
- `neonfs_core/lib/neon_fs/core/replication.ex` (modify — pass drive_id)
- `neonfs_core/lib/neon_fs/core/chunk_fetcher.ex` (modify — pass drive_id for reads)
- `neonfs_core/test/neon_fs/core/blob_store_test.exs` (modify — update all tests)
- `neonfs_core/config/config.exs` (modify — add drive config example)

## Reference
- spec/architecture.md — Storage layer design
- spec/implementation.md — Phase 3: Multi-drive storage

## Notes
The key insight is that `Native.store_open/2` already supports being called multiple times with different paths. Each call returns an independent handle. No Rust-side changes are needed — this is purely an Elixir-level refactor of how handles are managed. The `"default"` drive_id pattern used throughout the codebase must be found and replaced with actual drive_id references.
