# Task 0087: FileIndex and Directory Entry Migration

## Status
Not Started

## Phase
5 - Metadata Tiering

## Description
Migrate FileIndex from Ra-backed storage to the leaderless quorum system, and introduce DirectoryEntry as a new metadata type. With files sharded by file_id, directory listings require a separate structure — DirectoryEntry (sharded by parent_path) enables single-segment directory listings. File creation becomes a cross-segment operation (FileMeta + DirectoryEntry), coordinated via the IntentLog. The FileIndex public API is preserved where possible, with additions for directory operations.

## Acceptance Criteria
- [ ] New `NeonFS.Core.DirectoryEntry` struct: `%{parent_path, volume_id, children: %{name => %{type, id}}, mode, uid, gid, hlc_timestamp}`
  - `children` maps child name to `%{type: :file | :dir, id: binary}`
  - `type` distinguishes files from subdirectories
- [ ] `NeonFS.Client.FileMeta` extended with `hlc_timestamp` field (for quorum conflict resolution)
- [ ] File metadata key format: `"file:#{file_id}"` — sharded by `hash(file_id)`
- [ ] Directory entry key format: `"dir:#{volume_id}:#{parent_path}"` — sharded by `hash(parent_path)`
- [ ] `FileIndex.create/4` uses IntentLog for cross-segment atomicity:
  1. `IntentLog.try_acquire` with conflict_key `{:create, volume_id, parent_path, name}`
  2. Quorum write FileMeta to file segment
  3. Quorum write DirectoryEntry update to directory segment (add child)
  4. `IntentLog.complete`
- [ ] `FileIndex.get/1` — quorum read by file_id
- [ ] `FileIndex.get_by_path/3` — parse path → quorum read DirectoryEntry → get child id → quorum read FileMeta
- [ ] `FileIndex.list_dir/2` — `(volume_id, path)` → quorum read DirectoryEntry → return children map
- [ ] `FileIndex.delete/3` uses IntentLog for cross-segment atomicity:
  1. `IntentLog.try_acquire` with conflict_key `{:file, file_id}`
  2. Quorum delete FileMeta
  3. Quorum write DirectoryEntry update (remove child)
  4. `IntentLog.complete`
- [ ] `FileIndex.rename/4` — within same directory: single DirectoryEntry quorum write (update children map key)
- [ ] `FileIndex.move/5` — across directories: IntentLog + two DirectoryEntry quorum writes (remove from source, add to dest)
- [ ] `file_index_by_path` ETS table removed (replaced by DirectoryEntry lookups)
- [ ] Root directory entry (`"/"`) created automatically for each volume
- [ ] Nested directory creation: `FileIndex.mkdir/3` creates DirectoryEntry and adds to parent
- [ ] Unit tests: create file, verify both FileMeta and DirectoryEntry written
- [ ] Unit tests: get_by_path traverses directory entries correctly
- [ ] Unit tests: list_dir returns children
- [ ] Unit tests: delete file removes from both stores
- [ ] Unit tests: rename within directory (single segment operation)
- [ ] Unit tests: move across directories (cross-segment with IntentLog)
- [ ] Unit tests: concurrent create with same name returns conflict error

## Testing Strategy
- ExUnit tests for file creation lifecycle: create → get → get_by_path → list_dir → delete
- ExUnit tests for directory operations: mkdir, list, rename, move
- ExUnit tests for IntentLog integration: concurrent creates rejected, concurrent deletes rejected
- ExUnit tests for path traversal: create nested path `/a/b/c/file.txt`, get_by_path resolves correctly
- ExUnit tests for root directory auto-creation on volume creation
- Verify all existing ReadOperation and WriteOperation tests pass (they use FileIndex API)

## Dependencies
- task_0084 (QuorumCoordinator — quorum read/write operations)
- task_0085 (IntentLog — cross-segment atomicity for create/delete/move)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/directory_entry.ex` (new — DirectoryEntry struct)
- `neonfs_core/lib/neon_fs/core/file_index.ex` (modify — major rework: replace Ra with quorum + IntentLog)
- `neonfs_client/lib/neon_fs/core/file_meta.ex` (modify — add `hlc_timestamp` field)
- `neonfs_core/test/neon_fs/core/file_index_test.exs` (modify — rewrite for quorum + directory entry backend)
- `neonfs_core/test/neon_fs/core/directory_entry_test.exs` (new — struct tests)

## Reference
- spec/metadata.md — Directory and File Metadata Separation
- spec/metadata.md — Operation Examples (create, list, rename, move)
- spec/metadata.md — Intent Log: Transaction Safety and Write Coordination
- spec/metadata.md — Reactor-Based Orchestration (CreateFile example)
- spec/metadata.md — Crash Recovery (recovery policy table)

## Notes
The separation of DirectoryEntry from FileMeta is the key architectural change. Previously, `file_index_by_path` was a local ETS table mapping paths to file IDs — this doesn't work in a distributed system. DirectoryEntry solves this by making the path→ID mapping a first-class replicated data structure. The cross-segment atomicity for file creation is critical: if we create the FileMeta but crash before adding the DirectoryEntry, we get an orphaned file. The IntentLog + Reactor pattern (from the spec) handles this. For this task, the Reactor integration is optional — a simpler `with` block approach works if Reactor dependency isn't justified yet. The recovery logic (spec/metadata.md — Crash Recovery) should be implemented: on startup, scan for incomplete intents and resolve them.
