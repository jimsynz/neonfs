# Task 0093: SystemVolume Access API

## Status
Not Started

## Phase
7 - System Volume

## Description
Create the `NeonFS.Core.SystemVolume` module that provides a convenience API for reading, writing, appending, listing, and deleting files on the `_system` volume. This module wraps the existing volume read/write operations with the hardcoded volume name `_system`, providing a clean internal API for subsystems that store data in the system volume (cluster CA, audit logs, intent archives).

The system volume is accessed programmatically — it is never mounted via FUSE. All operations delegate to the existing `ReadOperation` and `WriteOperation` modules.

## Acceptance Criteria
- [ ] New `NeonFS.Core.SystemVolume` module with `@volume_name "_system"`
- [ ] `SystemVolume.read(path)` returns `{:ok, binary()}` or `{:error, term()}`
- [ ] `SystemVolume.write(path, content)` returns `:ok` or `{:error, term()}`
- [ ] `SystemVolume.append(path, content)` returns `:ok` or `{:error, term()}` — appends to existing file or creates if not exists
- [ ] `SystemVolume.list(path)` returns `{:ok, [String.t()]}` or `{:error, term()}`
- [ ] `SystemVolume.delete(path)` returns `:ok` or `{:error, term()}`
- [ ] `SystemVolume.exists?(path)` returns `boolean()`
- [ ] All functions return `{:error, :system_volume_not_found}` if the system volume has not been created yet
- [ ] Type specs on all public functions
- [ ] Unit tests for each function (read, write, append, list, delete, exists?)
- [ ] Unit tests for error cases (volume not created, path not found)

## Testing Strategy
- ExUnit tests that create the system volume via `VolumeRegistry.create_system_volume/0`, then exercise each `SystemVolume` function
- Test write + read roundtrip (write content, read it back, verify match)
- Test append (write initial content, append more, read back full content)
- Test list (write several files under a path, list them, verify names)
- Test delete (write a file, delete it, verify read returns error)
- Test exists? (verify true after write, false after delete)
- Test error case: call functions before system volume exists

## Dependencies
- task_0092 (Volume struct `system` field and VolumeRegistry guards)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/system_volume.ex` (new — SystemVolume module)
- `neonfs_core/test/neon_fs/core/system_volume_test.exs` (new — unit tests)

## Reference
- spec/system-volume.md — Access API section
- Existing pattern: `NeonFS.Core.ReadOperation` and `NeonFS.Core.WriteOperation` for volume I/O

## Notes
The `append/2` function is important for audit logs and intent archives which are written as append-only JSONL files. The implementation should read the existing file content (if any), concatenate the new content, and write the result back. This is acceptable because system volume files (audit logs, certificates) are small. A more efficient append-in-place mechanism could be added later if needed but is not required for Phase 7.

The `exists?/1` function is a convenience that wraps `read/1` — it returns `true` if the read succeeds and `false` if the error is `:not_found`. Other errors propagate.
