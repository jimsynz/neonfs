# Task 0115: Core Metadata Event Emission

## Status
Complete

## Phase
10 - Event Notification

## Description
Instrument the core metadata modules (`FileIndex`, `VolumeRegistry`, `ACLManager`) to emit events via `NeonFS.Events.Broadcaster.broadcast/2` after successful metadata writes. Events are emitted at the wrapper module level (not inside the storage engine), after the write has been committed and any local caches updated.

This task connects the Broadcaster (task 0114) to the actual metadata write paths, making the event notification system live. Only the core node that initiates the write broadcasts the event — other core nodes see the state change through normal replication but do not re-broadcast.

## Acceptance Criteria
- [ ] `NeonFS.Core.FileIndex` emits events after successful writes:
  - [ ] `create/1` (file creation) — broadcasts `%FileCreated{volume_id, file_id, path}`
  - [ ] `update/2` (file content update) — broadcasts `%FileContentUpdated{volume_id, file_id, path}`
  - [ ] `delete/1` (file deletion) — broadcasts `%FileDeleted{volume_id, file_id, path}`
  - [ ] `mkdir/3` (directory creation) — broadcasts `%DirCreated{volume_id, path}`
  - [ ] `rename/4` (file/dir rename within same parent) — broadcasts `%FileRenamed{volume_id, file_id, old_path, new_path}` or `%DirRenamed{volume_id, old_path, new_path}` based on entry type
  - [ ] `move/4` (file/dir move to different parent) — same event types as rename
  - [ ] Attribute changes (chmod, chown, utimens) when exposed — broadcasts `%FileAttrsChanged{volume_id, file_id, path}`
- [ ] `NeonFS.Core.VolumeRegistry` emits events after successful writes:
  - [ ] `create/2` — broadcasts `%VolumeCreated{volume_id}`
  - [ ] `delete/1` — broadcasts `%VolumeDeleted{volume_id}`
  - [ ] `update/2` — broadcasts `%VolumeUpdated{volume_id}`
  - [ ] `update_stats/2` — does NOT broadcast (stats updates are internal, not user-visible state)
  - [ ] `create_system_volume/0` — does NOT broadcast (system volume is internal)
  - [ ] `adjust_system_volume_replication/1` — does NOT broadcast (internal operation)
- [ ] `NeonFS.Core.ACLManager` emits events after successful writes:
  - [ ] `set_volume_acl/2` — broadcasts `%VolumeAclChanged{volume_id}`
  - [ ] `grant/3` — broadcasts `%VolumeAclChanged{volume_id}`
  - [ ] `revoke/2` — broadcasts `%VolumeAclChanged{volume_id}`
  - [ ] `delete_volume_acl/1` — broadcasts `%VolumeAclChanged{volume_id}`
  - [ ] File ACL changes (if `set_file_acl` exists) — broadcasts `%FileAclChanged{volume_id, path}`
- [ ] Events are broadcast AFTER the write succeeds, not before
- [ ] Events are broadcast in the same process that performed the write (no async dispatch to avoid ordering issues)
- [ ] `ChunkIndex` and `StripeIndex` do NOT emit events (chunk/stripe operations are internal)
- [ ] Broadcast failures (e.g., `:pg` not running) are logged but do not cause the write to fail
- [ ] Existing tests still pass — broadcasting is fire-and-forget, so existing tests that don't subscribe to events are unaffected
- [ ] Unit tests verifying events are broadcast after writes

## Testing Strategy
- ExUnit test: create a file via FileIndex, verify a subscriber receives `FileCreated` event with correct fields
- ExUnit test: update a file, verify `FileContentUpdated` event
- ExUnit test: delete a file, verify `FileDeleted` event
- ExUnit test: mkdir, verify `DirCreated` event
- ExUnit test: rename a file, verify `FileRenamed` event with old and new paths
- ExUnit test: create a volume, verify `VolumeCreated` event
- ExUnit test: delete a volume, verify `VolumeDeleted` event
- ExUnit test: update a volume, verify `VolumeUpdated` event
- ExUnit test: set volume ACL, verify `VolumeAclChanged` event
- ExUnit test: `update_stats` does NOT emit an event
- ExUnit test: `create_system_volume` does NOT emit an event
- ExUnit test: failed writes do NOT emit events
- ExUnit test: existing FileIndex, VolumeRegistry, and ACLManager tests still pass unchanged

## Dependencies
- task_0114 (Broadcaster and sequence counters)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/file_index.ex` (modify — add Broadcaster.broadcast calls in do_* functions)
- `neonfs_core/lib/neon_fs/core/volume_registry.ex` (modify — add Broadcaster.broadcast calls)
- `neonfs_core/lib/neon_fs/core/acl_manager.ex` (modify — add Broadcaster.broadcast calls)
- `neonfs_core/test/neon_fs/events/emission_test.exs` (new — verify events emitted after writes)

## Reference
- spec/pubsub.md — Where Events Originate section
- spec/pubsub.md — What is NOT an Event section
- spec/pubsub.md — Broadcast Semantics section
- Existing patterns in FileIndex (`do_create/2`, `do_update/2`, etc.)
- Existing patterns in VolumeRegistry (`do_create_volume/2`, `do_delete_volume/2`, etc.)

## Notes
The broadcast call should be a single line added after the successful write in each `do_*` function. For example, in FileIndex's `do_create`:

```elixir
def do_create(%FileMeta{} = file) do
  with :ok <- QuorumCoordinator.quorum_write(...) do
    # ... update ETS cache ...
    Broadcaster.broadcast(file.volume_id, %FileCreated{
      volume_id: file.volume_id,
      file_id: file.id,
      path: file.path
    })
    {:ok, file}
  end
end
```

The Broadcaster call is inside the `with` success path, ensuring events are only emitted on successful writes. Since `broadcast/2` returns `:ok` and uses fire-and-forget `send/2`, it cannot cause the write to fail.

For rename/move operations, determine whether the entry is a file or directory to choose the correct event type. FileIndex already tracks this distinction via the `DirectoryEntry` type field.

Wrap the `Broadcaster.broadcast/2` call in a try/rescue to handle the case where `:pg` hasn't started yet (e.g., during bootstrap). Log a warning but don't crash. This is defence in depth — supervision ordering should prevent this, but broadcasting is explicitly non-critical.

The `update_stats/2` exclusion is important: stats updates happen frequently (per-write size tracking) and don't represent user-visible state changes. Including them would flood the event system.
