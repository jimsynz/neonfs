# Task 0112: Event Structs and Envelope

## Status
Complete

## Phase
10 - Event Notification

## Description
Define all event type structs and the `Envelope` wrapper in the `neonfs_client` package. These are pure data types with `@enforce_keys` and typespecs, shared by both broadcasting (core) and subscribing (client/fuse) sides. The structs live in `neonfs_client` so that all packages have compile-time access to the same types and Dialyzer can verify that broadcasters construct valid events and subscribers handle all variants.

There are 14 event types in 5 categories: file content (4), file attributes (2), ACL (2), directory (3), and volume (3). Plus the `Envelope` wrapper and a combined `event()` type.

## Acceptance Criteria
- [ ] New `NeonFS.Events` module in `neonfs_client` with combined `@type event()` union type
- [ ] File content event structs in `neonfs_client`:
  - [ ] `NeonFS.Events.FileCreated` — `@enforce_keys [:volume_id, :file_id, :path]`
  - [ ] `NeonFS.Events.FileContentUpdated` — `@enforce_keys [:volume_id, :file_id, :path]`
  - [ ] `NeonFS.Events.FileTruncated` — `@enforce_keys [:volume_id, :file_id, :path]`
  - [ ] `NeonFS.Events.FileDeleted` — `@enforce_keys [:volume_id, :file_id, :path]`
- [ ] File attribute event structs:
  - [ ] `NeonFS.Events.FileAttrsChanged` — `@enforce_keys [:volume_id, :file_id, :path]` (covers chmod, chown, utimens, xattr)
  - [ ] `NeonFS.Events.FileRenamed` — `@enforce_keys [:volume_id, :file_id, :old_path, :new_path]`
- [ ] ACL event structs:
  - [ ] `NeonFS.Events.VolumeAclChanged` — `@enforce_keys [:volume_id]`
  - [ ] `NeonFS.Events.FileAclChanged` — `@enforce_keys [:volume_id, :path]`
- [ ] Directory event structs:
  - [ ] `NeonFS.Events.DirCreated` — `@enforce_keys [:volume_id, :path]`
  - [ ] `NeonFS.Events.DirDeleted` — `@enforce_keys [:volume_id, :path]`
  - [ ] `NeonFS.Events.DirRenamed` — `@enforce_keys [:volume_id, :old_path, :new_path]`
- [ ] Volume event structs:
  - [ ] `NeonFS.Events.VolumeCreated` — `@enforce_keys [:volume_id]`
  - [ ] `NeonFS.Events.VolumeUpdated` — `@enforce_keys [:volume_id]`
  - [ ] `NeonFS.Events.VolumeDeleted` — `@enforce_keys [:volume_id]`
- [ ] `NeonFS.Events.Envelope` struct — `@enforce_keys [:event, :source_node, :sequence, :hlc_timestamp]`
- [ ] Envelope `@type t()` uses `NeonFS.Events.event()` for the `event` field, `node()` for `source_node`, `non_neg_integer()` for `sequence`, and `NeonFS.Core.HLC.timestamp()` for `hlc_timestamp`
- [ ] Every event struct has `volume_id :: binary()` field for uniform routing
- [ ] All structs have `@type t()` typespecs
- [ ] All field types are `binary()` for IDs, `String.t()` for paths
- [ ] Unit tests verifying struct construction and enforcement

## Testing Strategy
- ExUnit test: each event struct can be constructed with all required keys
- ExUnit test: each event struct raises `ArgumentError` when a required key is missing
- ExUnit test: `Envelope` can wrap any event type
- ExUnit test: `Envelope` raises when required keys are missing
- ExUnit test: type consistency — all event structs have a `volume_id` field

## Dependencies
- None (pure data types, no runtime dependencies beyond existing codebase)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/events.ex` (new — combined type + subscribe/unsubscribe stubs for later)
- `neonfs_client/lib/neon_fs/events/envelope.ex` (new)
- `neonfs_client/lib/neon_fs/events/file_created.ex` (new)
- `neonfs_client/lib/neon_fs/events/file_content_updated.ex` (new)
- `neonfs_client/lib/neon_fs/events/file_truncated.ex` (new)
- `neonfs_client/lib/neon_fs/events/file_deleted.ex` (new)
- `neonfs_client/lib/neon_fs/events/file_attrs_changed.ex` (new)
- `neonfs_client/lib/neon_fs/events/file_renamed.ex` (new)
- `neonfs_client/lib/neon_fs/events/volume_acl_changed.ex` (new)
- `neonfs_client/lib/neon_fs/events/file_acl_changed.ex` (new)
- `neonfs_client/lib/neon_fs/events/dir_created.ex` (new)
- `neonfs_client/lib/neon_fs/events/dir_deleted.ex` (new)
- `neonfs_client/lib/neon_fs/events/dir_renamed.ex` (new)
- `neonfs_client/lib/neon_fs/events/volume_created.ex` (new)
- `neonfs_client/lib/neon_fs/events/volume_updated.ex` (new)
- `neonfs_client/lib/neon_fs/events/volume_deleted.ex` (new)
- `neonfs_client/test/neon_fs/events_test.exs` (new — struct construction and enforcement tests)

## Reference
- spec/pubsub.md — Event Model section (event types, envelope, combined type)
- spec/pubsub.md — Module Placement table

## Notes
Each event struct is deliberately minimal — events are hints, not data. They signal that something changed; the subscriber decides whether to invalidate, re-fetch, or ignore. This means event structs carry just enough information for routing (volume_id) and cache key identification (file_id, path), not the full updated state.

The `NeonFS.Events` module will initially contain only the combined type definition. Subscribe/unsubscribe functions are added in task 0113. Keeping both in the same module avoids a separate types-only module.

The `hlc_timestamp` field references `NeonFS.Core.HLC.timestamp()` which is a `{wall_ms, counter, node_id}` tuple. This type is defined in `neonfs_core`, so neonfs_client code that pattern-matches on the envelope will see it as a 3-tuple. This is fine since neonfs_client has no compile-time dependency on neonfs_core — the type is structural, not nominal.

Consider grouping structurally identical event structs (e.g., FileCreated/FileContentUpdated/FileTruncated/FileDeleted all have the same fields) into separate modules rather than a single module with a `type` field. This gives Dialyzer precise type information and makes pattern matching in handlers exhaustive.
