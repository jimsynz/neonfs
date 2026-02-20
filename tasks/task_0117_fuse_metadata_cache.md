# Task 0117: FUSE Metadata Cache with Event-Driven Invalidation

## Status
Complete

## Phase
10 - Event Notification

## Description
Implement `NeonFS.FUSE.MetadataCache` GenServer in the `neonfs_fuse` package. This module provides an ETS-backed local cache for file and directory metadata, reducing RPC round-trips to core nodes during normal operation. The cache subscribes to volume events and invalidates affected entries when it receives event notifications, providing both performance (cached reads) and correctness (push-based invalidation).

The MetadataCache implements gap detection via per-source-node sequence tracking. If a sequence gap is detected (indicating a dropped message), all caches are invalidated as a safety measure.

The FUSE Handler is updated to consult the MetadataCache before making RPC calls for `getattr`, `lookup`, and `readdir` operations.

## Acceptance Criteria
- [ ] New `NeonFS.FUSE.MetadataCache` GenServer in `neonfs_fuse`
- [ ] Implements `NeonFS.Client.EventHandler` behaviour
- [ ] ETS-backed cache with separate tables or key namespaces for:
  - [ ] File attributes (`getattr` results keyed by file_id or path)
  - [ ] Directory listings (`readdir` results keyed by directory path)
  - [ ] Lookup results (keyed by `{parent_path, name}`)
- [ ] Cache API:
  - [ ] `get_attrs(volume_id, file_id_or_path)` — returns cached attributes or `:miss`
  - [ ] `put_attrs(volume_id, file_id_or_path, attrs)` — caches attributes
  - [ ] `get_dir_listing(volume_id, path)` — returns cached directory listing or `:miss`
  - [ ] `put_dir_listing(volume_id, path, entries)` — caches directory listing
  - [ ] `get_lookup(volume_id, parent_path, name)` — returns cached lookup or `:miss`
  - [ ] `put_lookup(volume_id, parent_path, name, result)` — caches lookup result
  - [ ] `invalidate_all(volume_id)` — clears all cache entries for a volume
- [ ] Event subscription: subscribes to events for the mounted volume on start, unsubscribes on stop
- [ ] Event handling (via `handle_info({:neonfs_event, envelope})`):
  - [ ] `FileCreated` — invalidates parent directory listing
  - [ ] `FileContentUpdated` — invalidates file attributes
  - [ ] `FileTruncated` — invalidates file attributes
  - [ ] `FileDeleted` — invalidates file attributes and parent directory listing
  - [ ] `FileAttrsChanged` — invalidates file attributes
  - [ ] `FileRenamed` — invalidates file attributes for old path, parent directory listings for both old and new directories
  - [ ] `VolumeAclChanged` — invalidates all cached data for the volume
  - [ ] `FileAclChanged` — invalidates file attributes for the path
  - [ ] `DirCreated` — invalidates parent directory listing
  - [ ] `DirDeleted` — invalidates directory and parent directory listings
  - [ ] `DirRenamed` — invalidates old directory, parent directory listings for old and new locations
  - [ ] `VolumeUpdated` — invalidates all cached data for the volume
- [ ] Handles `:neonfs_invalidate_all` message (from PartitionRecovery) by clearing all caches
- [ ] Gap detection:
  - [ ] Tracks `last_sequences :: %{node() => non_neg_integer()}` per source node
  - [ ] On receiving an envelope, checks if `sequence > last + 1`
  - [ ] If gap detected, logs a warning and invalidates all caches for safety
  - [ ] Updates last sequence after processing
- [ ] FUSE Handler integration:
  - [ ] Handler's `lookup` operation checks MetadataCache before RPC
  - [ ] Handler's `getattr` operation checks MetadataCache before RPC
  - [ ] Handler's `readdir` operation checks MetadataCache before RPC
  - [ ] Handler populates cache after successful RPC responses
  - [ ] Cache is per-mount (each Handler has its own MetadataCache reference)
- [ ] MetadataCache is started per mount (one cache per mounted volume)
- [ ] Type specs on all public functions
- [ ] Unit tests for MetadataCache

## Testing Strategy
- ExUnit test: `put_attrs/3` and `get_attrs/2` roundtrip
- ExUnit test: `put_dir_listing/3` and `get_dir_listing/2` roundtrip
- ExUnit test: `put_lookup/4` and `get_lookup/3` roundtrip
- ExUnit test: `FileCreated` event invalidates parent directory listing
- ExUnit test: `FileDeleted` event invalidates file attributes and parent directory listing
- ExUnit test: `FileRenamed` event invalidates both old and new paths
- ExUnit test: `VolumeAclChanged` event invalidates all entries
- ExUnit test: gap detection — simulate sequence gap, verify all caches cleared
- ExUnit test: `:neonfs_invalidate_all` message clears all caches
- ExUnit test: cache miss returns `:miss`
- ExUnit test: handler reads from cache on hit (mock or verify no RPC call made)
- ExUnit test: handler populates cache after RPC on miss

## Dependencies
- task_0113 (Subscription API, Relay, and Registry)
- task_0115 (Core metadata event emission — events must actually be emitted for end-to-end testing)

## Files to Create/Modify
- `neonfs_fuse/lib/neon_fs/fuse/metadata_cache.ex` (new — MetadataCache GenServer)
- `neonfs_fuse/lib/neon_fs/fuse/handler.ex` (modify — add cache lookups before RPC calls, populate cache after RPC)
- `neonfs_fuse/lib/neon_fs/fuse/mount_supervisor.ex` or `mount_manager.ex` (modify — start MetadataCache alongside Handler for each mount)
- `neonfs_fuse/test/neon_fs/fuse/metadata_cache_test.exs` (new — unit tests)

## Reference
- spec/pubsub.md — Client-Side Event Handler section (MetadataCache reference implementation)
- spec/pubsub.md — Gap Detection section
- spec/pubsub.md — What This Spec Does NOT Cover section (cache internals are implementation-specific)
- Existing `NeonFS.FUSE.Handler` module for current RPC call patterns

## Notes
The spec explicitly states that "metadata cache implementation details (data structure, eviction policy, lookup API) are implementation concerns, not specified here." This gives flexibility in the cache design. Key considerations:

1. **ETS vs map**: ETS is preferred for concurrent read access from the Handler process. The MetadataCache GenServer owns the ETS table but the Handler can read directly without going through the GenServer.

2. **Eviction**: A simple bounded cache (max entries) with LRU eviction is sufficient for the initial implementation. The event-driven invalidation handles correctness; eviction handles memory pressure.

3. **TTL**: Optional — since events handle invalidation, TTL is a safety net, not a correctness mechanism. A conservative TTL (e.g., 60 seconds) provides defence in depth for events that might be dropped outside the gap detection window.

4. **Cache granularity**: Cache invalidation is at the file/directory level, not the volume level (except for VolumeAclChanged and VolumeUpdated which invalidate everything). This is more efficient than wholesale invalidation on every event.

5. **Handler integration**: The Handler currently calls core via `NeonFS.Client.core_call/3`. The cache sits between the Handler and the RPC layer: check cache first, on miss call core, on success populate cache, return result.

Start the MetadataCache as a child of the mount's supervision subtree (alongside the Handler). Pass the MetadataCache PID or ETS table name to the Handler so it can read directly from ETS.
