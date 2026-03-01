# Task 0160: Node-Level Cache Budget

## Status
Complete

## Phase
Gap Analysis — M-15

## Description
Rework `ChunkCache` to enforce a single node-level memory budget with LRU
eviction across all volumes, and remove the per-volume `max_memory` field
from the `Volume` struct's `caching_config`.

The spec defines `max_memory` as a per-volume setting, but this doesn't
make practical sense — operators care about total memory consumption on a
node, not per-volume budgets that compound as volumes are added. A node
with 10 volumes each configured for 256 MB would consume 2.5 GB of cache.

QW-3 (task 0121) already added `config :neonfs_core, :chunk_cache_max_memory`
as a node-level config. This task completes the transition by:
1. Making the global limit the sole memory constraint
2. Removing `max_memory` from `Volume.caching_config`
3. Enforcing cache policy flags (`transformed_chunks`, `reconstructed_stripes`,
   `remote_chunks`) that exist in the struct but are never checked

## Acceptance Criteria
- [ ] `ChunkCache` enforces a single global `max_memory` limit across all volumes
- [ ] LRU eviction is global (evicts least-recently-used entry regardless of volume)
- [ ] `max_memory` removed from `Volume.caching_config` type
- [ ] `Volume.default_caching/0` no longer includes `max_memory`
- [ ] `Volume.update/2` no longer accepts `max_memory` in caching updates
- [ ] `ChunkCache.put/4` checks volume's `caching.transformed_chunks` flag before caching transformed chunks
- [ ] `ChunkCache.put/4` checks volume's `caching.reconstructed_stripes` flag before caching reconstructed stripes
- [ ] `ChunkCache.put/4` checks volume's `caching.remote_chunks` flag before caching remote chunks
- [ ] `ChunkCache.put/4` accepts a `:chunk_type` option (`:transformed` | `:reconstructed` | `:remote` | `:local`)
- [ ] Chunks with a disabled cache category are silently not cached (no error)
- [ ] Volume lookup for caching flags uses VolumeRegistry (cached in ETS for performance)
- [ ] Unit test: global LRU eviction works across multiple volumes
- [ ] Unit test: cache respects global max_memory limit
- [ ] Unit test: transformed chunk not cached when `transformed_chunks: false`
- [ ] Unit test: remote chunk not cached when `remote_chunks: false`
- [ ] Unit test: Volume struct no longer has max_memory in caching_config
- [ ] Existing cache tests still pass (with adjustments for removed max_memory)
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/chunk_cache_test.exs` (modify):
  - Remove tests for per-volume max_memory
  - Add tests for global LRU eviction across volumes
  - Add tests for cache policy flag enforcement
- Unit tests in `neonfs_client/test/neon_fs/core/volume_test.exs` (modify):
  - Verify max_memory is no longer in caching_config

## Dependencies
- None (ChunkCache and Volume already exist)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_cache.ex` (modify — global LRU, policy flag checks)
- `neonfs_client/lib/neon_fs/core/volume.ex` (modify — remove max_memory from caching_config)
- `neonfs_core/test/neon_fs/core/chunk_cache_test.exs` (modify — update tests)
- `neonfs_client/test/neon_fs/core/volume_test.exs` (modify — update caching_config tests)

## Reference
- `spec/storage-tiering.md` — application-level caching
- `spec/gap-analysis.md` — M-15
- Existing: `neonfs_core/lib/neon_fs/core/chunk_cache.ex`
- Existing: `neonfs_client/lib/neon_fs/core/volume.ex` (caching_config type)
- Task 0121 (chunk_cache_max_memory app config — already complete)

## Notes
Removing `max_memory` from `Volume.caching_config` is a breaking change to
the Volume struct. Any code that accesses `volume.caching.max_memory` will
need updating. Search for usages and update all references.

The `chunk_type` option on `put/4` tells the cache what kind of chunk is
being cached, so it can check the volume's policy flags. The read path
should be updated to pass the appropriate type:
- Regular local reads: `:local` (always cached)
- Reads requiring decompression/decryption: `:transformed`
- Erasure-coded reconstruction: `:reconstructed`
- Fetched from remote node: `:remote`

For volume caching flag lookups, avoid hitting VolumeRegistry on every
cache put. Either pass the volume's caching config as a parameter to `put`
or cache the flags in ETS with a short TTL.
