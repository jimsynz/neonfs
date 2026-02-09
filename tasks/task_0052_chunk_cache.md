# Task 0052: Chunk Cache for Transformed Data

## Status
Complete

## Phase
3 - Policies, Tiering, and Compression

## Description
Implement an LRU cache (ETS-backed) for decompressed/decrypted chunks. Avoids redundant decompression on repeated reads of the same chunk. Per-volume memory limits are read from `volume.caching` config. Caching is skipped when unnecessary (uncompressed local data that requires no transformation).

## Acceptance Criteria
- [x] `ChunkCache` GenServer backed by ETS
- [x] `get/2` — `(volume_id, chunk_hash)` → `{:ok, data}` or `:miss`
- [x] `put/3` — `(volume_id, chunk_hash, data)` → `:ok` (stores with timestamp for LRU)
- [x] `invalidate/1` — `(chunk_hash)` → `:ok` (remove from all volumes)
- [x] `invalidate/2` — `(volume_id, chunk_hash)` → `:ok` (remove from specific volume)
- [x] `stats/0` — returns `%{hits: count, misses: count, evictions: count, memory_used: bytes}`
- [x] LRU eviction when volume exceeds `volume.caching.max_memory`
- [x] Memory tracking based on byte size of cached data
- [x] Cache skip logic: don't cache uncompressed local chunks (no transformation needed)
- [x] Cache skip logic: don't cache when `volume.caching.transformed_chunks` is false
- [x] Integrated into read path — check cache before BlobStore, populate after decompression
- [x] Telemetry events for hits, misses, evictions
- [x] All new code has unit tests

## Testing Strategy
- Unit tests for get/put/invalidate cycle
- Unit tests for LRU eviction (fill cache, verify oldest entries evicted first)
- Unit tests for memory limit enforcement
- Unit tests for cache skip logic
- Test stats tracking accuracy
- Integration: read same chunk twice, verify second read hits cache

## Dependencies
- task_0045 (needs volume caching config)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_cache.ex` (new)
- `neonfs_core/lib/neon_fs/core/chunk_fetcher.ex` (modify — integrate cache lookup/population)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add to supervision tree)
- `neonfs_core/test/neon_fs/core/chunk_cache_test.exs` (new)

## Reference
- spec/architecture.md — Caching strategy
- spec/implementation.md — Phase 3: Purpose-specific caching

## Notes
The LRU implementation uses two ETS tables: one keyed by `{volume_id, chunk_hash}` for data lookup, and one ordered_set keyed by `{timestamp, volume_id, chunk_hash}` for eviction ordering. On access, the timestamp entry is deleted and re-inserted with the current time. Memory tracking uses `byte_size/1` on the cached binary. The cache is intentionally simple — no distributed cache coherency. Each node caches independently. Cache invalidation on write is local only; stale reads from cache are acceptable for the short window until the entry is evicted.
