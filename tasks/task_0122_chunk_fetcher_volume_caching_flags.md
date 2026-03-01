# Task 0122: `ChunkFetcher` Consults Volume Caching Flags

## Status
Complete

## Phase
Gap Analysis — QW-4

## Description
`ChunkFetcher.maybe_cache_result/4` decides whether to cache based on a `decompress` boolean from the caller. It never consults the volume's `caching` configuration (`transformed_chunks`, `reconstructed_stripes`, `remote_chunks`). This means a volume with `transformed_chunks: false` will still have its decompressed chunks cached.

Update `ChunkFetcher` to look up the volume's caching config and respect its flags when deciding whether to cache a chunk.

## Acceptance Criteria
- [ ] `ChunkFetcher.fetch_chunk/2` looks up the volume's caching config (via `VolumeRegistry.get/1` or passed in opts)
- [ ] `maybe_cache_result` checks `volume.caching.transformed_chunks` before caching decompressed data
- [ ] `maybe_cache_result` checks `volume.caching.reconstructed_stripes` before caching EC-reconstructed data
- [ ] `maybe_cache_result` checks `volume.caching.remote_chunks` before caching data fetched from a remote node
- [ ] A volume with all caching flags set to `false` results in no chunks being cached
- [ ] Default behaviour unchanged — volumes with default config (`all: true`) still cache as before
- [ ] Existing tests pass
- [ ] `mix format` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/chunk_fetcher_test.exs`:
  - Fetch with `transformed_chunks: true` → chunk is cached
  - Fetch with `transformed_chunks: false` → chunk is NOT cached
  - Fetch a remote chunk with `remote_chunks: false` → not cached
  - Fetch a remote chunk with `remote_chunks: true` → cached
- Verify cache hit/miss via `ChunkCache.get/2` after fetch

## Dependencies
- task_0121 (ChunkCache configurable limit) — not strictly required but should be done first for consistency

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_fetcher.ex` (modify — add volume config lookup, update `maybe_cache_result`)
- `neonfs_core/test/neon_fs/core/chunk_fetcher_test.exs` (modify — add caching flag tests)

## Reference
- `spec/storage-tiering.md` — application-level caching
- `spec/gap-analysis.md` — QW-4
- Existing: `Volume.default_caching/0` in `neonfs_client/lib/neon_fs/core/volume.ex` for flag definitions

## Notes
The fetch source (local, remote, reconstructed) needs to be threaded through so `maybe_cache_result` can check the appropriate flag. Currently the function receives `decompress` as a boolean — this likely needs to become a richer context (e.g. `cache_reason: :transformed | :reconstructed | :remote`) or the volume config can be checked against the `source` already returned from `fetch_from_storage`.

Keep the fast path — if the volume lookup fails or returns nil, fall back to the current behaviour (cache if decompressed).
