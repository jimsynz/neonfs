# Task 0121: Make `ChunkCache` Memory Limit Configurable

## Status
Complete

## Phase
Gap Analysis — QW-3

## Description
The `ChunkCache` memory limit is hardcoded as a default parameter (`268_435_456` bytes = 256 MiB) on `put/4`. Operators cannot tune cache size for their node's available memory without code changes.

Read the limit from application config so it can be set per-node in `config/runtime.exs` or via environment variable.

## Acceptance Criteria
- [x] `ChunkCache` reads its memory limit from `Application.get_env(:neonfs_core, :chunk_cache_max_memory, 268_435_456)` at startup
- [x] The limit is stored in the GenServer state or module attribute, not re-read on every `put`
- [x] `put/3` (without explicit max_memory) uses the configured limit
- [x] `put/4` (with explicit max_memory) still works for callers that need to override
- [x] `config/runtime.exs` includes a commented example: `# config :neonfs_core, chunk_cache_max_memory: 536_870_912  # 512 MiB`
- [x] Existing tests pass without modification
- [x] `mix format` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/chunk_cache_test.exs`:
  - Verify default limit of 256 MiB when no config is set
  - Verify a smaller configured limit triggers eviction sooner
  - Verify `put/4` with explicit max_memory still overrides the configured limit

## Dependencies
None — standalone fix.

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_cache.ex` (modify — read config in `init/1`, use in `put`)
- `neonfs_core/config/runtime.exs` (modify — add commented example)
- `neonfs_core/test/neon_fs/core/chunk_cache_test.exs` (modify — add config-based tests)

## Reference
- `spec/storage-tiering.md` — application-level caching
- `spec/gap-analysis.md` — QW-3, M-15
- Existing pattern: `BackgroundWorker` reads config from `Application.get_env` in `init/1`

## Notes
This is a stepping stone toward M-15 (per-node cache model). For now, the goal is simply to make the existing limit configurable rather than hardcoded. The per-volume `max_memory` in `Volume.caching_config` will be addressed separately in M-15.
