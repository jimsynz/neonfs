# Task 0145: stream_data Dependencies and Test Generators

## Status
Complete

## Phase
Gap Analysis — M-8 (1/4)

## Description
Add `stream_data` as a test dependency to packages that lack it and create
a shared test generators module with generators for common NeonFS types.
This is the foundation for property-based testing across the project.

Currently `stream_data` is only a dependency in `neonfs_core`. It needs to
be added to `neonfs_client`, `neonfs_fuse`, and `neonfs_integration`. A
shared generators module in `neonfs_core/test/support/` will provide
reusable generators for volume configs, file metadata, chunk hashes, paths,
and other common types.

## Acceptance Criteria
- [x] `{:stream_data, "~> 1.0", only: [:test]}` added to `neonfs_client/mix.exs`
- [x] `{:stream_data, "~> 1.0", only: [:test]}` added to `neonfs_fuse/mix.exs`
- [x] `{:stream_data, "~> 1.0", only: [:test]}` added to `neonfs_integration/mix.exs`
- [x] `NeonFS.TestGenerators` module created in `neonfs_core/test/support/test_generators.ex`
- [x] Generator: `chunk_hash/0` — 32-byte binary (SHA-256 hash format)
- [x] Generator: `volume_id/0` — valid UUID string
- [x] Generator: `file_path/0` — valid POSIX path (1–5 segments, alphanumeric + common chars)
- [x] Generator: `volume_config/0` — valid `Volume` configuration map with all required fields
- [x] Generator: `file_meta/0` — valid `FileMeta` struct
- [x] Generator: `compression_mode/0` — one of `:none`, `:lz4`, `:zstd`
- [x] Generator: `tier/0` — one of `:hot`, `:warm`, `:cold`
- [x] Generator: `binary_data/0` — arbitrary binary between 1 byte and 64 KB
- [x] All generators produce valid values (verified by a property test)
- [x] Unit test: each generator produces values that pass type validation
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Property tests in `neonfs_core/test/neon_fs/test_generators_test.exs`:
  - For each generator, verify 100+ generated values pass relevant validation
  - `chunk_hash/0` always produces 32-byte binaries
  - `volume_id/0` always produces valid UUID format
  - `file_path/0` always produces paths starting with `/` with no empty segments
  - `volume_config/0` always produces configs that `Volume.new/1` accepts

## Dependencies
- None

## Files to Create/Modify
- `neonfs_client/mix.exs` (modify — add stream_data dep)
- `neonfs_fuse/mix.exs` (modify — add stream_data dep)
- `neonfs_integration/mix.exs` (modify — add stream_data dep)
- `neonfs_core/test/support/test_generators.ex` (create — generator module)
- `neonfs_core/test/neon_fs/test_generators_test.exs` (create — validation tests)

## Reference
- `spec/testing.md` lines 290–336
- `spec/gap-analysis.md` — M-8
- Existing: `neonfs_core/test/neon_fs/core/metadata_ring_test.exs` (uses ExUnitProperties)
- https://hex2txt.fly.dev/stream_data/llms.txt (package documentation)

## Notes
The generators module lives in `neonfs_core/test/support/` because most
property tests will run in `neonfs_core`. Other packages can define
their own lightweight generators if needed, or use `stream_data`'s
built-in generators directly.

`test/support/` files are automatically compiled in the test environment
via the `elixirc_paths` config in `mix.exs`. Verify that
`neonfs_core/mix.exs` includes `"test/support"` in `elixirc_paths` for
the `:test` environment.

Keep generators simple and composable. Each generator should produce the
minimal valid structure — tests that need specific invalid or edge-case
values should construct them directly.
