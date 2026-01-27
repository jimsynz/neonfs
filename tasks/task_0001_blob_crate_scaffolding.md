# Task 0001: Create neonfs_blob Rust Crate with Rustler

## Status
Complete

## Phase
1 - Foundation

## Description
Create the neonfs_blob Rust crate using `mix rustler.new` inside the neonfs_core package. This crate will handle content-addressed chunk storage, chunking algorithms, compression, and encryption. The scaffolding should use Rustler's generator to ensure proper integration.

## Acceptance Criteria
- [x] Run `mix rustler.new` in neonfs_core directory to create `neonfs_blob` crate
- [x] Crate created at `neonfs_core/native/neonfs_blob/`
- [x] Elixir NIF module created at `lib/neon_fs/core/blob/native.ex`
- [x] mix.exs updated with rustler configuration pointing to the crate
- [x] Default `add/2` NIF function works when called from Elixir
- [x] Mix project compiles successfully with `mix compile`
- [x] `cargo clippy --all-targets -- -D warnings` passes in native/neonfs_blob
- [x] `cargo fmt --check` passes in native/neonfs_blob
- [x] `cargo test` passes in native/neonfs_blob

## Commands to Run
```bash
cd neonfs_core
mix deps.get
mix rustler.new
# When prompted:
#   Module name: NeonFS.Core.Blob.Native
#   Library name: neonfs_blob
```

## Testing Strategy
- Verify `mix compile` succeeds
- Elixir test: call the generated add function and verify it works
- Run `cargo test` in native/neonfs_blob
- Run `cargo clippy --all-targets -- -D warnings` in native/neonfs_blob

## Dependencies
None - this is a foundational task

## Files Created by rustler.new
- `neonfs_core/native/neonfs_blob/Cargo.toml`
- `neonfs_core/native/neonfs_blob/src/lib.rs`
- `neonfs_core/lib/neon_fs/core/blob/native.ex`
- Updates to `neonfs_core/mix.exs`

## Reference
- spec/architecture.md - Rustler NIF Integration section
- spec/implementation.md - neonfs_blob crate dependencies

## Notes
After running rustler.new, the Cargo.toml will need additional dependencies added in subsequent tasks (sha2, fastcdc, zstd, etc.). This task focuses on getting the basic NIF infrastructure working.
