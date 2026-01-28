# Task 0010: Create neonfs_fuse Rust Crate with Rustler

## Status
Complete

## Phase
1 - Foundation

## Description
Create the neonfs_fuse Rust crate using `mix rustler.new` inside the neonfs_fuse package. This crate will handle FUSE filesystem operations, translating POSIX calls into requests to the Elixir control plane.

## Acceptance Criteria
- [x] Run `mix rustler.new` in neonfs_fuse directory to create `neonfs_fuse` crate
- [x] Crate created at `neonfs_fuse/native/neonfs_fuse/`
- [x] Elixir NIF module created at `lib/neon_fs/fuse/native.ex`
- [x] mix.exs updated with rustler configuration
- [x] Add `fuser` dependency to Cargo.toml (FUSE library, made optional due to system dependencies)
- [x] Add `tokio` dependency for async runtime
- [x] Default NIF function works when called from Elixir
- [x] Update `.check.exs` to include this crate's Rust checks
- [x] All checks pass: clippy, fmt, test

## Commands to Run
```bash
cd neonfs_fuse
mix deps.get
mix rustler.new
# When prompted:
#   Module name: NeonFS.FUSE.Native
#   Library name: neonfs_fuse
```

## Cargo.toml Dependencies
```toml
[dependencies]
rustler = "0.34"
fuser = "0.15"
tokio = { version = "1", features = ["sync", "rt-multi-thread"] }
log = "0.4"
thiserror = "1.0"
```

## Testing Strategy
- Verify `mix compile` succeeds
- Elixir test: call the generated NIF function
- Run `cargo test` in native/neonfs_fuse
- Run `cargo clippy` and `cargo fmt --check`

## Dependencies
- task_0002_check_exs_rust_integration (for the pattern to follow)

## Files Created/Modified
- `neonfs_fuse/native/neonfs_fuse/Cargo.toml`
- `neonfs_fuse/native/neonfs_fuse/src/lib.rs`
- `neonfs_fuse/lib/neon_fs/fuse/native.ex`
- `neonfs_fuse/mix.exs`
- `neonfs_fuse/.check.exs` (update Rust crate path)

## Reference
- spec/architecture.md - neonfs_fuse crate section
- spec/implementation.md - neonfs_fuse crate dependencies

## Notes
The actual FUSE implementation comes in subsequent tasks. This task establishes the crate structure and NIF communication channel.

**Important:** The `fuser` dependency has been made optional (behind a `fuse` feature flag) because it requires system FUSE libraries (libfuse3-dev or libfuse2-dev) at build time. In environments without these system libraries, the crate can still be built and tested with the basic NIF scaffolding. The `fuse` feature should be enabled when deploying to environments with FUSE libraries installed.
