# Task 0041: Fix FUSE Rust Compilation Errors

## Status

Complete

## Phase

1 (Foundation Addendum - must complete before Phase 2)

## Description

The neonfs_fuse Rust crate has 6 compilation errors that prevent it from building when the `fuse` feature is enabled. These must be fixed for FUSE mounting to work.

## Acceptance Criteria

- [x] Add `"time"` to tokio features in Cargo.toml
- [x] Fix `getattr` method signature to include `fh: Option<u64>` parameter
- [x] Implement `From<String>` for `FuseError` or refactor error handling
- [x] All type inference errors resolved
- [x] `cargo build --features fuse` succeeds
- [x] `cargo clippy --features fuse --all-targets -- -D warnings` passes
- [x] `cargo test --features fuse` passes
- [x] Enable FUSE tests in test_helper.exs (remove from exclude list)
- [x] FUSE integration tests pass when FUSE is available

## Specific Fixes Required

### 1. Missing tokio `time` feature (filesystem.rs:56, 66)

```toml
# neonfs_fuse/native/neonfs_fuse/Cargo.toml
[dependencies]
tokio = { version = "1", features = ["sync", "rt-multi-thread", "time"] }
```

### 2. Incorrect `getattr` signature (filesystem.rs:141)

```rust
// Current (wrong):
fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr)

// Fixed:
fn getattr(&mut self, _req: &Request, ino: u64, fh: Option<u64>, reply: ReplyAttr)
```

### 3. FuseError missing `From<String>` (filesystem.rs:46)

```rust
// In error.rs, add:
impl From<String> for FuseError {
    fn from(s: String) -> Self {
        FuseError::OperationFailed(s)
    }
}

// Or add a new variant:
#[derive(Debug, thiserror::Error)]
pub enum FuseError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Operation failed: {0}")]
    OperationFailed(String),
}
```

### 4. Enable FUSE tests

```elixir
# neonfs_fuse/test/test_helper.exs
# Change from:
ExUnit.start(exclude: [:integration, :fuse])
# To:
ExUnit.start(exclude: [:integration])
```

## Testing Strategy

1. Run `cargo build --features fuse` and verify success
2. Run `cargo test --features fuse` and verify all tests pass
3. Run `mix test` in neonfs_fuse with FUSE available
4. Run manual mount test: mount directory, create file, verify contents

## Dependencies

- Task 0010 (FUSE scaffolding) - Complete
- Task 0011 (FUSE channels) - Complete
- Task 0012 (FUSE mount ops) - Complete
- Task 0013 (FUSE write ops) - Complete

## Files to Modify

- `neonfs_fuse/native/neonfs_fuse/Cargo.toml`
- `neonfs_fuse/native/neonfs_fuse/src/filesystem.rs`
- `neonfs_fuse/native/neonfs_fuse/src/error.rs`
- `neonfs_fuse/test/test_helper.exs`

## Reference

- fuser crate documentation: https://docs.rs/fuser
- Task 0012 acceptance criteria
