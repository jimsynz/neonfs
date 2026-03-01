# Task 0179: neonfs_blob Cargo-Fuzz Scaffolding

## Status
Blocked — requires nightly toolchain; needs CI design for periodic fuzzing job

## Phase
Gap Analysis — H-3 (1/5)

## Description
Set up the `cargo-fuzz` infrastructure for the `neonfs_blob` Rust crate.
This includes initialising the fuzz directory structure, adding
`libfuzzer-sys` as a dev dependency, and creating the `Cargo.toml`
configuration for fuzz targets.

The `neonfs_blob` crate handles chunk storage, hashing, compression, and
encryption — all parsing-heavy operations that benefit from fuzzing.

## Acceptance Criteria
- [ ] `cargo fuzz init` run in `neonfs_core/native/neonfs_blob/` (or equivalent manual setup)
- [ ] `neonfs_core/native/neonfs_blob/fuzz/Cargo.toml` created with `libfuzzer-sys` dependency
- [ ] Fuzz `Cargo.toml` references the parent `neonfs_blob` crate as a path dependency
- [ ] `neonfs_core/native/neonfs_blob/fuzz/fuzz_targets/` directory created
- [ ] A placeholder fuzz target (`neonfs_core/native/neonfs_blob/fuzz/fuzz_targets/placeholder.rs`) compiles and runs
- [ ] `neonfs_core/native/neonfs_blob/fuzz/corpus/` directory created for seed corpora
- [ ] `.gitignore` updated to exclude fuzz artifacts (`fuzz/artifacts/`, `fuzz/corpus/` large files)
- [ ] `cargo fuzz list` shows the placeholder target
- [ ] `cargo fuzz run placeholder -- -max_total_time=5` runs without error (5-second smoke test)
- [ ] README or doc comment in fuzz directory explaining how to run targets

## Testing Strategy
- Verify `cargo fuzz list` outputs the placeholder target name
- Run `cargo fuzz run placeholder -- -max_total_time=5` to confirm the toolchain works
- This is infrastructure setup — the actual fuzz targets come in tasks 0180 and 0181

## Dependencies
- None

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/fuzz/Cargo.toml` (create — fuzz workspace config)
- `neonfs_core/native/neonfs_blob/fuzz/fuzz_targets/placeholder.rs` (create — minimal fuzz target)
- `neonfs_core/native/neonfs_blob/fuzz/corpus/` (create — empty directory for seed data)
- `neonfs_core/native/neonfs_blob/.gitignore` (modify — exclude fuzz artifacts)

## Reference
- `spec/gap-analysis.md` — H-3
- `spec/testing.md` lines 419–487 (fuzz target definitions)
- `cargo-fuzz` book: https://rust-fuzz.github.io/book/cargo-fuzz.html
- Existing: `neonfs_core/native/neonfs_blob/Cargo.toml`

## Notes
`cargo-fuzz` requires the nightly Rust toolchain. The `fuzz/Cargo.toml`
should specify `[package] edition = "2021"` and use the same Rust edition
as the parent crate.

The placeholder target is trivial:

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // placeholder — replaced by real targets in tasks 0180/0181
    let _ = data;
});
```

The `fuzz/` directory is a separate Cargo workspace — it has its own
`Cargo.toml` and `Cargo.lock`. This is standard `cargo-fuzz` structure.

Seed corpora will be populated in subsequent tasks (0180, 0181) with
representative inputs for each fuzz target.
