# Task 0139: Volume Update Rust CLI Command

## Status
Complete

## Phase
Gap Analysis — M-4 (2/2)

## Description
Add a `volume update` subcommand to the Rust CLI that allows operators to
modify volume configuration at runtime. This is the Rust counterpart to
the Elixir handler function added in task 0138.

The command should accept the volume name as a positional argument and
use flags for each updatable field. Only specified flags are included in
the update — omitted flags mean "keep current value".

## Acceptance Criteria
- [ ] `VolumeCommand::Update` variant added to the volume command enum
- [ ] Accepts volume name as positional argument
- [ ] Flag `--compression` (none/lz4/zstd)
- [ ] Flag `--write-ack` (local/quorum/all)
- [ ] Flag `--io-weight` (positive integer)
- [ ] Flag `--atime-mode` (noatime/relatime)
- [ ] Flag `--initial-tier` (hot/warm/cold)
- [ ] Flag `--promotion-threshold` (positive integer, accesses per hour)
- [ ] Flag `--demotion-delay` (positive integer, hours)
- [ ] Flag `--cache-transformed` (true/false)
- [ ] Flag `--cache-reconstructed` (true/false)
- [ ] Flag `--cache-remote` (true/false)
- [ ] Flag `--verify-on-read` (always/never/sampling)
- [ ] Flag `--verify-sampling-rate` (float 0.0–1.0)
- [ ] Flag `--scrub-interval` (positive integer, hours)
- [ ] Flag `--metadata-replicas` (positive integer)
- [ ] Flag `--read-quorum` (positive integer)
- [ ] Flag `--write-quorum` (positive integer)
- [ ] At least one flag must be specified (error otherwise)
- [ ] Calls `Elixir.NeonFS.CLI.Handler.handle_volume_update/2` via RPC
- [ ] Displays updated volume summary on success
- [ ] Displays error message on failure
- [ ] `cargo clippy --all-targets -- -D warnings` passes
- [ ] `cargo fmt --check` passes

## Testing Strategy
- Rust unit tests for argument parsing (valid/invalid flag combinations)
- Rust unit tests for term construction (flags → eetf::Map)
- Manual end-to-end testing: create a volume, update with each flag group,
  verify via `volume show`

## Dependencies
- Task 0138 (Volume update handler)

## Files to Create/Modify
- `neonfs-cli/src/commands/volume.rs` (modify — add `Update` subcommand)

## Reference
- `spec/gap-analysis.md` — M-4
- Existing: `neonfs-cli/src/commands/volume.rs` (`Create` subcommand for pattern)
- Existing: `neonfs_client/lib/neon_fs/core/volume.ex` (updatable fields)
- Task 0138 (handler function this calls)

## Notes
Follow the existing `Create` subcommand pattern for term construction. Each
flag is optional — build an `eetf::Map` containing only the flags that were
explicitly provided on the command line. Clap's `Option<T>` type handles
this naturally.

The flag names use kebab-case (`--write-ack`) which maps to snake_case in
the Elixir handler (`write_ack`). Use the same string-key convention as
`volume create`.

Group related flags in the help text using Clap's `help_heading` attribute:
"Tiering", "Caching", "Verification", "Metadata Consistency", "General".
