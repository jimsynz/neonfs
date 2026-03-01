# Task 0123: Add `scrub_interval` to Volume `verification_config`

## Status
Complete

## Phase
Gap Analysis — QW-5

## Description
The Volume `verification_config` type currently only has `on_read` and `sampling_rate`. The spec requires a `scrub_interval` field — the duration between full integrity scans of all chunks on a volume. Without this field, the future scrubbing job (M-3) has no per-volume configuration to read.

Add `scrub_interval` to the `verification_config` type with a sensible default.

## Acceptance Criteria
- [ ] `verification_config` type includes `scrub_interval: pos_integer() | nil`
- [ ] `scrub_interval` is measured in seconds
- [ ] `default_verification/0` sets `scrub_interval` to `2_592_000` (30 days)
- [ ] `Volume.validate/1` accepts the new field
- [ ] CLI `volume create` and `volume update` accept `--scrub-interval` flag (in seconds)
- [ ] CLI handler passes `scrub_interval` through to the volume config
- [ ] MetadataStateMachine handles the new field in volume maps (no version bump needed — additive field)
- [ ] Existing tests pass
- [ ] `mix format` passes

## Testing Strategy
- Unit tests in `neonfs_client/test/neon_fs/core/volume_test.exs`:
  - `default_verification/0` includes `scrub_interval: 2_592_000`
  - Volume created with custom `scrub_interval` retains the value
  - Volume created without `scrub_interval` gets the default

## Dependencies
None — standalone fix.

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/core/volume.ex` (modify — add field to type, update default)
- `neonfs_core/lib/neon_fs/cli/handler.ex` (modify — accept scrub_interval in volume create/update)
- `cli/src/commands/volume.rs` (modify — add `--scrub-interval` flag)
- `neonfs_client/test/neon_fs/core/volume_test.exs` (modify — add scrub_interval tests)

## Reference
- `spec/architecture.md` lines 330–388 — verification and scrubbing
- `spec/gap-analysis.md` — QW-5
- Existing: `default_verification/0` in `neonfs_client/lib/neon_fs/core/volume.ex`

## Notes
The scrubbing job itself is tracked in M-3 (gap analysis). This task just adds the configuration field so that the job has something to read when it's implemented. Setting `scrub_interval` to `nil` could mean "never scrub" — but the spec says scrubbing is mandatory, so consider validating that `scrub_interval` is always a positive integer.
