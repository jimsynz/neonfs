# Task 0045: Volume Policy Configuration

## Status
Complete

## Phase
3 - Policies, Tiering, and Compression

## Description
Restructure the Volume struct to support tiering policies, caching configuration, and I/O weighting. Move the existing `initial_tier` field into a new `tiering` config map alongside `promotion_threshold` and `demotion_delay`. Add `caching` config for transformed data cache limits and `io_weight` for scheduling priority. Update all callers and bump the Ra state machine version with a migration path.

## Acceptance Criteria
- [x] Volume struct has `tiering` map: `%{initial_tier: atom, promotion_threshold: pos_integer, demotion_delay: pos_integer}`
- [x] Volume struct has `caching` map: `%{transformed_chunks: boolean, reconstructed_stripes: boolean, remote_chunks: boolean, max_memory: pos_integer}`
- [x] Volume struct has `io_weight` field (integer, default 100)
- [x] `initial_tier` top-level field removed; all access goes through `volume.tiering.initial_tier`
- [x] Validation in `Volume.validate/1` covers new fields (tier must be `:hot`, `:warm`, or `:cold`; thresholds positive; io_weight > 0)
- [x] Default values set for all new fields (sensible out-of-box: initial_tier `:hot`, promotion_threshold 10, demotion_delay 86400, caching all true, max_memory 256MB, io_weight 100)
- [x] MetadataStateMachine version bumped from current to next with `{:machine_version, old, new}` migration handler
- [x] VolumeRegistry serialisation handles new fields
- [x] All callers of `volume.initial_tier` updated to `volume.tiering.initial_tier`
- [x] WriteOperation uses `volume.tiering.initial_tier` instead of hardcoded `"hot"`
- [x] Replication module updated for new volume structure
- [x] CLI handler volume creation accepts tiering/caching options
- [x] All existing tests pass with updated Volume struct
- [x] New unit tests for validation of tiering, caching, and io_weight fields

## Testing Strategy
- Unit tests for `Volume.new/1` and `Volume.validate/1` with new fields
- Unit tests for Ra state machine migration (version N → N+1)
- Verify serialisation round-trip in VolumeRegistry
- Run full `mix check --no-retry` to catch all broken callers

## Dependencies
- None (first Phase 3 task)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/core/volume.ex` (modify — restructure struct, validation)
- `neonfs_core/lib/neon_fs/core/metadata_state_machine.ex` (modify — version bump, migration)
- `neonfs_core/lib/neon_fs/core/volume_registry.ex` (modify — serialisation)
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — use `volume.tiering.initial_tier`)
- `neonfs_core/lib/neon_fs/core/replication.ex` (modify — updated volume access)
- `neonfs_core/lib/neon_fs/core/handler.ex` (modify — volume creation options)
- `neonfs_client/test/neon_fs/core/volume_test.exs` (modify — new validation tests)
- `neonfs_core/test/neon_fs/core/metadata_state_machine_test.exs` (modify — migration test)

## Reference
- spec/specification.md — Volume configuration
- spec/architecture.md — Volume policies
- spec/implementation.md — Phase 3 deliverables

## Notes
No backward compatibility concerns — this is the first time tiering policy fields are being added. The `initial_tier` field exists but is a simple atom; moving it into a map is a clean restructure. The Ra state machine migration must handle volumes created before this change by wrapping their `initial_tier` value into the new `tiering` map with defaults for the other fields.
