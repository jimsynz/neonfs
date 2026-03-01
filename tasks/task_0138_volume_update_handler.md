# Task 0138: Volume Update CLI Handler

## Status
Complete

## Phase
Gap Analysis — M-4 (1/2)

## Description
Add a `handle_volume_update/2` function to the CLI handler so that volume
configuration can be changed at runtime. Currently `volume create` sets all
fields, but there is no way to update an existing volume's configuration
without editing the metadata state machine directly.

The `VolumeRegistry.update/2` function already exists and accepts keyword
opts that are passed to `Volume.update/2`. This task exposes that
functionality through the CLI handler.

The handler should accept a volume name and a configuration map, resolve
the volume, validate the requested changes, and call
`VolumeRegistry.update/2`.

## Acceptance Criteria
- [ ] `handle_volume_update/2` exists in `NeonFS.CLI.Handler`
- [ ] Accepts `(name, config)` where `config` is a map of fields to update
- [ ] Resolves volume by name via `VolumeRegistry.get_by_name/1`
- [ ] Returns `{:error, :volume_not_found}` for unknown volumes
- [ ] Supports updating: `compression`, `write_ack`, `io_weight`, `atime_mode`
- [ ] Supports updating tiering config: `initial_tier`, `promotion_threshold`, `demotion_delay`
- [ ] Supports updating caching config: `transformed_chunks`, `reconstructed_stripes`, `remote_chunks`
- [ ] Supports updating verification config: `on_read`, `sampling_rate`, `scrub_interval`
- [ ] Supports updating metadata consistency: `metadata_replicas`, `read_quorum`, `write_quorum`
- [ ] Does NOT allow updating: `name`, `id`, `durability` (mode/replicas/data_chunks/parity_chunks), `encryption` (use key rotation instead)
- [ ] Returns `{:ok, volume}` with the updated volume on success
- [ ] Returns `{:error, reason}` on validation failure
- [ ] Telemetry event `[:neonfs, :cli, :volume_updated]` emitted on success
- [ ] Unit test: update compression on an existing volume
- [ ] Unit test: update tiering config
- [ ] Unit test: update verification config
- [ ] Unit test: reject update for non-existent volume
- [ ] Unit test: reject attempt to change immutable fields
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/cli/handler_test.exs` (extend):
  - Create a volume, then update various config fields
  - Verify the returned volume reflects the changes
  - Verify immutable fields are rejected
  - Verify volume not found error

## Dependencies
- None (VolumeRegistry.update/2 and Volume.update/2 already exist)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cli/handler.ex` (modify — add `handle_volume_update/2`)
- `neonfs_core/test/neon_fs/cli/handler_test.exs` (modify — add update tests)

## Reference
- `spec/gap-analysis.md` — M-4
- Existing: `neonfs_core/lib/neon_fs/cli/handler.ex` (`create_volume/2` for pattern)
- Existing: `neonfs_core/lib/neon_fs/core/volume_registry.ex` (`update/2`)
- Existing: `neonfs_client/lib/neon_fs/core/volume.ex` (`update/2` with supported fields)

## Notes
The config map from the CLI will use string keys (from Erlang term parsing).
The handler needs to convert string keys to the keyword list format that
`VolumeRegistry.update/2` expects. Follow the same key conversion pattern
used in `create_volume/2`.

Durability changes (switching between replication and erasure coding, or
changing replica count) are excluded because they require data migration.
Encryption changes are excluded because they go through key rotation. These
restrictions should be documented in the CLI help text.
