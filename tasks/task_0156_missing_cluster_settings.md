# Task 0156: Missing Cluster-Level Settings and Startup Wiring

## Status
Complete

## Phase
Gap Analysis — M-12 (2/2)

## Description
Add the missing cluster-level settings to `Cluster.State` that the spec
defines but don't exist yet, and wire them into the modules that need them.
Also improve the application startup to log specific validation failures
instead of silently ignoring them.

The spec defines several cluster-level settings that should live in
`cluster.json`:
- `peer_sync_interval` — how often to sync the peer list
- `peer_connect_timeout` — timeout for establishing node connections
- `min_peers_for_operation` — minimum connected peers before accepting writes
- `startup_peer_timeout` — how long to wait for peers on boot

Additionally, `Application.start/2` currently silently ignores config
loading failures (`_ -> :ok`). It should log a warning with specific
details.

## Acceptance Criteria
- [ ] `Cluster.State` struct includes `peer_sync_interval` field (default: 30_000 ms)
- [ ] `Cluster.State` struct includes `peer_connect_timeout` field (default: 10_000 ms)
- [ ] `Cluster.State` struct includes `min_peers_for_operation` field (default: 1)
- [ ] `Cluster.State` struct includes `startup_peer_timeout` field (default: 30_000 ms)
- [ ] `parse_state/1` reads these fields from `cluster.json` with defaults for missing values
- [ ] `Application.load_config_from_cluster_state/0` puts new settings into app env
- [ ] `NeonFS.Client.Connection` reads `peer_connect_timeout` from app env
- [ ] `NeonFS.Client.Discovery` reads `peer_sync_interval` from app env
- [ ] `Application.start/2` logs a `Logger.warning` with specific error details when config loading fails
- [ ] `Application.start/2` logs the validation errors from task 0155 if validation fails
- [ ] Missing fields in `cluster.json` use defaults (backward compatible)
- [ ] Validator (task 0155) extended to validate new fields
- [ ] Unit test: new fields are read from cluster.json correctly
- [ ] Unit test: defaults are used when fields are missing
- [ ] Unit test: Connection uses configured peer_connect_timeout
- [ ] Unit test: Discovery uses configured peer_sync_interval
- [ ] Existing tests still pass
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/cluster/state_test.exs` (extend):
  - Parse a cluster.json with new fields, verify they're in the struct
  - Parse a cluster.json without new fields, verify defaults
- Unit tests for Connection and Discovery config reading
- Verify application startup logs warnings for invalid config (capture log)

## Dependencies
- Task 0155 (Cluster.State validator — for extending validation)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cluster/state.ex` (modify — add new fields, update parse_state)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — load new settings, improve error logging)
- `neonfs_client/lib/neon_fs/client/connection.ex` (modify — read peer_connect_timeout from app env)
- `neonfs_client/lib/neon_fs/client/discovery.ex` (modify — read peer_sync_interval from app env)
- `neonfs_core/lib/neon_fs/cluster/state/validator.ex` (modify — validate new fields)
- `neonfs_core/test/neon_fs/cluster/state_test.exs` (modify — add tests for new fields)

## Reference
- `spec/operations.md` — configuration management
- `spec/node-management.md`
- `spec/gap-analysis.md` — M-12
- Existing: `neonfs_core/lib/neon_fs/cluster/state.ex`
- Existing: `neonfs_core/lib/neon_fs/core/application.ex` (`load_config_from_cluster_state/0`)

## Notes
These settings are intentionally conservative in their defaults:
- `peer_sync_interval: 30_000` — sync every 30 seconds
- `peer_connect_timeout: 10_000` — 10 second connection timeout
- `min_peers_for_operation: 1` — require at least 1 peer (plus self) for writes
- `startup_peer_timeout: 30_000` — wait up to 30 seconds for peers on boot

The `min_peers_for_operation` setting is informational for now — it can be
used by future write-path checks to reject writes when insufficient peers
are connected. For this task, just load it into app env and make it
available.

Backward compatibility is critical: existing `cluster.json` files without
these new fields must still load correctly. All new fields have defaults
in `parse_state/1`.
