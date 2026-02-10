# Task 0071: Per-Volume Key Management (KeyManager)

## Status
Not Started

## Phase
6 - Security

## Description
Implement the `KeyManager` module that handles per-volume encryption key lifecycle: generating AES-256 volume keys, wrapping them with the cluster master key for storage in Ra, unwrapping them for chunk encryption/decryption, and managing key versions. The master key is read from the local `cluster.json` (established in Phase 2). Volume keys are stored wrapped in Ra via MetadataStateMachine v6 commands.

## Acceptance Criteria
- [ ] New `NeonFS.Core.KeyManager` module (not a GenServer — stateless functions operating on Ra state)
- [ ] `generate_volume_key/1` generates a random 256-bit AES key and wraps it with the master key, stores via Ra, returns `{:ok, key_version}`
- [ ] `get_volume_key/2` takes `(volume_id, key_version)`, reads wrapped key from Ra, unwraps with master key, returns `{:ok, plaintext_key}`
- [ ] `get_current_key/1` takes `volume_id`, determines current key version from volume encryption config, returns `{:ok, {key, version}}`
- [ ] Key wrapping uses AES-256-GCM with the master key (wrap key = encrypt, unwrap key = decrypt)
- [ ] `setup_volume_encryption/1` called during encrypted volume creation — generates first key (version 1), updates volume encryption config
- [ ] `rotate_volume_key/1` generates new key version, updates volume's `current_key_version`, returns `{:ok, new_version}` (does not start re-encryption — that's task 0074)
- [ ] Master key loaded from cluster state on each operation (no caching of master key in process state)
- [ ] Error handling: master key not found, volume not encrypted, unknown key version, unwrap failure (wrong master key)
- [ ] Wrapping nonce is unique per wrapped key (stored alongside wrapped key data)
- [ ] Unit tests for key generation, wrapping/unwrapping round-trip
- [ ] Unit tests for volume key setup and version management
- [ ] Unit test: unwrap with wrong master key returns error

## Testing Strategy
- ExUnit tests with a test master key for wrapping/unwrapping round-trips
- ExUnit tests for volume encryption setup flow (create volume → setup encryption → retrieve key)
- ExUnit tests for key version management (generate v1, rotate to v2, retrieve both)
- ExUnit test for error cases (no master key, wrong master key, unknown version)
- Integration with MetadataStateMachine (verify wrapped keys stored and retrievable via Ra)

## Dependencies
- task_0070 (MetadataStateMachine v6 — encryption key Ra commands)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/key_manager.ex` (new — key generation, wrapping, version management)
- `neonfs_core/test/neon_fs/core/key_manager_test.exs` (new)

## Reference
- spec/security.md — Key Hierarchy section
- spec/security.md — Key Rotation section (key versioning)
- spec/operations.md — Cluster Initialisation (master key generation)

## Notes
The master key is a 256-bit value stored base64-encoded in `/var/lib/neonfs/meta/cluster.json`. It was generated during `cluster init` (Phase 2). The KeyManager reads it via `NeonFS.Cluster.State.load/0`. The master key is never cached in process state or ETS — it is read from disk on each operation to minimise exposure window. In production, users may want to derive the master key from a passphrase or load it from an external secret manager — but the current implementation reads from the local cluster state file, matching the existing Phase 2 behaviour. Key wrapping uses AES-256-GCM (the master key encrypts the volume key) with a random nonce stored alongside the wrapped key in Ra.
