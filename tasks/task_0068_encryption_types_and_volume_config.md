# Task 0068: Encryption Types and Volume Config Extension

## Status
Not Started

## Phase
6 - Security

## Description
Create encryption-related types in neonfs_client and extend the Volume struct to support encryption configuration. This adds `VolumeEncryption` (per-volume encryption state with key versioning) and `ChunkCrypto` (per-chunk encryption metadata) structs. The Volume struct gains an `encryption` field that controls whether chunks are encrypted at rest. These types are shared across packages via neonfs_client.

## Acceptance Criteria
- [ ] New `NeonFS.Core.VolumeEncryption` struct in neonfs_client with fields: `mode` (`:none` or `:server_side`), `current_key_version` (integer), `keys` (map of version => wrapped key data), `rotation` (nil or rotation state)
- [ ] `VolumeEncryption.new/1` creates a new encryption config, `VolumeEncryption.active?/1` returns whether encryption is enabled
- [ ] New `NeonFS.Core.ChunkCrypto` struct in neonfs_client with fields: `algorithm` (`:aes_256_gcm`), `nonce` (12-byte binary), `key_version` (integer)
- [ ] Volume struct extended with `encryption` field (default: `%VolumeEncryption{mode: :none}`)
- [ ] `Volume.encrypted?/1` helper returns true if encryption mode is not `:none`
- [ ] `VolumeEncryption` validation: mode must be `:none` or `:server_side`, key_version must be positive integer when mode is `:server_side`
- [ ] Rotation state struct: `from_version`, `to_version`, `started_at`, `progress` (total_chunks, migrated)
- [ ] Wrapped key entry struct: `wrapped_key` (binary), `created_at` (DateTime), `deprecated_at` (nil or DateTime)
- [ ] All existing Volume tests pass (encryption defaults to `:none`)
- [ ] Unit tests for VolumeEncryption creation, validation, and helpers
- [ ] Unit tests for ChunkCrypto creation

## Testing Strategy
- ExUnit tests for VolumeEncryption struct creation with various modes
- ExUnit tests for Volume with encryption config (backwards compatible — defaults to none)
- ExUnit tests for ChunkCrypto struct creation and field validation
- Verify existing volume tests pass unchanged

## Dependencies
- None (independent, defines types only)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/core/volume_encryption.ex` (new — VolumeEncryption struct and helpers)
- `neonfs_client/lib/neon_fs/core/chunk_crypto.ex` (new — ChunkCrypto struct)
- `neonfs_client/lib/neon_fs/core/volume.ex` (modify — add `encryption` field)
- `neonfs_client/test/neon_fs/core/volume_encryption_test.exs` (new)
- `neonfs_client/test/neon_fs/core/chunk_crypto_test.exs` (new)

## Reference
- spec/security.md — Data Encryption section, Key Versioning section
- spec/security.md — Key Rotation section (VolumeEncryption rotation state)

## Notes
The `mode: :envelope` is intentionally excluded from this phase — only `:none` and `:server_side` are supported. The wrapped key entries store volume keys encrypted with the cluster master key. The `keys` map is keyed by version number, allowing multiple key versions to coexist during rotation. Old key versions are needed to read chunks encrypted with previous keys. The `ChunkCrypto` struct is intentionally lightweight — it stores just enough to locate the correct key and decrypt the chunk.
