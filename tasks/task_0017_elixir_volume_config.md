# Task 0017: Implement Volume Configuration Structure

## Status
Complete

## Phase
1 - Foundation

## Description
Define the volume configuration structure that holds all settings for a storage volume: durability, tiering, compression, encryption, caching, and verification policies. For Phase 1, implement a basic subset; full policies come in Phase 3.

## Acceptance Criteria
- [ ] `NeonFS.Core.Volume` struct with configuration fields
- [ ] `NeonFS.Core.VolumeRegistry` GenServer managing volumes
- [ ] Create volume with configuration
- [ ] Get volume by name or ID
- [ ] List all volumes
- [ ] Update volume configuration (limited fields)
- [ ] Delete volume (with checks for existing files)
- [ ] Validation of configuration values
- [ ] Default configuration for quick setup

## Volume Struct (Phase 1 Subset)
```elixir
defmodule NeonFS.Core.Volume do
  defstruct [
    :id,                  # Unique volume ID
    :name,                # Human-readable name
    :owner,               # Owner user ID

    # Durability (Phase 1: replication only)
    :durability,          # %{type: :replicate, factor: 3, min_copies: 2}

    # Write acknowledgement
    :write_ack,           # :local | :quorum | :all

    # Tiering (basic for Phase 1)
    :initial_tier,        # :hot | :warm | :cold

    # Compression
    :compression,         # %{algorithm: :zstd | :none, level: 3, min_size: 4096}

    # Verification
    :verification,        # %{on_read: :always | :never | :sampling, ...}

    # Statistics
    :logical_size,        # Bytes after dedup
    :physical_size,       # Actual bytes on disk
    :chunk_count,         # Number of chunks

    :created_at,
    :updated_at
  ]
end
```

## Testing Strategy
- Unit tests:
  - Create volume with valid config
  - Reject invalid config (e.g., replication factor < 1)
  - Get volume by name and ID
  - List volumes
  - Delete empty volume
  - Prevent delete of volume with files
  - Default configuration works

## Dependencies
None - can be developed in parallel

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/volume.ex` (new)
- `neonfs_core/lib/neon_fs/core/volume_registry.ex` (new)
- `neonfs_core/lib/neon_fs/core/application.ex` (add to supervision)
- `neonfs_core/test/neon_fs/core/volume_registry_test.exs` (new)

## Reference
- spec/data-model.md - Volumes section
- spec/data-model.md - Volume Configuration Examples

## Notes
Encryption mode and full tiering policies are deferred to later phases. The struct includes fields for forward compatibility but they may not all be enforced in Phase 1.
