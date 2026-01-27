# Task 0015: Implement Chunk Metadata Structure

## Status
Not Started

## Phase
1 - Foundation

## Description
Define the Elixir-side chunk metadata structure that tracks everything about chunks across the cluster. This includes location tracking, commit state, compression info, and active write references. For Phase 1, this is stored in-memory (ETS); Ra integration comes in Phase 2.

## Acceptance Criteria
- [ ] `NeonFS.Core.ChunkMeta` struct defined with all fields from spec
- [ ] `NeonFS.Core.ChunkIndex` GenServer managing chunk metadata
- [ ] ETS table for fast chunk lookups by hash
- [ ] `put/2` - store/update chunk metadata
- [ ] `get/1` - retrieve by hash
- [ ] `delete/1` - remove metadata
- [ ] `list_by_location/1` - find chunks on a specific node/drive
- [ ] `list_uncommitted/0` - find uncommitted chunks
- [ ] Commit state transitions enforced
- [ ] Active write refs management (add/remove write_id)

## ChunkMeta Struct
```elixir
defmodule NeonFS.Core.ChunkMeta do
  defstruct [
    :hash,              # Binary (32 bytes)
    :original_size,     # Integer
    :stored_size,       # Integer
    :compression,       # :none | :zstd
    :locations,         # [%{node: atom, drive_id: String.t, tier: atom}]
    :target_replicas,   # Integer
    :commit_state,      # :uncommitted | :committed
    :active_write_refs, # MapSet.t(write_id)
    :stripe_id,         # nil | stripe_id (for erasure coding)
    :stripe_index,      # nil | integer
    :created_at,        # DateTime
    :last_verified      # DateTime | nil
  ]
end
```

## Testing Strategy
- Unit tests:
  - CRUD operations on chunk metadata
  - Commit state transitions
  - Active write ref management
  - Query by location
  - Query uncommitted chunks
  - Concurrent access (ETS is concurrent-safe)

## Dependencies
None - can be developed in parallel with blob store tasks

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_meta.ex` (new)
- `neonfs_core/lib/neon_fs/core/chunk_index.ex` (new)
- `neonfs_core/lib/neon_fs/core/application.ex` (add to supervision)
- `neonfs_core/test/neon_fs/core/chunk_index_test.exs` (new)

## Reference
- spec/data-model.md - Chunk Metadata (Elixir Layer)
- spec/replication.md - Uncommitted Chunks and Deduplication

## Notes
This is an in-memory implementation for Phase 1. In Phase 2, this will be backed by Ra consensus for cluster-wide consistency. The API should remain stable across this transition.
