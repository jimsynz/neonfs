# Task 0014: Implement Elixir Blob Store Wrapper Module

## Status
Not Started

## Phase
1 - Foundation

## Description
Create a higher-level Elixir wrapper around the blob store NIF that provides a clean API for the rest of the system. This module manages the blob store resource lifecycle and provides convenient functions with proper error handling and telemetry.

## Acceptance Criteria
- [ ] `NeonFS.Core.BlobStore` module wrapping NIF calls
- [ ] `start_link/1` for supervised blob store initialisation
- [ ] GenServer holding the blob store resource
- [ ] `write_chunk/3` - write data, return hash and chunk info
- [ ] `read_chunk/2` - read by hash, return data
- [ ] `read_chunk/3` - read with options (verify, decompress)
- [ ] `delete_chunk/2` - delete by hash
- [ ] `migrate_chunk/3` - move between tiers
- [ ] `chunk_data/2` - split data into chunks
- [ ] Proper error tuple returns `{:ok, result} | {:error, reason}`
- [ ] Telemetry events for operations (latency, bytes, errors)
- [ ] Documentation with examples

## Module Structure
```elixir
defmodule NeonFS.Core.BlobStore do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def write_chunk(data, tier, opts \\ []) do
    GenServer.call(__MODULE__, {:write_chunk, data, tier, opts})
  end

  # ... other functions
end
```

## Testing Strategy
- Unit tests using actual blob store (temp directory):
  - Write and read roundtrip
  - Verify option works correctly
  - Compression roundtrip
  - Error handling for missing chunks
  - Telemetry events emitted
- Test supervision tree restart behaviour

## Dependencies
- task_0007_blob_compression (full blob store NIF)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/blob_store.ex` (new)
- `neonfs_core/lib/neon_fs/core/application.ex` (add to supervision tree)
- `neonfs_core/test/neon_fs/core/blob_store_test.exs` (new)

## Reference
- spec/architecture.md - Blob Store operations
- spec/data-model.md - Chunk operations

## Notes
The GenServer approach provides a clean boundary for telemetry and ensures proper resource cleanup. The blob store resource is held in the GenServer state.
