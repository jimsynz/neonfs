# Task 0018: Implement Basic Write Path

## Status
Not Started

## Phase
1 - Foundation

## Description
Implement the write path that takes file data, chunks it, stores chunks in the blob store, and creates file metadata. This is the core data flow for writing files. For Phase 1, this is single-node only (no replication).

## Acceptance Criteria
- [ ] `NeonFS.Core.WriteOperation` module handling writes
- [ ] `write_file/4` - (volume, path, data, opts) -> {:ok, file_meta}
- [ ] Generate unique write_id for tracking
- [ ] Chunk data using blob store chunking
- [ ] Compute hash for each chunk
- [ ] Check for existing chunks (deduplication)
- [ ] Store new chunks with uncommitted state
- [ ] Create file metadata with chunk list
- [ ] Commit chunks on successful write
- [ ] Cleanup on failure (abort uncommitted chunks)
- [ ] Respect volume compression settings
- [ ] Telemetry events for write operations

## Write Flow
```elixir
def write_file(volume, path, data, opts \\ []) do
  write_id = generate_write_id()

  with {:ok, chunks} <- chunk_and_store(data, volume, write_id),
       {:ok, file_meta} <- create_file_metadata(volume, path, chunks),
       :ok <- commit_chunks(write_id, Enum.map(chunks, & &1.hash)) do
    {:ok, file_meta}
  else
    {:error, reason} ->
      abort_chunks(write_id)
      {:error, reason}
  end
end
```

## Testing Strategy
- Unit tests:
  - Write small file (single chunk)
  - Write large file (multiple chunks)
  - Write with compression enabled
  - Deduplication: write same data twice, verify single chunk
  - Failed write cleans up uncommitted chunks
  - Telemetry events emitted

## Dependencies
- task_0014_elixir_blob_wrapper
- task_0015_elixir_chunk_metadata
- task_0016_elixir_file_metadata
- task_0017_elixir_volume_config

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (new)
- `neonfs_core/test/neon_fs/core/write_operation_test.exs` (new)

## Reference
- spec/replication.md - Write Flow (Replicated Volume)
- spec/replication.md - Uncommitted Chunks and Deduplication

## Notes
Replication across nodes comes in Phase 2. This task focuses on the single-node write path with proper chunk lifecycle management.
