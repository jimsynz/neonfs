# Task 0019: Implement Basic Read Path

## Status
Not Started

## Phase
1 - Foundation

## Description
Implement the read path that takes a file path, looks up metadata, fetches chunks from the blob store, and assembles the data. This handles offset and length parameters for partial reads.

## Acceptance Criteria
- [ ] `NeonFS.Core.ReadOperation` module handling reads
- [ ] `read_file/3` - (volume, path, opts) -> {:ok, data}
- [ ] `read_file/4` - (volume, path, offset, length) -> {:ok, data}
- [ ] Look up file metadata by path
- [ ] Determine which chunks are needed for the read range
- [ ] Fetch chunks from blob store
- [ ] Handle decompression based on chunk metadata
- [ ] Assemble data from chunks, respecting offset/length
- [ ] Verification based on volume settings
- [ ] Telemetry events for read operations
- [ ] Proper error handling (file not found, chunk not found)

## Read Flow
```elixir
def read_file(volume, path, offset \\ 0, length \\ :all) do
  with {:ok, file_meta} <- FileIndex.get_by_path(volume.id, path),
       {:ok, needed_chunks} <- calculate_needed_chunks(file_meta, offset, length),
       {:ok, chunk_data} <- fetch_chunks(needed_chunks, volume),
       {:ok, data} <- assemble_data(chunk_data, file_meta, offset, length) do
    {:ok, data}
  end
end

defp calculate_needed_chunks(file_meta, offset, length) do
  # Determine which chunks contain the requested byte range
  # Return list of {chunk_hash, chunk_offset, bytes_needed}
end
```

## Testing Strategy
- Unit tests:
  - Read entire small file
  - Read entire large file (multiple chunks)
  - Partial read (offset, length)
  - Read spanning chunk boundaries
  - Read with verification enabled
  - File not found error
  - Chunk not found error (data loss scenario)

## Dependencies
- task_0014_elixir_blob_wrapper
- task_0015_elixir_chunk_metadata
- task_0016_elixir_file_metadata
- task_0018_elixir_write_path (for creating test data)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/read_operation.ex` (new)
- `neonfs_core/test/neon_fs/core/read_operation_test.exs` (new)

## Reference
- spec/specification.md - Read Path (Simplified)
- spec/data-model.md - When a client reads a file

## Notes
Remote chunk fetching comes in Phase 2. This task assumes all chunks are local. The architecture supports adding remote fetching without changing the API.
