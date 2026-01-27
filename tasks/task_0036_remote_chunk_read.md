# Task 0036: Implement Remote Chunk Reading

## Status
Not Started

## Phase
2 - Clustering

## Description
Enable reading chunks from remote nodes. When a chunk isn't available locally, fetch it from a node that has it. This completes the distributed read path.

## Acceptance Criteria
- [ ] `NeonFS.Core.ChunkFetcher` module
- [ ] `fetch_chunk/2` - get chunk, local or remote
- [ ] Check local blob store first
- [ ] Look up chunk locations from metadata
- [ ] Fetch from remote node via Erlang distribution
- [ ] Prefer "closest" node (same rack/zone if configured)
- [ ] Handle fetch failures, try alternate locations
- [ ] Cache policy for remote chunks (configurable)
- [ ] Telemetry for local vs remote reads

## Fetch Flow
```elixir
def fetch_chunk(hash, opts) do
  case BlobStore.read_chunk(hash, opts) do
    {:ok, data} ->
      {:ok, data, :local}

    {:error, :not_found} ->
      case ChunkIndex.get(hash) do
        nil ->
          {:error, :chunk_not_found}

        %{locations: locations} ->
          fetch_from_remote(hash, locations, opts)
      end
  end
end

defp fetch_from_remote(hash, locations, opts) do
  # Try each location until success
  Enum.find_value(locations, {:error, :all_replicas_failed}, fn loc ->
    case rpc_read_chunk(loc.node, hash, opts) do
      {:ok, data} -> {:ok, data, {:remote, loc.node}}
      _ -> nil
    end
  end)
end
```

## Testing Strategy
- Unit tests:
  - Local chunk returned directly
  - Remote fetch when not local
  - Fallback to alternate locations
- Integration tests (3-node cluster):
  - Write chunk on node 1
  - Read from node 2 (no local copy)
  - Verify data correct

## Dependencies
- task_0035_basic_replication

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_fetcher.ex` (new)
- `neonfs_core/lib/neon_fs/core/read_operation.ex` (use ChunkFetcher)
- `neonfs_core/test/neon_fs/core/chunk_fetcher_test.exs` (new)

## Reference
- spec/specification.md - Read Path (Simplified)
- spec/metadata.md - Chunk location tracking

## Notes
This enables location-transparent storage - reads work regardless of which node the data is physically on.
