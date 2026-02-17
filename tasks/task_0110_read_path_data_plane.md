# Task 0110: Read Path Data Plane Migration

## Status
Complete

## Phase
9 - Data Transfer

## Description
Migrate the remote chunk retrieval read path from Erlang distribution RPCs to the TLS data plane. When reading a chunk that is not available locally, the read path currently uses `:rpc.call/5` to invoke `BlobStore.read_chunk_with_options/4` on the target node (see `NeonFS.Core.ChunkFetcher` around line 310). This task changes remote chunk retrieval to use `Router.data_call/4` with `:get_chunk`, fetching chunk bytes over dedicated TLS connections.

This covers both replicated and erasure-coded read paths, including degraded reads where chunks are fetched from multiple remote nodes for reconstruction.

## Acceptance Criteria
- [x] `NeonFS.Core.ChunkFetcher` remote reads use `Router.data_call(node, :get_chunk, args)` instead of `:rpc.call(node, BlobStore, :read_chunk_with_options, ...)`
- [x] Erasure-coded degraded reads fetch available chunks from multiple remote nodes via `data_call`
- [x] Local reads unchanged — still go directly through BlobStore NIF
- [x] Chunk verification (hash check) still performed after retrieval (unchanged)
- [x] Parallel remote reads preserved — multiple chunks fetched concurrently via `Task.async_stream` or equivalent
- [x] `get_chunk` responses handled: `{:ok, chunk_bytes}` → success, `{:error, :not_found}` → try next replica
- [x] Timeout of 10 seconds for remote reads (matching existing `:rpc.call` timeout)
- [x] Graceful fallback: if `data_call` returns `{:error, :no_data_endpoint}`, fall back to distribution RPC via `:rpc.call` (allows rolling upgrades)
- [x] Fallback logged at `:info` level
- [x] Chunk cache integration unchanged — cached chunks bypass remote retrieval
- [x] Location scoring/preference unchanged — still sorts by tier preference (local SSD=0, remote SSD=10, etc.)
- [x] Existing unit tests for read path continue to pass
- [x] New unit tests for data plane read path

## Testing Strategy
- ExUnit test: remote chunk read calls `data_call` with `:get_chunk` for the target node
- ExUnit test: chunk hash verified after data plane retrieval
- ExUnit test: `:not_found` response triggers fallback to next replica in the location list
- ExUnit test: parallel reads from multiple nodes work correctly (Task.async_stream pattern)
- ExUnit test: degraded erasure read fetches sufficient chunks for reconstruction via `data_call`
- ExUnit test: fallback to `:rpc.call` when `data_call` returns `{:error, :no_data_endpoint}`
- ExUnit test: local reads bypass data plane entirely
- ExUnit test: cached chunks bypass remote retrieval entirely
- ExUnit test: timeout handling — returns error and tries next location on timeout

## Dependencies
- task_0108 (Router.data_call/4)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_fetcher.ex` (modify — use `data_call` for remote chunk retrieval)
- `neonfs_core/test/neon_fs/core/chunk_fetcher_test.exs` (modify — add data plane tests)

## Reference
- spec/data-transfer.md — Integration with Read Path section
- spec/data-transfer.md — What Stays on Distribution section
- spec/replication.md — Read flow
- Existing `NeonFS.Core.ChunkFetcher.try_fetch_from_location/5` (`:rpc.call` at ~line 310)
- Existing location scoring in `ChunkFetcher.fetch_from_remote/5` (~line 224)

## Notes
The read timeout is shorter than the write timeout (10s vs 30s in the spec, but the existing code uses 10s for both). Since reads should be fast — a chunk is either available or it's not — keeping the existing 10-second timeout is appropriate. If a node is slow to respond, the location scoring should favour alternative replicas.

The fallback to `:rpc.call` ensures compatibility during rolling upgrades, matching the write path behaviour from task 0109.

For erasure-coded reads in degraded mode, the read path fetches chunks from multiple nodes in parallel. The change is mechanical — replacing `:rpc.call` with `data_call` — but verify that the parallel fetch pattern works correctly with the connection pool (each concurrent fetch should get its own pooled connection via separate `execute/3` calls).

The `ChunkFetcher` currently returns `{:ok, data, {:remote, node}}` for successful remote reads. This return value should remain unchanged — only the transport mechanism changes, not the API contract.
