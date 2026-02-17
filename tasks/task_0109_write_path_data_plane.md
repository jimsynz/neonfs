# Task 0109: Write Path Data Plane Migration

## Status
Complete

## Phase
9 - Data Transfer

## Description
Migrate the chunk replication write path from Erlang distribution RPCs to the TLS data plane. When replicating chunks to remote nodes, the write path currently uses `:rpc.call/5` to invoke `BlobStore.write_chunk/4` on the target node (see `NeonFS.Core.Replication.replicate_to_node/4` around line 284). This task changes the replication data flow to use `Router.data_call/4` with `:put_chunk`, sending chunk bytes over dedicated TLS connections while metadata coordination (commit, abort, intent log) remains on Erlang distribution.

This covers both replicated volume writes (`NeonFS.Core.Replication`) and erasure-coded volume writes (`NeonFS.Core.WriteOperation` stripe distribution).

## Acceptance Criteria
- [x] `NeonFS.Core.Replication.replicate_to_node/4` uses `Router.data_call(node, :put_chunk, args)` instead of `:rpc.call(node, BlobStore, :write_chunk, ...)`
- [x] `NeonFS.Core.WriteOperation` stripe distribution uses `Router.data_call(node, :put_chunk, args)` instead of `:rpc.call`
- [x] Metadata operations (commit, abort, intent log writes) remain on Erlang distribution (unchanged)
- [x] Local writes unchanged — still go directly through BlobStore NIF
- [x] Write acknowledgement semantics preserved: `:local` (background), `:quorum` (W of N), `:all` (synchronous) all continue to work correctly
- [x] `put_chunk` responses handled: `:ok` → success, `{:error, :already_exists}` → treated as success (idempotent), `{:error, reason}` → failure
- [x] Timeout matches existing 10-second timeout for remote writes (configurable)
- [x] Graceful fallback: if `data_call` returns `{:error, :no_data_endpoint}`, fall back to distribution RPC via `:rpc.call` (allows rolling upgrades)
- [x] Fallback logged at `:info` level so operators can track upgrade progress
- [x] Existing unit tests for write path and replication continue to pass
- [x] New unit tests for data plane write path

## Testing Strategy
- ExUnit test: replicated write calls `data_call` with `:put_chunk` for remote replicas (mock or verify via test process message)
- ExUnit test: erasure-coded write distributes stripe chunks via `data_call`
- ExUnit test: `:already_exists` response treated as success (no error returned to caller)
- ExUnit test: write failure on one replica does not block others (parallel replication preserved)
- ExUnit test: fallback to `:rpc.call` when `data_call` returns `{:error, :no_data_endpoint}`
- ExUnit test: metadata commit still goes over distribution after data plane chunk transfer
- ExUnit test: timeout handling — write returns error if `data_call` times out
- ExUnit test: local writes bypass data plane entirely (still direct BlobStore NIF)
- ExUnit test: `:local` ack mode still spawns background tasks for replication

## Dependencies
- task_0108 (Router.data_call/4)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/replication.ex` (modify — use `data_call` in `replicate_to_node/4`)
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — use `data_call` for stripe chunk distribution)
- `neonfs_core/test/neon_fs/core/replication_test.exs` (modify — add data plane tests)
- `neonfs_core/test/neon_fs/core/write_operation_test.exs` (modify — add data plane tests, if applicable)

## Reference
- spec/data-transfer.md — Integration with Write Flows section (replicated and erasure-coded)
- spec/data-transfer.md — What Stays on Distribution section
- spec/replication.md — Write flow steps
- Existing `NeonFS.Core.Replication.replicate_to_node/4` (`:rpc.call` at ~line 284)
- Existing `NeonFS.Core.WriteOperation` stripe distribution (`:rpc.call` at ~line 510)

## Notes
The key principle is that **only chunk data** moves to the data plane. All metadata operations (intent log, commit, abort, quorum coordination) remain on Erlang distribution. This is a clean separation: the data plane is a dumb pipe for bytes, while the control plane handles all coordination.

The fallback to `:rpc.call` ensures compatibility during rolling upgrades — when upgrading a cluster node by node, some nodes may not yet have the data transfer listener running. Once all nodes are upgraded, all transfers use the data plane. The fallback can be removed in a future release.

The `put_chunk` message includes `write_id` and `tier` so that the remote Handler can tag the chunk correctly for later commit/abort. Note the Handler currently calls `BlobStore.write_chunk/4` which returns `{:ok, hash, chunk_info}` — the `{:ok, {}}` Rustler pattern should be handled correctly in the response normalisation.

The existing replication code handles three `:rpc.call` return patterns: `{:ok, ...}`, `{:error, reason}`, and `{:badrpc, reason}`. With `data_call`, the patterns become `:ok`, `{:error, reason}`, and `{:error, :no_data_endpoint}` (for fallback). The `{:badrpc, ...}` case is eliminated — connection failures in the pool manifest as checkout timeouts or connection errors.
