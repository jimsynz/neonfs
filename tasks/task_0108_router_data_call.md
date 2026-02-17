# Task 0108: Router.data_call/4

## Status
Complete

## Phase
9 - Data Transfer

## Description
Extend the existing `NeonFS.Client.Router` module with a `data_call/4` function that routes chunk data operations over the TLS data plane. While the existing `call/4` and `metadata_call/3` route RPCs over Erlang distribution, `data_call/4` uses PoolManager and ConnPool to send chunk operations over dedicated TLS connections.

Also update the ServiceRegistry registration flow so that nodes advertise their data transfer endpoint in their ServiceInfo metadata when they register. This ensures that PoolManager on other nodes can discover the endpoint and create connection pools.

## Acceptance Criteria
- [x] New `Router.data_call/4` function: `data_call(node, operation, args, opts \\ [])`
- [x] `operation` is one of `:put_chunk`, `:get_chunk`, `:has_chunk`
- [x] For `:put_chunk`, `args` is a keyword list with keys: `hash:`, `volume_id:`, `write_id:`, `tier:`, `data:`
- [x] For `:get_chunk`, `args` is a keyword list with keys: `hash:`, `volume_id:`
- [x] For `:has_chunk`, `args` is a keyword list with keys: `hash:`
- [x] Builds the appropriate message tuple (e.g., `{:put_chunk, ref, hash, volume_id, write_id, tier, data}`) with a unique `ref` via `make_ref()`
- [x] Uses `PoolManager.get_pool/1` to find the pool for the target node
- [x] Calls `ConnPool.execute/3` on the pool with the serialised message
- [x] Validates the response `ref` matches the request `ref`, returns `{:error, :ref_mismatch}` otherwise
- [x] Normalises responses: `{:ok, ref}` → `:ok`, `{:ok, ref, data}` → `{:ok, data}`, `{:ok, ref, tier, size}` → `{:ok, %{tier: tier, size: size}}`, `{:error, ref, reason}` → `{:error, reason}`
- [x] Options: `:timeout` (default 30_000), passed through to `ConnPool.execute/3`
- [x] Returns `{:error, :no_data_endpoint}` if no pool exists for the target node
- [x] ServiceInfo registration updated: after Listener starts, include `data_endpoint: {host, port}` in ServiceInfo metadata
- [x] `NeonFS.Cluster.Init.init_cluster/1` registers with `data_endpoint` after Listener is started
- [x] `NeonFS.Cluster.Join.accept_join/4` registers the joining node's data_endpoint if provided
- [x] `NeonFS.Cluster.Join.join_cluster/3` sends the joining node's data_endpoint to the accepting node
- [x] Type specs on all public functions
- [x] Unit tests for `data_call/4`

## Testing Strategy
- ExUnit test: `data_call/4` with `:put_chunk` builds the correct message tuple and returns `:ok` (mock PoolManager and ConnPool)
- ExUnit test: `data_call/4` with `:get_chunk` returns `{:ok, chunk_bytes}`
- ExUnit test: `data_call/4` with `:has_chunk` returns `{:ok, %{tier: tier, size: size}}`
- ExUnit test: `data_call/4` returns `{:error, :not_found}` when remote returns not_found
- ExUnit test: `data_call/4` returns `{:error, :no_data_endpoint}` when no pool exists
- ExUnit test: `data_call/4` returns `{:error, :ref_mismatch}` when response ref doesn't match
- ExUnit test: ServiceInfo registration includes `data_endpoint` in metadata
- End-to-end test: start Listener + PoolManager, create pool to local listener, send and receive via `data_call`

## Dependencies
- task_0107 (Transport.PoolManager — pool lookup via `get_pool/1`)
- task_0105 (Transport.ConnPool — `execute/3` function)
- task_0106 (Transport.Listener/Handler — for end-to-end testing)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/client/router.ex` (modify — add `data_call/4`)
- `neonfs_core/lib/neon_fs/cluster/init.ex` (modify — register data_endpoint after Listener start)
- `neonfs_core/lib/neon_fs/cluster/join.ex` (modify — exchange data_endpoint during join)
- `neonfs_client/test/neon_fs/client/router_data_call_test.exs` (new — unit tests)

## Reference
- spec/data-transfer.md — Router Integration section
- spec/data-transfer.md — Service Discovery Integration section
- spec/data-transfer.md — Registration Flow (5 steps)
- Existing `NeonFS.Client.Router.call/4` and `metadata_call/3` for routing patterns
- Existing `NeonFS.Cluster.Init.init_cluster/1` flow (Ra → system volume → identity → CA → cert → master key → register)
- Existing `NeonFS.Cluster.Join.accept_join/4` and `join_cluster/3` flow

## Notes
Unlike `call/4` which uses CostFunction to select the best node, `data_call/4` takes an explicit target node. Chunk placement is determined by the metadata layer (which node owns which chunk), so the data plane is a point-to-point transfer, not a load-balanced RPC. This is an important distinction — `data_call` is always directed.

The registration flow changes are small but important. The init flow after this task:
1. Initialise Ra cluster
2. Create system volume
3. Write cluster identity
4. Generate cluster CA
5. Issue first node certificate
6. Start Listener (gets port)  ← NEW
7. Generate master encryption key
8. Register first node with `data_endpoint` in metadata  ← MODIFIED

For `join_cluster/3`, the joining node should start its Listener before requesting to join, so it can include its data_endpoint in the join request. The accepting node includes the endpoint in the ServiceRegistry entry for the joining node.

The response normalisation in `data_call/4` makes the API cleaner for callers — they get consistent `{:ok, result}` or `{:error, reason}` tuples without needing to know about request refs or raw protocol details.
