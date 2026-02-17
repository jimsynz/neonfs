# Task 0106: Transport.Listener and Transport.Handler

## Status
Complete

## Phase
9 - Data Transfer

## Description
Create the `NeonFS.Transport.Listener` and `NeonFS.Transport.Handler` modules in `neonfs_client`. The Listener opens a TLS listening socket with mutual TLS verification, spawns configurable acceptor tasks, and hands accepted connections to Handler processes. The Handler owns an accepted socket, processes inbound chunk operation requests (`put_chunk`, `get_chunk`, `has_chunk`), and dispatches to the local blob store.

The Listener binds to a configurable address and port (default: all interfaces, OS-assigned port). After binding, the actual port is retrievable via `get_port/0` for registration in ServiceRegistry (task 0108).

The Handler uses `{active, N}` for backpressure — the BEAM delivers N frames then pauses until re-armed via `:ssl_passive`, preventing mailbox overflow from burst transfers.

## Acceptance Criteria
- [x] New `NeonFS.Transport.Listener` module (GenServer) in `neonfs_client`
- [x] `Listener.start_link/1` takes opts: `:port` (default 0), `:bind` (default `{0,0,0,0,0,0,0,0}` for dual-stack), `:num_acceptors` (default 4)
- [x] Listener reads TLS paths from `NeonFS.Transport.TLS.tls_dir/0` for `certfile`, `keyfile`, `cacertfile`
- [x] Listener opens an `:ssl.listen/2` socket with mTLS: `verify: :verify_peer`, `fail_if_no_peer_cert: true`, `versions: [:"tlsv1.3"]`, `:binary`, `packet: 4`, `active: false`, `reuseaddr: true`
- [x] Spawns `:num_acceptors` linked acceptor tasks that loop on `:ssl.transport_accept/1` → `:ssl.handshake/2` → hand off to Handler
- [x] `get_port/0` returns the actual listening port (from `:ssl.sockname/1`)
- [x] Listener registered with name `NeonFS.Transport.Listener`
- [x] New `NeonFS.Transport.Handler` module (GenServer) in `neonfs_client`
- [x] `Handler.start_link/1` takes `socket:` option, sets `{active, 10}` on the socket in `init/1`
- [x] Handler processes `{:ssl, _socket, data}` messages by deserialising with `:erlang.binary_to_term(data, [:safe])`
- [x] Handler re-arms active mode on `{:ssl_passive, socket}` with `:ssl.setopts(socket, [{:active, 10}])`
- [x] Handler stops normally on `{:ssl_closed, _}` and `{:ssl_error, _, _}`
- [x] Handler dispatches `{:put_chunk, ref, hash, volume_id, write_id, tier, data}` — calls `NeonFS.Core.BlobStore.write_chunk/4` on the local node, sends `{:ok, ref}` or `{:error, ref, reason}` response
- [x] Handler dispatches `{:get_chunk, ref, hash, volume_id}` — calls `NeonFS.Core.BlobStore.read_chunk/3`, sends `{:ok, ref, chunk_bytes}` or `{:error, ref, :not_found}`
- [x] Handler dispatches `{:has_chunk, ref, hash}` — checks the local blob store, sends `{:ok, ref, tier, size}` or `{:error, ref, :not_found}`
- [x] Responses serialised with `:erlang.term_to_binary/1` and sent via `:ssl.send/2`
- [x] Acceptor hands socket ownership to Handler via `:ssl.controlling_process/2`
- [x] Handler processes supervised under a DynamicSupervisor (`NeonFS.Transport.HandlerSupervisor`)
- [x] TLS handshake timeout of 10 seconds (`:ssl.handshake/2` second argument)
- [x] Failed handshakes logged at `:warning` and do not crash the acceptor
- [x] New `NeonFS.Transport.HandlerSupervisor` module (DynamicSupervisor) in `neonfs_client`
- [x] Listener and HandlerSupervisor added to neonfs_core's supervision tree (after BlobStore, before ServiceRegistry)
- [x] Type specs on all public functions
- [x] Unit tests for Listener and Handler

## Testing Strategy
- ExUnit test: Listener starts and binds to an OS-assigned port (verify `get_port/0` returns a port > 0)
- ExUnit test: Listener accepts a TLS connection (connect with `:ssl.connect/3`, verify handshake succeeds)
- ExUnit test: Handler processes `put_chunk` message and returns `{:ok, ref}` (mock or stub BlobStore)
- ExUnit test: Handler processes `get_chunk` message and returns `{:ok, ref, data}` (mock or stub BlobStore)
- ExUnit test: Handler processes `has_chunk` message and returns `{:ok, ref, tier, size}` (mock or stub BlobStore)
- ExUnit test: Handler returns `{:error, ref, :not_found}` for missing chunk
- ExUnit test: Handler stops on `ssl_closed`
- ExUnit test: Handler re-arms active mode after processing batch (verify `ssl_passive` handling)
- ExUnit test: multiple concurrent connections accepted (connect N clients, all succeed)
- ExUnit test: failed TLS handshake (wrong CA) does not crash acceptor (acceptor continues accepting)

## Dependencies
- All Phase 8 tasks complete (local TLS certificates available)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/transport/listener.ex` (new — Listener GenServer)
- `neonfs_client/lib/neon_fs/transport/handler.ex` (new — Handler GenServer)
- `neonfs_client/lib/neon_fs/transport/handler_supervisor.ex` (new — DynamicSupervisor for Handlers)
- `neonfs_core/lib/neon_fs/core/supervisor.ex` (modify — add Listener + HandlerSupervisor to child list)
- `neonfs_client/test/neon_fs/transport/listener_test.exs` (new — unit tests)
- `neonfs_client/test/neon_fs/transport/handler_test.exs` (new — unit tests)

## Reference
- spec/data-transfer.md — Listener section
- spec/data-transfer.md — Handler section
- spec/data-transfer.md — Transport: TLS with `{packet, 4}` section
- spec/data-transfer.md — Protocol section (message format)
- Existing `NeonFS.Transport.TLS` module for certificate path helpers
- Existing `NeonFS.Core.BlobStore` API: `write_chunk/4`, `read_chunk/3`

## Notes
The Handler calls `NeonFS.Core.BlobStore` functions directly. Since `neonfs_client` has no compile-time dependency on `neonfs_core`, this is a runtime-only dependency — the module is available on core nodes where both packages are loaded. The Listener should only be started on nodes that have a local blob store (core nodes). Non-core nodes (FUSE, S3) use only the outbound pool (ConnPool/PoolManager) and do not start the Listener.

For unit testing the Handler, accept a `:dispatch_module` option (defaulting to `NeonFS.Core.BlobStore`) to allow tests to inject a stub module. This avoids requiring a full BlobStore setup in neonfs_client unit tests.

The DynamicSupervisor (`HandlerSupervisor`) ensures Handler processes are supervised but their failure does not affect the Listener or other Handlers. Each accepted connection gets its own Handler process.

The `{active, 10}` setting provides backpressure — the runtime delivers 10 frames then sends `{:ssl_passive, socket}`, at which point the Handler re-arms. This prevents unbounded mailbox growth if a peer sends faster than the Handler can process.

The Handler's `has_chunk` dispatch needs a blob store function that returns tier and size for a chunk hash. If `BlobStore` does not expose a suitable function, one may need to be added (e.g., `chunk_info/2` returning `{:ok, %{tier: tier, size: size}}`).
