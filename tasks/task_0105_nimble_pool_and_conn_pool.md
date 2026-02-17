# Task 0105: nimble_pool Dependency and Transport.ConnPool

## Status
Complete

## Phase
9 - Data Transfer

## Description
Add the `nimble_pool` Hex package as a dependency of `neonfs_client` and create the `NeonFS.Transport.ConnPool` module implementing the `NimblePool` behaviour. This module manages a pool of persistent TLS connections to a single peer node, handling async connection establishment, checkout/checkin, health checks, and automatic replacement of dead connections.

ConnPool is the outbound connection primitive — each peer node gets its own ConnPool instance (managed by PoolManager in task 0107). The pool uses the cluster CA certificates from Phase 8 for mutual TLS authentication.

## Acceptance Criteria
- [x] `nimble_pool` added to `neonfs_client/mix.exs` dependencies (`~> 1.1`)
- [x] `:ssl` added to `extra_applications` in `neonfs_client/mix.exs`
- [x] New `NeonFS.Transport.ConnPool` module in `neonfs_client`
- [x] Implements all required `NimblePool` callbacks: `init_pool/1`, `init_worker/1`, `handle_checkout/4`, `handle_checkin/4`, `handle_info/2`, `handle_ping/2`, `terminate_worker/3`
- [x] `init_worker/1` uses `{:async, fn -> ... end, pool_state}` for non-blocking TLS connection establishment via `:ssl.connect/3`
- [x] TLS options: `versions: [:"tlsv1.3"]`, `verify: :verify_peer`, `fail_if_no_peer_cert: true`, `cacertfile:` from local CA cert, `certs_keys:` from local node cert/key, `:binary`, `packet: 4`, `active: false`
- [x] `handle_checkout/4` returns the socket for use
- [x] `handle_checkin/4` accepts the socket back after use
- [x] `handle_info/2` handles `{:ssl_closed, _}` and `{:ssl_error, _, _}` with `{:remove, reason}`
- [x] `handle_ping/2` verifies the connection is alive via `:ssl.connection_information/1`
- [x] `terminate_worker/3` closes the socket via `:ssl.close/1`
- [x] `start_link/1` convenience function wrapping `NimblePool.start_link/1` with defaults (`lazy: false`, configurable `pool_size` and `worker_idle_timeout`)
- [x] `execute/3` public function — checks out a socket, sends a serialised message via `:ssl.send/2`, receives response via `:ssl.recv/3`, deserialises with `:erlang.binary_to_term(data, [:safe])`, checks in socket, returns response
- [x] `execute/3` options: `:timeout` for checkout (default 30_000), `:recv_timeout` for recv (default 30_000)
- [x] Pool state includes `peer: {host, port}` and `ssl_opts` derived from local TLS files
- [x] SSL option paths read from `NeonFS.Transport.TLS.tls_dir/0`
- [x] Type specs on all public functions
- [x] Unit tests for ConnPool behaviour

## Testing Strategy
- ExUnit test: `start_link/1` creates a pool (set up a local `:ssl.listen` as echo target in test setup)
- ExUnit test: `execute/3` sends a message and receives a response (echo server in test)
- ExUnit test: pool replaces dead connections (close socket server-side, verify next checkout gets a fresh connection)
- ExUnit test: `handle_ping` detects stale connections
- ExUnit test: `handle_info` removes connections on `ssl_closed` and `ssl_error`
- ExUnit test: pool initialises all connections eagerly (`lazy: false`)
- ExUnit test: checkout times out when pool is exhausted (all connections in use)
- ExUnit test: SSL options correctly use local TLS certificate paths

## Dependencies
- All Phase 8 tasks complete (local TLS certificates available via `NeonFS.Transport.TLS`)

## Files to Create/Modify
- `neonfs_client/mix.exs` (modify — add `{:nimble_pool, "~> 1.1"}` dependency, add `:ssl` to `extra_applications`)
- `neonfs_client/lib/neon_fs/transport/conn_pool.ex` (new — NimblePool behaviour)
- `neonfs_client/test/neon_fs/transport/conn_pool_test.exs` (new — unit tests)

## Reference
- spec/data-transfer.md — Connection Pooling section
- spec/data-transfer.md — Security: Mutual TLS section
- NimblePool documentation (https://hexdocs.pm/nimble_pool/)
- Existing `NeonFS.Transport.TLS` module for certificate path management

## Notes
The `execute/3` function implements the checkout-send-recv-checkin pattern from the spec's Transfer Flow section. During checkout, the socket uses `{active, false}` for synchronous send/recv. The Handler (task 0106) uses `{active, N}` for asynchronous inbound processing on the server side.

The pool_size default is 4 connections per peer, configurable via application env `:neonfs_client, :data_transfer, :pool_size`.

For unit testing, create a minimal TLS echo server in test setup using `:ssl.listen/2` and `:ssl.transport_accept/1` with test certificates generated via `NeonFS.Transport.TLS.generate_ca/1` and `sign_csr/5`. This avoids dependency on the full Listener module.

`nimble_pool` is pure Elixir with zero dependencies — consistent with the project's preference for minimal dependency footprint.
