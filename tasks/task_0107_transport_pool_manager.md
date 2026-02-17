# Task 0107: Transport.PoolManager and Endpoint Advertisement

## Status
Complete

## Phase
9 - Data Transfer

## Description
Create the `NeonFS.Transport.PoolManager` module in `neonfs_client` that manages per-peer `ConnPool` instances. When a new peer's data transfer endpoint is discovered (via `NeonFS.Client.Discovery`), PoolManager creates a new ConnPool for that peer. When a peer departs, the pool is shut down.

PoolManager watches for `:nodedown` events and periodically checks Discovery for peer data endpoints to create or remove pools. Pool references are stored in an ETS table for fast concurrent lookups from `Router.data_call/4` (task 0108).

This task also provides the `advertise_endpoint/1` helper for constructing the `{host, port}` tuple that nodes include in their ServiceInfo metadata when registering with ServiceRegistry.

## Acceptance Criteria
- [x] New `NeonFS.Transport.PoolManager` module (GenServer) in `neonfs_client`
- [x] `start_link/1` takes opts including `:pool_size` (default 4), `:worker_idle_timeout` (default 30_000)
- [x] Registered with name `NeonFS.Transport.PoolManager`
- [x] `get_pool/1` — takes a node name, returns `{:ok, pool_pid}` or `{:error, :no_pool}` for looking up the ConnPool for a peer
- [x] `ensure_pool/2` — takes a node name and `{host, port}` endpoint, creates a ConnPool if one doesn't already exist for that peer
- [x] `remove_pool/1` — takes a node name, stops and removes the ConnPool for that peer
- [x] Pool references stored in an ETS table (`:neonfs_transport_pools`) for fast concurrent lookups without serialising through the GenServer
- [x] Monitors node connections via `:net_kernel.monitor_nodes/1` to remove pools for departed peers on `:nodedown`
- [x] Periodically refreshes peer endpoints from `NeonFS.Client.Discovery` (configurable interval via `:discovery_refresh_interval`, default 30_000ms)
- [x] On discovery refresh, creates pools for newly discovered peers that have `data_endpoint` in their ServiceInfo metadata
- [x] On discovery refresh, removes pools for peers that are no longer in Discovery
- [x] ConnPool instances supervised under a DynamicSupervisor (`NeonFS.Transport.PoolSupervisor`)
- [x] New `NeonFS.Transport.PoolSupervisor` module (DynamicSupervisor) in `neonfs_client`
- [x] `advertise_endpoint/1` — takes a port number, returns a `{host, port}` tuple using the configured advertise address or auto-detected address
- [x] Auto-detection: if `:advertise` config is empty, uses the first non-loopback IPv4 address from `:inet.getifaddrs/0`
- [x] Configured advertise address read from application env `:neonfs_client, :data_transfer, :advertise`
- [x] PoolManager and PoolSupervisor added to both neonfs_core and neonfs_fuse supervision trees (after Discovery/Connection/CostFunction)
- [x] Type specs on all public functions
- [x] Unit tests for PoolManager

## Testing Strategy
- ExUnit test: `start_link/1` creates the ETS table and starts the GenServer
- ExUnit test: `ensure_pool/2` creates a ConnPool for a peer (provide a local TLS listener as target)
- ExUnit test: `get_pool/1` returns `{:ok, pid}` after pool creation
- ExUnit test: `get_pool/1` returns `{:error, :no_pool}` for unknown peer
- ExUnit test: `remove_pool/1` stops the ConnPool and removes the ETS entry
- ExUnit test: pool removed on `:nodedown` event (simulate with `send(pid, {:nodedown, node, _})`)
- ExUnit test: `advertise_endpoint/1` returns `{host, port}` tuple with configured address
- ExUnit test: `advertise_endpoint/1` auto-detects address when not configured
- ExUnit test: discovery refresh creates pools for new peers (mock Discovery to return peers with `data_endpoint` metadata)
- ExUnit test: duplicate `ensure_pool/2` calls for same peer are idempotent

## Dependencies
- task_0105 (Transport.ConnPool — the pool instances managed by PoolManager)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/transport/pool_manager.ex` (new — PoolManager GenServer)
- `neonfs_client/lib/neon_fs/transport/pool_supervisor.ex` (new — DynamicSupervisor for ConnPool instances)
- `neonfs_core/lib/neon_fs/core/supervisor.ex` (modify — add PoolManager + PoolSupervisor to child list)
- `neonfs_fuse/lib/neon_fs/fuse/supervisor.ex` (modify — add PoolManager + PoolSupervisor to child list)
- `neonfs_client/test/neon_fs/transport/pool_manager_test.exs` (new — unit tests)

## Reference
- spec/data-transfer.md — Connection Pooling section (pool sizing table)
- spec/data-transfer.md — Service Discovery Integration section
- spec/data-transfer.md — Configuration section (advertise address, pool_size)
- Existing `NeonFS.Client.Discovery` module for peer endpoint discovery pattern
- Existing `NeonFS.Transport.CertRenewal` for GenServer-in-neonfs_client supervision pattern

## Notes
PoolManager uses ETS for pool lookups rather than GenServer calls to avoid the GenServer becoming a bottleneck — `Router.data_call/4` can look up pools directly from ETS without serialising through the PoolManager process.

The advertise address logic follows the spec: if configured, use that value; otherwise auto-detect. The spec mentions preferring WireGuard interfaces, but for Phase 9 the simpler "first non-loopback IPv4" heuristic is sufficient. WireGuard preference can be added later.

The DynamicSupervisor (`PoolSupervisor`) ensures ConnPool instances are supervised. If a ConnPool crashes (e.g., all connections fail), the supervisor restarts it and PoolManager's monitor detects the new PID. Use `Process.monitor/1` on each started pool to track crashes and update the ETS table.

Both neonfs_core and neonfs_fuse need PoolManager because both send chunk data to core nodes — core nodes for replication, FUSE nodes for writes and reads. PoolManager must start after Discovery and Connection (it depends on Discovery for peer endpoints) but before any module that might call `Router.data_call/4`.

The periodic discovery refresh catches peers that appeared between `:nodeup` events, or peers whose data endpoints changed. The refresh interval (30 seconds) matches the Discovery cache refresh.
