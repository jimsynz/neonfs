# Service Discovery Specification

**Status**: Implemented (Phase 2)
**Phase**: Phase 2 (Distributed)

## Overview

NeonFS uses a service discovery mechanism for non-core nodes (FUSE, S3, Docker, etc.) to locate and communicate with core nodes in a distributed cluster. The implementation lives in the `neonfs_client` package — a pure Elixir library that any cluster participant can depend on without pulling in core's heavy dependencies (Ra, Rustler NIFs, etc.).

## Architecture

### Package Structure

```
neonfs_client/
├── lib/neon_fs/client.ex                # Top-level convenience API
├── lib/neon_fs/client/connection.ex     # Bootstrap node connectivity
├── lib/neon_fs/client/cost_function.ex  # Latency/load-based node selection
├── lib/neon_fs/client/discovery.ex      # Service discovery cache (ETS)
├── lib/neon_fs/client/router.ex         # RPC routing with failover
├── lib/neon_fs/client/service_info.ex   # Service registration data
├── lib/neon_fs/client/service_metrics.ex # Load metrics for routing
└── lib/neon_fs/client/service_type.ex   # Service type identifiers
```

### Dependency Graph

```
neonfs_client  ← neonfs_core  (shared types, service registry)
neonfs_client  ← neonfs_fuse  (service discovery, RPC routing)
neonfs_client  ← neonfs_s3    (future)
neonfs_client  ← neonfs_csi   (future)
```

`neonfs_fuse` has **no dependency** on `neonfs_core`. All communication happens via Erlang distribution, routed through `NeonFS.Client.Router`.

## Components

### NeonFS.Client.Connection

Manages Erlang distribution connections to bootstrap nodes.

- On init, connects to configured bootstrap nodes via `Node.connect/1`
- Monitors connections via `Node.monitor/2`
- Automatically reconnects on `:nodedown` (5s interval)
- Bootstrap nodes configured via child spec option or application env:

```elixir
{NeonFS.Client.Connection, bootstrap_nodes: [:neonfs_core@host1]}
# or
config :neonfs_client, bootstrap_nodes: [:neonfs_core@host1]
```

### NeonFS.Client.Discovery

Discovers and caches service information from the cluster's `ServiceRegistry`.

- Queries `NeonFS.Core.ServiceRegistry.list/0` on a connected core node via RPC
- Caches results in a local ETS table (`:neonfs_client_services`) with `read_concurrency: true`
- Indexes by type (`{:by_type, type}`) and by node (`{:by_node, node}`)
- Subscribes to `:nodedown`/`:nodeup` via `:net_kernel.monitor_nodes/2` for cache invalidation
- Periodic refresh every 5 seconds (configurable)

### NeonFS.Client.CostFunction

Calculates routing costs for core nodes based on latency and load.

- Periodically probes known core nodes (10s interval, configurable)
- Measures latency via `:rpc.call(node, :erlang, :node, [])`
- Fetches CPU load via `:cpu_sup.avg1/0` and run queue via `:erlang.statistics(:run_queue)`
- Composite cost function:

```
cost = 0.3 * latency_score + 0.4 * load_score + 0.3 * queue_score
```

- Supports `prefer_leader: true` option for metadata operations (selects Ra leader if cost is within tolerance)

### NeonFS.Client.Router

Routes RPC calls to the best available core node with retry and failover.

- Uses `CostFunction` for node selection
- Retries on `:badrpc` with failover to next-best node (max 2 retries)
- Falls back to direct discovery if `CostFunction` has no nodes yet
- Two entry points:
  - `call/4` — general-purpose routing
  - `metadata_call/3` — prefers Ra leader node

### NeonFS.Core.ServiceRegistry

Ra-backed service registry running on core nodes.

- Dual-path: ETS for fast reads, Ra for persistence and replication
- Two ETS tables: `:services` (by node) and `:services_by_type` (by type)
- Ra commands: `{:register_service, info}` and `{:deregister_service, node}`
- Monitors remote nodes via `Node.monitor/2`, deregisters on `:nodedown`
- Integrated into `MetadataStateMachine` (v2) alongside existing volume/file commands
- Bootstraps from Ra state on startup (`init_from_ra/0`)

## Node Lifecycle

### Core Node Registration

When a core node starts:
1. `ServiceRegistry` starts as part of the supervision tree
2. Registers itself as `type: :core` via `ServiceRegistry.register/1`
3. Begins accepting discovery queries from non-core nodes

### Non-Core Node Registration

When a FUSE (or other non-core) node starts:
1. `NeonFS.Client.Connection` connects to bootstrap nodes
2. `NeonFS.Client.Discovery` queries `ServiceRegistry` for available services
3. `NeonFS.Client.CostFunction` begins probing core nodes
4. Application optionally registers itself via `NeonFS.Client.register(:fuse, metadata)`

### Cluster Join (Non-Core)

Non-core nodes can join an existing cluster via the invite token mechanism:
1. `Cluster.Join.join_cluster(token, via_node, :fuse)` — type parameter determines behaviour
2. Non-core nodes skip Ra membership (no `ra:add_member`)
3. Instead, they register as services in `ServiceRegistry`
4. They receive the cluster state (ID, name, peers) but don't participate in consensus

### Deregistration

On graceful shutdown:
- `NeonFS.Client.deregister/0` sends RPC to `ServiceRegistry.deregister/1`

On crash / node down:
- `ServiceRegistry` receives `:nodedown` from `Node.monitor/2`
- Automatically deregisters the downed node and removes from ETS
- Persists removal to Ra for cluster-wide consistency

## Supervision Trees

### Non-Core Node (e.g. neonfs_fuse)

```
NeonFS.FUSE.Supervisor
├── NeonFS.Client.Connection     # Connect to bootstrap nodes
├── NeonFS.Client.Discovery      # Cache service registry data
├── NeonFS.Client.CostFunction   # Probe and rank core nodes
├── NeonFS.FUSE.InodeTable
├── NeonFS.FUSE.MountSupervisor
└── NeonFS.FUSE.MountManager
```

### Core Node

```
NeonFS.Core.Supervisor
├── ...existing children...
└── NeonFS.Core.ServiceRegistry  # Ra-backed service registry
```

## Service Types

Defined in `NeonFS.Client.ServiceType`:

| Type | Description |
|------|-------------|
| `:core` | Core storage/metadata node (Ra member) |
| `:fuse` | FUSE filesystem mount node |
| `:s3` | S3-compatible API gateway (future) |
| `:docker` | Docker volume plugin (future) |
| `:csi` | Kubernetes CSI driver (future) |
| `:cifs` | CIFS/SMB share server (future) |

## Configuration

### Bootstrap Nodes

Non-core nodes need at least one bootstrap node to connect to:

```elixir
# In neonfs_fuse config
config :neonfs_fuse, core_node: :neonfs_core@host1

# Or via neonfs_client directly
config :neonfs_client, bootstrap_nodes: [:neonfs_core@host1, :neonfs_core@host2]
```

### Timing Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| Discovery refresh | 5s | How often Discovery re-queries ServiceRegistry |
| CostFunction probe | 10s | How often CostFunction measures latency/load |
| Connection reconnect | 5s | How often Connection retries failed bootstrap nodes |
| Router max retries | 2 | Number of RPC retry attempts before giving up |

## Testing

### Unit Tests (neonfs_client)

77 tests covering:
- Service types, info, and metrics data structures
- Volume and FileMeta shared types (validation, normalisation, round-tripping)
- GenServer initial states (Connection, Discovery, CostFunction)
- Router behaviour when no nodes are reachable

### Integration Tests (neonfs_integration)

10 FUSE handler tests exercising the full client stack end-to-end:
- Connection → Discovery → CostFunction → Router → core node RPC
- File operations (lookup, getattr, create, read, write, mkdir, readdir, unlink, rmdir, rename)
- Error handling for unreachable nodes

## Future Work

- **Leader-aware routing**: `CostFunction.select_core_node(prefer_leader: true)` is implemented but leader detection is not yet wired up — currently selects cheapest node regardless
- **mDNS/DNS-SD**: Zero-configuration bootstrap for local network clusters
- **Cross-datacenter**: Cost function could incorporate datacenter locality
- **Service health status**: `ServiceInfo.status` supports `:online`, `:offline`, `:draining` but draining/offline transitions are not yet automated
- **Read replicas**: Route read-heavy operations to non-leader nodes
