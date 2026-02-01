# Service Discovery

NeonFS consists of multiple service types that need to find each other dynamically. This document describes the service discovery architecture using Ra cluster membership and Erlang distribution.

## Overview

NeonFS services communicate via Erlang distribution. Rather than hardcoding node addresses in configuration, services register themselves with the Ra cluster and discover peers dynamically. This enables:

- **Dynamic scaling**: Add or remove service instances without configuration changes
- **Fault tolerance**: Services automatically discover healthy peers
- **Simplified deployment**: No need to coordinate node addresses across configuration files
- **Location transparency**: Services find each other regardless of network topology

## Service Types

| Service | Application | Role | Discovery Need |
|---------|-------------|------|----------------|
| Core | `neonfs_core` | Storage, metadata, coordination | Discovers other core nodes for replication |
| FUSE | `neonfs_fuse` | FUSE filesystem mounts | Discovers core nodes for data access |
| S3 | `neonfs_s3` | S3-compatible API gateway | Discovers core nodes for data access |
| Docker | `neonfs_docker` | Container volume plugin | Discovers FUSE nodes for mount operations |
| CSI | `neonfs_csi` | Kubernetes CSI driver | Discovers FUSE nodes for mount operations |
| CIFS | `neonfs_cifs` | SMB/CIFS via Samba | Discovers core nodes for data access |

### Service Categories

**Cluster members** (participate in Ra consensus):
- `neonfs_core` nodes form the Ra cluster for metadata consensus

**Cluster clients** (consume services, don't participate in consensus):
- `neonfs_fuse`, `neonfs_s3`, `neonfs_docker`, `neonfs_csi`, `neonfs_cifs`

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Ra Cluster (Core Nodes)                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ core@node1  │──│ core@node2  │──│ core@node3  │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│         │                │                │                      │
│         └────────────────┼────────────────┘                      │
│                          │                                       │
│              ┌───────────┴───────────┐                           │
│              │   Service Registry    │                           │
│              │   (Ra state machine)  │                           │
│              └───────────────────────┘                           │
└─────────────────────────────────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
    ┌─────┴─────┐    ┌─────┴─────┐    ┌─────┴─────┐
    │ fuse@edge1│    │ fuse@edge2│    │ s3@gateway│
    └───────────┘    └───────────┘    └───────────┘
```

## Ra-Based Service Registry

The service registry is stored in the Ra cluster's state machine alongside volume metadata. This provides:

- **Strong consistency**: Service registrations are replicated across all core nodes
- **Durability**: Registrations survive node restarts
- **Atomic updates**: Registration changes are applied atomically
- **Leader forwarding**: Any core node can handle registration requests

### Registry Data Model

```elixir
defmodule NeonFS.Core.ServiceRegistry do
  @type service_type :: :core | :fuse | :s3 | :docker | :csi | :cifs

  @type service_info :: %{
    node: node(),
    type: service_type(),
    registered_at: DateTime.t(),
    last_heartbeat: DateTime.t(),
    metadata: map()
  }

  @type registry :: %{
    services: %{node() => service_info()},
    by_type: %{service_type() => [node()]}
  }
end
```

### Registration Flow

1. Service starts and connects to a known core node (bootstrap node)
2. Service calls `ServiceRegistry.register(type, metadata)`
3. Registration is committed via Ra consensus
4. Service receives confirmation and list of peer services
5. Service sends periodic heartbeats to maintain registration

```elixir
# In neonfs_fuse application startup
def start(_type, _args) do
  # Connect to bootstrap core node
  core_node = Application.get_env(:neonfs_fuse, :bootstrap_node)
  Node.connect(core_node)

  # Register with the cluster
  :ok = NeonFS.Core.ServiceRegistry.register(:fuse, %{
    capabilities: [:mount, :unmount],
    version: Application.spec(:neonfs_fuse, :vsn)
  })

  # Start supervision tree
  Supervisor.start_link(children, opts)
end
```

### Discovery Flow

Services discover peers by querying the registry:

```elixir
# Find all FUSE nodes
{:ok, fuse_nodes} = ServiceRegistry.list_by_type(:fuse)

# Find a specific service
{:ok, service_info} = ServiceRegistry.get(:"neonfs_fuse@edge1")

# Select a healthy core node for an operation
{:ok, core_node} = ServiceRegistry.select_core_node()
```

### Heartbeat and Liveness

Services must send periodic heartbeats to maintain their registration:

```elixir
# Heartbeat interval (configurable)
@heartbeat_interval_ms 30_000

# Service is considered dead after missing heartbeats
@heartbeat_timeout_ms 90_000
```

Core nodes periodically scan for stale registrations and remove them. This ensures the registry reflects the actual state of the cluster.

## Erlang Distribution Layer

Service discovery builds on Erlang's distributed node capabilities:

### Node Connection

When a service registers, it first establishes an Erlang distribution connection:

```elixir
# Connect to bootstrap node
case Node.connect(bootstrap_node) do
  true -> :ok
  false -> {:error, :connection_failed}
  :ignored -> {:error, :distribution_not_started}
end
```

### Node Monitoring

Services monitor connected nodes for failures:

```elixir
# Monitor a node
Node.monitor(peer_node, true)

# Handle nodedown
def handle_info({:nodedown, node}, state) do
  # Remove from local cache, attempt reconnection
  handle_node_failure(node, state)
end
```

### Fallback Discovery

If Ra-based discovery is unavailable (e.g., during bootstrap), services fall back to:

1. **Configured nodes**: Explicit list in application config
2. **Erlang distribution**: `Node.list()` to find connected nodes matching naming patterns
3. **DNS-based discovery**: Query DNS for service records (future)

```elixir
defp discover_core_nodes do
  # Try Ra registry first
  case ServiceRegistry.list_by_type(:core) do
    {:ok, nodes} when nodes != [] ->
      {:ok, nodes}

    _ ->
      # Fallback to configured nodes
      case Application.get_env(:neonfs_fuse, :core_nodes) do
        nodes when is_list(nodes) and nodes != [] ->
          {:ok, nodes}

        _ ->
          # Fallback to Erlang distribution
          nodes = Node.list()
                  |> Enum.filter(&core_node?/1)

          if nodes != [], do: {:ok, nodes}, else: {:error, :no_core_nodes}
      end
  end
end

defp core_node?(node) do
  node |> Atom.to_string() |> String.starts_with?("neonfs_core@")
end
```

## Service-Specific Discovery

### Core → Core

Core nodes discover each other via Ra cluster membership:

```elixir
{:ok, members, _leader} = :ra.members(server_id)
core_nodes = Enum.map(members, fn {_name, node} -> node end)
```

### Core → FUSE

When CLI requests a mount operation, core discovers FUSE nodes:

```elixir
defp get_fuse_node do
  # First try Ra-based registry
  case ServiceRegistry.list_by_type(:fuse) do
    {:ok, [fuse_node | _]} ->
      {:ok, fuse_node}

    _ ->
      # Fallback to Erlang distribution discovery
      case discover_fuse_via_distribution() do
        {:ok, fuse_node} -> {:ok, fuse_node}
        :not_found -> {:error, :fuse_not_available}
      end
  end
end

defp discover_fuse_via_distribution do
  Node.list()
  |> Enum.find(&fuse_node?/1)
  |> case do
    nil -> :not_found
    node -> {:ok, node}
  end
end
```

### FUSE → Core

FUSE nodes discover core nodes for data operations:

```elixir
defp select_core_node do
  case ServiceRegistry.select_core_node() do
    {:ok, node} -> {:ok, node}
    {:error, _} -> select_from_configured_nodes()
  end
end
```

### S3/CIFS → Core

Gateway services use the same pattern as FUSE, selecting core nodes based on:

- **Locality**: Prefer nodes on the same network segment
- **Load**: Distribute requests across available nodes
- **Health**: Avoid nodes with recent failures

## Bootstrap Process

### Initial Cluster Bootstrap

When the first core node starts:

1. Ra cluster initialises with single member
2. ServiceRegistry state machine starts with empty registry
3. Core node registers itself

### Joining Existing Cluster

When a new node joins:

1. New node connects to bootstrap node (from config)
2. For core nodes: Ra membership is updated via `ra:add_member`
3. For client services: Register via ServiceRegistry
4. New node receives current registry state

### Bootstrap Node Configuration

Services need at least one bootstrap node to find the cluster:

```elixir
# config/runtime.exs
config :neonfs_fuse,
  bootstrap_node: System.get_env("NEONFS_BOOTSTRAP_NODE", "neonfs_core@localhost")
                  |> String.to_atom()
```

For production deployments, multiple bootstrap nodes can be specified:

```elixir
config :neonfs_fuse,
  bootstrap_nodes: [
    :"neonfs_core@storage1.example.com",
    :"neonfs_core@storage2.example.com",
    :"neonfs_core@storage3.example.com"
  ]
```

## Implementation Phases

### Phase 1 (Current)

- Erlang distribution-based discovery via `Node.list()`
- FUSE connects to core on startup
- Core discovers FUSE by scanning connected nodes
- Fallback to configured node names

### Phase 2

- Ra-based ServiceRegistry state machine
- Automatic registration on service startup
- Heartbeat-based liveness tracking
- Query API for service discovery

### Phase 3

- Locality-aware node selection
- Load balancing across service instances
- Health scoring and automatic failover
- Metrics and observability for discovery

## Configuration Reference

### Bootstrap Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `NEONFS_BOOTSTRAP_NODE` | Initial node to connect to | `neonfs_core@localhost` |
| `NEONFS_CORE_NODE` | (Deprecated) Alias for bootstrap node | — |

### Registry Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `NEONFS_HEARTBEAT_INTERVAL_MS` | Heartbeat send interval | `30000` |
| `NEONFS_HEARTBEAT_TIMEOUT_MS` | Time before service considered dead | `90000` |
| `NEONFS_DISCOVERY_CACHE_TTL_MS` | Local discovery cache TTL | `5000` |

### Fallback Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `NEONFS_FUSE_NODE` | (Deprecated) Explicit FUSE node for core | — |
| `NEONFS_CORE_NODES` | Comma-separated list of core nodes | — |

## Related Documents

- [Architecture](architecture.md) - System structure and service topology
- [Deployment](deployment.md) - Node configuration and systemd setup
- [Node Management](node-management.md) - Node lifecycle and failure handling
