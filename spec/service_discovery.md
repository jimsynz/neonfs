# Service Discovery Specification

**Status**: Draft
**Phase**: Future (post Phase 2)

## Overview

NeonFS requires a service discovery mechanism for FUSE nodes to locate and communicate with Core nodes in a distributed cluster. The current implementation uses static configuration, which doesn't scale to multi-node deployments or handle node failures gracefully.

## Current State (Temporary)

`MountManager` in neonfs_fuse currently:
1. Checks if `VolumeRegistry` is running locally (same node)
2. Falls back to RPC to a configured `core_node` if not local

This works for:
- Single-node deployments
- Test environments where both apps run on the same node

This does NOT work for:
- Multi-node production deployments
- Automatic failover when a core node goes down
- Load balancing across multiple core nodes

## Requirements

### Functional Requirements

1. **Node Discovery**: FUSE nodes must discover available Core nodes without static configuration
2. **Leader Awareness**: For Ra-backed operations, FUSE nodes should route requests to the Ra leader when possible
3. **Failover**: If a Core node becomes unavailable, FUSE nodes must automatically route to another available node
4. **Health Checking**: Unhealthy nodes should be excluded from routing decisions

### Non-Functional Requirements

1. **Low Latency**: Service discovery should not add significant latency to operations
2. **Consistency**: All FUSE nodes should eventually converge on the same view of available Core nodes
3. **Partition Tolerance**: Service discovery should degrade gracefully during network partitions

## Proposed Architecture

### Node Registry

A distributed registry of available nodes, stored in Ra state:

```elixir
defmodule NeonFS.Core.NodeRegistry do
  @moduledoc """
  Distributed registry of NeonFS nodes, backed by Ra consensus.
  """

  @type node_info :: %{
    node: node(),
    roles: [:core | :fuse],
    cost: cost_function_result(),
    last_heartbeat: DateTime.t(),
    capabilities: [atom()]
  }

  @type cost_function_result :: %{
    load: float(),          # 0.0 - 1.0, current load
    latency_ms: integer(),  # estimated latency from requester
    capacity: integer(),    # available capacity units
    priority: integer()     # static priority (for preferred nodes)
  }

  @doc "Register this node with the cluster"
  @spec register(roles :: [atom()]) :: :ok | {:error, term()}

  @doc "Deregister this node from the cluster"
  @spec deregister() :: :ok

  @doc "List all nodes with a given role"
  @spec list_nodes(role :: atom()) :: [node_info()]

  @doc "Select the best node for a given operation"
  @spec select_node(role :: atom(), opts :: keyword()) :: {:ok, node()} | {:error, :no_nodes}
end
```

### Cost Functions

Node selection should consider multiple factors:

```elixir
defmodule NeonFS.Core.NodeCost do
  @moduledoc """
  Cost functions for node selection.

  Lower cost = more preferred node.
  """

  @doc """
  Calculate composite cost for a node.

  Factors:
  - load: Current CPU/memory utilisation (0.0 - 1.0)
  - latency: Network round-trip time to the node
  - queue_depth: Number of pending operations
  - is_leader: Whether this node is the Ra leader (bonus for metadata ops)
  """
  @spec calculate(node_info(), operation_type()) :: float()

  @doc """
  Update cost metrics for the local node.
  Called periodically by a background process.
  """
  @spec update_local_metrics() :: :ok
end
```

### Integration with Ra

For metadata operations (volume lookups, file index queries), the system should prefer routing to the Ra leader to avoid redirects:

```elixir
defmodule NeonFS.Core.ClusterRouter do
  @moduledoc """
  Routes requests to appropriate cluster nodes.
  """

  @doc """
  Execute a metadata operation, routing to the Ra leader if possible.
  """
  @spec metadata_call(module(), atom(), [term()]) :: term()
  def metadata_call(module, function, args) do
    case get_ra_leader() do
      {:ok, leader_node} ->
        :rpc.call(leader_node, module, function, args)

      :error ->
        # Fall back to any available core node
        {:ok, node} = NodeRegistry.select_node(:core)
        :rpc.call(node, module, function, args)
    end
  end

  @doc """
  Execute a data operation (read/write), selecting node by cost function.
  """
  @spec data_call(module(), atom(), [term()], keyword()) :: term()
  def data_call(module, function, args, opts \\ []) do
    {:ok, node} = NodeRegistry.select_node(:core, opts)
    :rpc.call(node, module, function, args)
  end
end
```

## Node Lifecycle

### Registration

When a Core node starts:
1. Wait for Ra cluster to be ready
2. Register with `NodeRegistry.register([:core])`
3. Start periodic heartbeat/metrics updates

When a FUSE node starts:
1. Connect to any known cluster node (bootstrap list or mDNS)
2. Register with `NodeRegistry.register([:fuse])`
3. Cache the current node list locally

### Deregistration

On graceful shutdown:
1. Call `NodeRegistry.deregister()`
2. Allow in-flight operations to complete
3. Stop accepting new operations

On crash:
1. Heartbeat timeout triggers automatic deregistration
2. Other nodes remove the crashed node from their local caches

### Health Checking

Options to evaluate:
1. **Heartbeat-based**: Nodes send periodic heartbeats to Ra
2. **Gossip-based**: Nodes exchange health info with neighbours
3. **Active probing**: Nodes periodically ping each other

Recommendation: Start with heartbeat-based (simplest), evaluate gossip if heartbeat creates too much Ra traffic.

## Migration Path

### Phase 1 (Current)
- Static `core_node` configuration
- Local-first lookup (check if VolumeRegistry is local)

### Phase 2 (This Spec)
- Implement `NodeRegistry` backed by Ra
- Implement basic cost function (load-based)
- Update `MountManager` to use `ClusterRouter`

### Phase 3 (Future)
- Add latency-aware routing
- Implement read replicas for data operations
- Add mDNS/DNS-SD for zero-config bootstrap

## Open Questions

1. **Heartbeat frequency**: How often should nodes update their metrics? (Proposed: every 5 seconds)
2. **Stale node timeout**: How long before a non-responsive node is considered dead? (Proposed: 30 seconds)
3. **Bootstrap mechanism**: How do new nodes discover the cluster initially?
4. **Cross-datacenter**: Should cost functions consider datacenter locality?

## References

- [Ra documentation](https://github.com/rabbitmq/ra)
- [Erlang `:pg` module](https://www.erlang.org/doc/man/pg.html) - potential alternative to custom registry
- [HashiCorp Serf](https://www.serf.io/) - gossip-based membership (for reference)
