# NeonFS Client

Shared types and service discovery client for NeonFS cluster members.

This is a pure library package with no OTP application module. Consumers start
its GenServers in their own supervision trees.

## Components

### Shared Types

- `NeonFS.Core.Volume` — volume configuration and metadata
- `NeonFS.Core.FileMeta` — file metadata (chunk lists, sizes, timestamps)

### Service Discovery

- `NeonFS.Client.Connection` — bootstrap node connectivity via `Node.connect/1`
- `NeonFS.Client.Discovery` — queries `NeonFS.Core.ServiceRegistry` on core
  nodes, caches results in local ETS
- `NeonFS.Client.CostFunction` — latency and load-based node selection
- `NeonFS.Client.Router` — RPC routing with automatic failover across core nodes

## Usage

Add to your `mix.exs`:

```elixir
{:neonfs_client, path: "../neonfs_client"}
```

Start the client infrastructure in your supervision tree:

```elixir
children = [
  {NeonFS.Client.Connection, bootstrap_nodes: [:"neonfs_core@host"]},
  NeonFS.Client.Discovery,
  NeonFS.Client.CostFunction
]
```

Then route calls to core nodes:

```elixir
NeonFS.Client.Router.core_call(mod, fun, args)
```

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
