# NeonFS Client

The shared library every NeonFS interface node is built on. If you want
to add a new way of accessing a NeonFS cluster — a protocol server, a
plugin, a tool — this package is your entire API surface: it finds core
nodes, routes RPCs with failover, and streams chunk data over the TLS
data plane. Interface packages never depend on `neonfs_core`.

This is a pure library with no OTP application module — consumers start
its processes in their own supervision trees.

## What it provides

### Service discovery and routing

- `NeonFS.Client.Connection` — bootstrap node connectivity via `Node.connect/1`
- `NeonFS.Client.Discovery` — queries the core service registry, caches in local ETS
- `NeonFS.Client.CostFunction` — latency- and load-based core node selection
- `NeonFS.Client.Router` — RPC routing with automatic failover across core nodes
- `NeonFS.Client.Registrar` — registers the local node as a service in the cluster

### Data plane

- `NeonFS.Client.ChunkReader` — distribution-safe lazy `Stream` of file
  chunks, fetched over the mTLS data plane. The canonical way to read
  file contents from an interface node — bounded memory regardless of
  file size.
- `NeonFS.Client.ChunkWriter` — streaming writes, staged at chunk
  granularity.

### Cluster utilities

- `NeonFS.Client.KV` — generic Ra-backed key-value access
- `NeonFS.Client.EventHandler` — cluster event subscription (cache invalidation)
- `NeonFS.Client.HealthCheck` — health endpoint plumbing for interface nodes
- Shared types: `NeonFS.Core.Volume`, `NeonFS.Core.FileMeta`, `ServiceInfo`

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

And stream file contents without buffering:

```elixir
NeonFS.Client.ChunkReader.read_file_stream(volume, path, [])
|> Stream.each(&send_downstream/1)
|> Stream.run()
```

The existing interface packages (`neonfs_fuse`, `neonfs_nfs`,
`neonfs_s3`, `neonfs_webdav`, …) are the reference consumers — copy
their supervision-tree wiring when building a new interface.

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
