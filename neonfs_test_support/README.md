# neonfs_test_support

Shared peer-cluster scaffolding for NeonFS integration tests. Any
package can boot a real multi-node NeonFS cluster inside its test
suite — actual BEAM peers via `:peer.start_link/1`, not mocks — by
pulling this in as a path dependency:

```elixir
# In your package's mix.exs
{:neonfs_test_support, path: "../neonfs_test_support", only: :test}
```

This is why per-interface end-to-end tests (FUSE, NFS, S3, WebDAV,
Docker, …) can live with their owning packages while
[`neonfs_integration`](../neonfs_integration/) keeps only the
cross-node cluster-correctness scenarios.

## Modules

| Module                                       | Purpose                                                                 |
|----------------------------------------------|-------------------------------------------------------------------------|
| `NeonFS.TestSupport.PeerCluster`             | Spawn `:peer.start_link/1` peer nodes, RPC against them, partition.     |
| `NeonFS.TestSupport.ClusterCase`             | ExUnit case template that boots a peer cluster per test.                |
| `NeonFS.TestSupport.PeerClusterTelemetry`    | Per-phase timing summary attached to peer-cluster lifecycle telemetry.  |
| `NeonFS.TestSupport.StreamingHelpers`        | Streaming-write helpers callable on peer nodes (e.g. `seeded_stream/2`).|
| `NeonFS.TestSupport.AppProfiler`             | Per-application start-time profiling on peer nodes.                     |
| `NeonFS.TestSupport.ProcessMemoryProfile`    | Per-process memory snapshot helper for streaming-write profiling.      |
| `NeonFS.TestSupport.SupervisorStartTimer`    | Collector for `NeonFS.Core.Supervisor` per-child start telemetry.      |
| `NeonFS.TestSupport.LoopbackDevice`          | Helpers for creating + tearing down loopback block devices.            |
| `NeonFS.TestSupport.EventCollector`          | GenServer that subscribes to volume events on a peer node.             |
| `NeonFS.TestSupport.TelemetryForwarder`      | Attaches a peer-side telemetry handler that forwards events back.      |

Start new multi-node tests with `NeonFS.TestSupport.ClusterCase` and
`alias NeonFS.TestSupport.PeerCluster` — the integration suites in the
interface packages are the reference usage.

## Licence

Apache-2.0 — see [LICENSE](../LICENSE) for details.
