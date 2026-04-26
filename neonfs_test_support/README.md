# neonfs_test_support

Shared peer-cluster scaffolding for NeonFS cross-package integration
tests. Path-only dependency — every package that needs a multi-node
test harness pulls it in with `only: :test`.

```elixir
# In your interface package's mix.exs
{:neonfs_test_support, path: "../neonfs_test_support", only: :test}
```

## Modules

| Module                                       | Purpose                                                                 |
|----------------------------------------------|-------------------------------------------------------------------------|
| `NeonFS.TestSupport.PeerCluster`             | Spawn `:peer.start_link/1` peer nodes, RPC against them, partition.     |
| `NeonFS.TestSupport.PeerClusterTelemetry`    | Per-phase timing summary attached to peer-cluster lifecycle telemetry.  |
| `NeonFS.TestSupport.StreamingHelpers`        | Streaming-write helpers callable on peer nodes (e.g. `seeded_stream/2`).|
| `NeonFS.TestSupport.AppProfiler`             | Per-application start-time profiling on peer nodes.                     |
| `NeonFS.TestSupport.ProcessMemoryProfile`    | Per-process memory snapshot helper for streaming-write profiling.       |
| `NeonFS.TestSupport.SupervisorStartTimer`    | Collector for `NeonFS.Core.Supervisor` per-child start telemetry.       |
| `NeonFS.TestSupport.ClusterCase`             | ExUnit case template that boots a peer cluster per test.                |
| `NeonFS.TestSupport.LoopbackDevice`          | Helpers for creating + tearing down loopback block devices.             |
| `NeonFS.TestSupport.EventCollector`          | GenServer that subscribes to volume events on a peer node.              |
| `NeonFS.TestSupport.TelemetryForwarder`      | Attaches a peer-side telemetry handler that forwards events back.       |

## Compatibility

`neonfs_integration` keeps the old `NeonFS.Integration.*` module
names working via thin re-export shims so existing tests continue to
compile during the per-interface migration tracked under #582. The
shims are deleted by #604 once all interfaces have moved.
