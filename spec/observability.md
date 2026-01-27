# Observability

This document describes metrics collection, health checks, and operational visibility for NeonFS.

## Instrumentation Approach

NeonFS uses [Telemetry](https://hexdocs.pm/telemetry/) for instrumentation and exposes metrics via a Prometheus endpoint. This follows Elixir ecosystem conventions and integrates with standard infrastructure monitoring.

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  NeonFS Code    │────▶│  Telemetry       │────▶│  Prometheus     │
│                 │     │  Events          │     │  /metrics       │
│  :telemetry.    │     │                  │     │                 │
│  execute(...)   │     │  TelemetryMetrics│     │  Scraped by     │
│                 │     │  Prometheus      │     │  monitoring     │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## Telemetry Events

### Chunk Operations

```elixir
# Emitted by neonfs_blob (Rust NIF)
[:neonfs, :chunk, :read, :stop]
  measurements: %{duration: native_time, bytes: integer}
  metadata: %{chunk_hash: binary, tier: atom, node: atom, cache_hit: boolean}

[:neonfs, :chunk, :write, :stop]
  measurements: %{duration: native_time, bytes: integer}
  metadata: %{chunk_hash: binary, tier: atom, compression: atom}

[:neonfs, :chunk, :read, :exception]
  measurements: %{duration: native_time}
  metadata: %{chunk_hash: binary, kind: atom, reason: term}
```

### Replication

```elixir
[:neonfs, :replication, :send, :stop]
  measurements: %{duration: native_time, bytes: integer}
  metadata: %{chunk_hash: binary, target_node: atom, result: :ok | :error}

[:neonfs, :replication, :queue, :size]
  measurements: %{count: integer, bytes: integer}
  metadata: %{node: atom}

[:neonfs, :replication, :lag]
  measurements: %{bytes: integer, oldest_ms: integer}
  metadata: %{target_node: atom}
```

### Repair

```elixir
[:neonfs, :repair, :queue, :size]
  measurements: %{count: integer}
  metadata: %{priority: atom}  # :critical, :high, :normal, :low

[:neonfs, :repair, :complete, :stop]
  measurements: %{duration: native_time}
  metadata: %{chunk_hash: binary, repair_type: atom, success: boolean}
```

### Metadata Operations

```elixir
[:neonfs, :metadata, :query, :stop]
  measurements: %{duration: native_time}
  metadata: %{operation: atom, segment: atom}

[:neonfs, :ra, :command, :stop]
  measurements: %{duration: native_time}
  metadata: %{command: atom, result: :ok | :error}

[:neonfs, :intent, :acquired]
  measurements: %{}
  metadata: %{operation: atom, conflict_key: term}

[:neonfs, :intent, :completed]
  measurements: %{duration: native_time}
  metadata: %{operation: atom, outcome: atom}
```

### FUSE Operations

```elixir
[:neonfs, :fuse, :request, :stop]
  measurements: %{duration: native_time}
  metadata: %{operation: atom, volume: binary, result: :ok | :error}

# operation: :read, :write, :open, :readdir, :lookup, :getattr, etc.
```

### Storage Tiers

```elixir
[:neonfs, :tier, :migration, :stop]
  measurements: %{duration: native_time, bytes: integer}
  metadata: %{from_tier: atom, to_tier: atom, reason: atom}

[:neonfs, :gc, :run, :stop]
  measurements: %{duration: native_time, chunks_deleted: integer, bytes_freed: integer}
  metadata: %{node: atom}
```

## Prometheus Metrics

Metrics are exposed at `http://localhost:9568/metrics` (configurable port).

### Chunk Metrics

```prometheus
# Histogram: chunk read latency
neonfs_chunk_read_duration_seconds_bucket{tier="hot",le="0.001"} 1234
neonfs_chunk_read_duration_seconds_bucket{tier="hot",le="0.01"} 5678
neonfs_chunk_read_duration_seconds_sum{tier="hot"} 123.45
neonfs_chunk_read_duration_seconds_count{tier="hot"} 10000

# Histogram: chunk write latency
neonfs_chunk_write_duration_seconds_bucket{tier="hot",le="0.001"} 1234
neonfs_chunk_write_duration_seconds_sum{tier="hot"} 123.45
neonfs_chunk_write_duration_seconds_count{tier="hot"} 10000

# Counter: chunk bytes read/written
neonfs_chunk_read_bytes_total{tier="hot"} 1234567890
neonfs_chunk_write_bytes_total{tier="hot"} 1234567890

# Counter: chunk operation errors
neonfs_chunk_errors_total{operation="read",tier="hot",error="not_found"} 123

# Gauge: cache hit rate (computed over sliding window)
neonfs_chunk_cache_hit_ratio{cache="app_level"} 0.85
```

### Replication Metrics

```prometheus
# Gauge: replication queue depth
neonfs_replication_queue_size{target_node="neonfs@node2"} 1234
neonfs_replication_queue_bytes{target_node="neonfs@node2"} 12345678

# Gauge: replication lag
neonfs_replication_lag_bytes{target_node="neonfs@node2"} 12345678
neonfs_replication_lag_seconds{target_node="neonfs@node2"} 5.2

# Histogram: replication send latency
neonfs_replication_send_duration_seconds_bucket{target_node="neonfs@node2",le="0.1"} 1234

# Counter: replication outcomes
neonfs_replication_total{target_node="neonfs@node2",result="success"} 10000
neonfs_replication_total{target_node="neonfs@node2",result="error"} 12
```

### Repair Metrics

```prometheus
# Gauge: repair queue size by priority
neonfs_repair_queue_size{priority="critical"} 0
neonfs_repair_queue_size{priority="high"} 12
neonfs_repair_queue_size{priority="normal"} 456
neonfs_repair_queue_size{priority="low"} 1234

# Counter: repairs completed
neonfs_repairs_total{result="success"} 5678
neonfs_repairs_total{result="failed"} 12

# Histogram: repair duration
neonfs_repair_duration_seconds_bucket{le="1"} 1234
neonfs_repair_duration_seconds_bucket{le="10"} 5678

# Gauge: chunks below target replication
neonfs_chunks_underreplicated{severity="critical"} 0  # 1 copy remaining
neonfs_chunks_underreplicated{severity="degraded"} 45  # 2 copies remaining
```

### Storage Metrics

```prometheus
# Gauge: storage capacity by tier and node
neonfs_storage_capacity_bytes{node="neonfs@node1",tier="hot"} 500000000000
neonfs_storage_used_bytes{node="neonfs@node1",tier="hot"} 350000000000
neonfs_storage_reserved_bytes{node="neonfs@node1",tier="hot"} 25000000000

# Gauge: storage utilisation ratio
neonfs_storage_utilisation_ratio{node="neonfs@node1",tier="hot"} 0.70

# Gauge: chunk counts
neonfs_chunks_total{node="neonfs@node1",tier="hot"} 1234567

# Counter: GC activity
neonfs_gc_chunks_deleted_total{node="neonfs@node1"} 12345
neonfs_gc_bytes_freed_total{node="neonfs@node1"} 123456789

# Gauge: deduplication ratio (logical / physical)
neonfs_dedup_ratio{volume="documents"} 1.35
```

### Cluster Metrics

```prometheus
# Gauge: node state
neonfs_node_up{node="neonfs@node1"} 1
neonfs_node_up{node="neonfs@node2"} 1
neonfs_node_up{node="neonfs@node3"} 0

# Gauge: Ra cluster state
neonfs_ra_leader{cluster="neonfs_meta"} 1  # 1 if this node is leader
neonfs_ra_term{cluster="neonfs_meta"} 42
neonfs_ra_commit_index{cluster="neonfs_meta"} 123456

# Counter: Ra leader elections
neonfs_ra_elections_total{cluster="neonfs_meta"} 5

# Gauge: connected peers
neonfs_connected_peers 2
```

### FUSE Metrics

```prometheus
# Histogram: FUSE operation latency
neonfs_fuse_request_duration_seconds_bucket{operation="read",volume="documents",le="0.001"} 1234
neonfs_fuse_request_duration_seconds_bucket{operation="read",volume="documents",le="0.01"} 5678

# Counter: FUSE operations
neonfs_fuse_requests_total{operation="read",volume="documents",result="ok"} 100000
neonfs_fuse_requests_total{operation="read",volume="documents",result="error"} 12

# Gauge: active mounts
neonfs_fuse_mounts_active{volume="documents"} 1
```

### Intent Log Metrics

```prometheus
# Gauge: active intents by operation type
neonfs_intents_active{operation="write_file"} 5
neonfs_intents_active{operation="rename_directory"} 0
neonfs_intents_active{operation="migrate_chunk"} 12

# Counter: intent outcomes
neonfs_intents_total{operation="write_file",outcome="completed"} 10000
neonfs_intents_total{operation="write_file",outcome="failed"} 12
neonfs_intents_total{operation="write_file",outcome="expired"} 3

# Histogram: intent duration
neonfs_intent_duration_seconds_bucket{operation="write_file",le="1"} 9000
neonfs_intent_duration_seconds_bucket{operation="write_file",le="10"} 9900
```

## Service Level Indicators (SLIs)

Key metrics that indicate system health:

### Availability SLIs

| SLI | Good | Degraded | Critical |
|-----|------|----------|----------|
| Node availability | All nodes up | 1 node down | Quorum lost |
| FUSE error rate | < 0.1% | 0.1% - 1% | > 1% |
| Ra leader stable | No elections in 1h | 1-3 elections/h | > 3 elections/h |

### Durability SLIs

| SLI | Good | Degraded | Critical |
|-----|------|----------|----------|
| Chunks at target replication | 100% | > 99.9% | < 99.9% |
| Critical repair queue | 0 | 1-10 | > 10 |
| Replication lag | < 10s | 10s - 60s | > 60s |

### Latency SLIs

| SLI | Good (p99) | Degraded (p99) | Critical (p99) |
|-----|------------|----------------|----------------|
| Chunk read (hot) | < 10ms | 10ms - 100ms | > 100ms |
| Chunk read (cold) | < 100ms | 100ms - 1s | > 1s |
| Chunk write | < 50ms | 50ms - 500ms | > 500ms |
| Metadata query | < 5ms | 5ms - 50ms | > 50ms |

### Capacity SLIs

| SLI | Good | Warning | Critical |
|-----|------|---------|----------|
| Hot tier utilisation | < 80% | 80% - 90% | > 90% |
| Cold tier utilisation | < 85% | 85% - 95% | > 95% |
| Replication queue | < 1000 | 1000 - 10000 | > 10000 |

## Health Check Endpoint

The daemon exposes a health check at `http://localhost:9568/health`:

```json
{
  "status": "healthy",
  "checks": {
    "ra_cluster": {"status": "healthy", "leader": "neonfs@node2", "term": 42},
    "storage": {"status": "healthy", "tiers": {"hot": "ok", "warm": "ok", "cold": "ok"}},
    "replication": {"status": "healthy", "lag_seconds": 2.1},
    "repair_queue": {"status": "healthy", "critical": 0, "high": 5}
  },
  "node": "neonfs@node1",
  "version": "0.1.0",
  "uptime_seconds": 86400
}
```

**Status values:**
- `healthy`: All checks pass
- `degraded`: Some non-critical checks failing
- `unhealthy`: Critical checks failing

**HTTP response codes:**
- `200 OK`: healthy or degraded
- `503 Service Unavailable`: unhealthy

This endpoint is suitable for load balancer health checks and Kubernetes liveness/readiness probes.

## Alerting Rules

Example Prometheus alerting rules:

```yaml
groups:
  - name: neonfs
    rules:
      - alert: NeonFSNodeDown
        expr: neonfs_node_up == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "NeonFS node {{ $labels.node }} is down"

      - alert: NeonFSReplicationLagHigh
        expr: neonfs_replication_lag_seconds > 60
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Replication lag to {{ $labels.target_node }} is {{ $value }}s"

      - alert: NeonFSCriticalRepairsQueued
        expr: neonfs_repair_queue_size{priority="critical"} > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "{{ $value }} critical repairs queued"

      - alert: NeonFSStorageNearlyFull
        expr: neonfs_storage_utilisation_ratio > 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "{{ $labels.tier }} tier on {{ $labels.node }} is {{ $value | humanizePercentage }} full"

      - alert: NeonFSHighErrorRate
        expr: rate(neonfs_fuse_requests_total{result="error"}[5m]) / rate(neonfs_fuse_requests_total[5m]) > 0.01
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "FUSE error rate is {{ $value | humanizePercentage }}"
```

## Configuration

```yaml
observability:
  # Prometheus metrics endpoint
  prometheus:
    enabled: true
    port: 9568
    path: /metrics

  # Health check endpoint
  health:
    enabled: true
    port: 9568
    path: /health

  # Telemetry polling interval for gauges
  poll_interval: 10s

  # Histogram buckets (seconds)
  histogram_buckets:
    chunk_read: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5]
    chunk_write: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5]
    fuse_request: [0.0001, 0.001, 0.01, 0.1, 1, 10]
    replication: [0.01, 0.1, 0.5, 1, 5, 10, 30]
```

## Implementation

```elixir
defmodule NeonFS.Telemetry do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    children = [
      # Prometheus exporter
      {TelemetryMetricsPrometheus, metrics: metrics()},

      # Periodic gauge polling
      {NeonFS.Telemetry.Poller, interval: :timer.seconds(10)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp metrics do
    [
      # Chunk operations
      summary("neonfs.chunk.read.stop.duration",
        unit: {:native, :second},
        tags: [:tier]
      ),
      counter("neonfs.chunk.read.stop.bytes",
        tags: [:tier]
      ),
      # ... additional metrics
    ]
  end
end

defmodule NeonFS.Telemetry.Poller do
  use GenServer

  def init(opts) do
    interval = Keyword.fetch!(opts, :interval)
    schedule_poll(interval)
    {:ok, %{interval: interval}}
  end

  def handle_info(:poll, state) do
    emit_gauge_metrics()
    schedule_poll(state.interval)
    {:noreply, state}
  end

  defp emit_gauge_metrics do
    # Replication queue sizes
    for {node, size} <- ReplicationQueue.sizes() do
      :telemetry.execute(
        [:neonfs, :replication, :queue, :size],
        %{count: size.count, bytes: size.bytes},
        %{target_node: node}
      )
    end

    # Repair queue sizes
    for {priority, count} <- RepairQueue.sizes() do
      :telemetry.execute(
        [:neonfs, :repair, :queue, :size],
        %{count: count},
        %{priority: priority}
      )
    end

    # Storage utilisation
    for drive <- Storage.drives() do
      :telemetry.execute(
        [:neonfs, :storage, :utilisation],
        %{
          capacity: drive.capacity,
          used: drive.used,
          reserved: drive.reserved
        },
        %{node: drive.node, tier: drive.tier}
      )
    end
  end
end
```
