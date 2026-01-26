# Node Management

This document describes node lifecycle, failure handling, partition behaviour, and the cost functions used for read/write routing.

## Node States

```
:online          # Normal operation
:draining        # Accepting reads, rejecting writes, migrating roles
:unreachable     # Cannot contact, unknown cause
:suspect         # Unreachable for extended period
:maintenance     # Operator-initiated downtime, expected return
:decommissioning # Migrating all data off, then remove
:dead            # Confirmed gone, repair aggressively
```

## Network Partition Behaviour

NeonFS relies on Ra (Raft) for cluster-wide consensus and quorum operations for metadata. Both provide well-defined partition behaviour.

**During a partition:**

| Component | Majority Partition | Minority Partition |
|-----------|-------------------|-------------------|
| Ra consensus | Continues operating | Cannot commit; reads may work |
| Metadata quorum ops | Work if quorum reachable | Fail (can't reach quorum) |
| Local chunk reads | Work (local data available) | Work (local data available) |
| Writes | Work if quorum reachable | Fail |

**Practical implications:**

- Minority partition becomes effectively read-only for data on local node
- No split-brain writes: quorum requirement prevents conflicting writes
- Operations in flight when partition occurs may timeout and fail
- HLC timestamps ensure write ordering is preserved across partition heal

**After partition heals:**

- Ra automatically reconciles (Raft guarantees)
- Metadata quorum reads will perform read-repair if stale
- Anti-entropy process catches any missed repairs
- No manual intervention required for data consistency

**Limitations (accepted):**

This design assumes:
- Partitions are rare (trusted network, typically LAN or WireGuard mesh)
- Partitions are transient (minutes to hours, not permanent)
- Minority side being read-only is acceptable during partition

For deployments requiring availability during partitions (e.g., geo-distributed with unreliable links), a more sophisticated approach would be needed—this is out of scope for the target environment.

**Monitoring:**

Partition events should trigger alerts:
- Node unreachable from multiple peers simultaneously
- Ra leader election (may indicate partition or leader failure)
- Quorum operation failure rate spike

## Clock Synchronisation

NeonFS uses timestamps for conflict resolution (HLC), lease management, and operational metrics. Correct operation requires reasonably synchronised clocks across nodes.

**Requirements:**

- All nodes must run NTP (or equivalent: chrony, systemd-timesyncd)
- Maximum acceptable clock skew: 1 second
- HLC tolerates small skew but cannot correct large drift

**Why clocks matter:**

| Feature | Clock Dependency | Effect of Skew |
|---------|------------------|----------------|
| HLC timestamps | Wall-clock component | Large skew → HLC degrades to logical clock only |
| Lease expiry | Absolute time comparison | Skew → early/late expiry, potential conflicts |
| Invite token TTL | Expiry checking | Skew → tokens expire early/late |
| Access statistics | Recency calculations | Skew → incorrect tiering decisions |
| Audit logs | Event ordering | Skew → misleading timelines |

**Clock skew detection:**

Nodes compare clocks during regular heartbeats and gossip:

```elixir
defmodule NeonFS.ClockMonitor do
  @max_skew_ms 1_000

  def check_peer_clock(peer_node) do
    t1 = System.system_time(:millisecond)
    {:ok, peer_time} = :rpc.call(peer_node, System, :system_time, [:millisecond])
    t2 = System.system_time(:millisecond)

    # Account for round-trip time
    local_estimate = div(t1 + t2, 2)
    skew = abs(peer_time - local_estimate)

    if skew > @max_skew_ms do
      Logger.warning("Clock skew detected",
        peer: peer_node,
        skew_ms: skew,
        threshold_ms: @max_skew_ms
      )
      :telemetry.execute([:neonfs, :clock, :skew_warning], %{skew_ms: skew}, %{peer: peer_node})
    end

    {:ok, skew}
  end
end
```

**Behaviour under skew:**

- **Skew < 1s**: Normal operation, HLC handles it
- **Skew 1-10s**: Warning logged, operations continue but lease timing may be affected
- **Skew > 10s**: Error logged, node should be investigated; may cause operational issues

**Operational guidance:**

```yaml
# /etc/chrony/chrony.conf or equivalent
server time.cloudflare.com iburst
server time.google.com iburst
makestep 0.1 3
```

Ensure NTP is running and synchronised before joining a cluster. The `neonfs cluster join` command should verify local clock is within tolerance.

## State Transitions

```
                    ┌─────────────┐
        ┌──────────▶│   online    │◀──────────┐
        │           └─────────────┘           │
        │                 │                   │
        │                 │ (network issue)   │
        │                 ▼                   │
        │           ┌─────────────┐           │
        │           │ unreachable │           │
        │           └─────────────┘           │
        │                 │                   │
   (reconnect)            │ (timeout)    (operator)
        │                 ▼                   │
        │           ┌─────────────┐           │
        │           │   suspect   │───────────┤
        │           └─────────────┘           │
        │                 │                   │
        │       (operator │ (extended         │
        │        input)   │  timeout)         │
        │                 ▼                   │
        │           ┌─────────────┐     ┌─────────────┐
        │           │    dead     │     │ maintenance │
        │           └─────────────┘     └─────────────┘
        │                                     │
        └─────────────────────────────────────┘
```

## Escalation Ladder

| Duration Unreachable | Action |
|---------------------|--------|
| < 5 minutes | Wait, likely transient |
| 5-30 minutes | Prepare repair plan, don't execute |
| 30 min - 2 hours | Escalate to operator for decision |
| > 2 hours (no response) | Begin repair if capacity available |

## Repair Prioritisation

When repair is triggered, chunks are processed in priority order:

1. **Risk tier**: At-risk (0 healthy copies) > Degraded (below minimum) > Below target > Satisfied
2. **Volume priority**: Per-policy priority (critical > high > normal > low)
3. **Access recency**: Hot data before cold
4. **Repair cost**: Cheaper repairs first within tier (tiebreaker)

```elixir
def repair_priority(chunk) do
  {
    risk_tier(chunk),           # 0 = at-risk, 3 = satisfied
    volume_priority(chunk),     # Per policy
    -access_recency(chunk),     # More recent = higher priority
    repair_cost(chunk)          # Cheaper first
  }
end
```

## Node Cost Function

Each node has a cost function used for placement decisions, read routing, and multi-site replication.

```elixir
%NodeCost{
  node: :node1,

  # Network characteristics (measured or configured)
  latency_ms: %{
    node2: 1,        # Same rack
    node3: 2,        # Same datacenter
    node4: 50,       # Remote site
    node5: 120       # Different continent
  },
  bandwidth_mbps: %{
    node2: 10_000,
    node3: 10_000,
    node4: 1_000,
    node5: 100
  },

  # Current load
  io_pressure: 0.3,           # 0-1, current I/O utilisation
  storage_pressure: 0.7,      # 0-1, how full
  cpu_pressure: 0.2,

  # Administrative preferences
  site: "us-west",
  rack: "rack-1",
  maintenance_window: ~T[02:00:00]..~T[06:00:00]
}
```

### Placement Cost Calculation

```elixir
def placement_cost(source_node, target_node, chunk_size) do
  costs = get_node_costs(source_node)
  target = get_node_costs(target_node)

  # Transfer cost (latency + bandwidth)
  transfer_time = costs.latency_ms[target_node] +
                  (chunk_size / costs.bandwidth_mbps[target_node] * 8)

  # Load cost
  load_penalty = target.io_pressure * 100 + target.storage_pressure * 50

  # Site diversity bonus (negative cost = good)
  site_bonus = if target.site != costs.site, do: -200, else: 0
  rack_bonus = if target.rack != costs.rack, do: -50, else: 0

  transfer_time + load_penalty + site_bonus + rack_bonus
end
```

### Read Routing

```elixir
def select_read_source(chunk_hash, requesting_node) do
  locations = get_chunk_locations(chunk_hash)

  locations
  |> Enum.map(fn loc ->
    {loc, read_cost(requesting_node, loc.node, loc.tier)}
  end)
  |> Enum.min_by(fn {_loc, cost} -> cost end)
  |> elem(0)
end

def read_cost(from, to, tier) do
  base = if from == to, do: 0, else: get_latency(from, to)
  tier_cost = tier_latency(tier)  # SSD fast, HDD slow/maybe spinning
  base + tier_cost
end
```

### Write Placement with Site Awareness

For multi-site deployments, ensure replicas span sites:

```elixir
def select_write_targets(chunk_hash, volume) do
  candidates = available_nodes()
  required = volume.durability.factor

  # Group by site
  by_site = Enum.group_by(candidates, & &1.site)

  # Select from different sites where possible
  selected = by_site
    |> Enum.flat_map(fn {_site, nodes} ->
      # Prefer lowest-cost node per site
      nodes |> Enum.sort_by(&placement_cost(Node.self(), &1, chunk_size)) |> Enum.take(1)
    end)
    |> Enum.take(required)

  # If not enough sites, fill from best remaining
  if length(selected) < required do
    remaining = candidates -- selected
    additional = remaining
      |> Enum.sort_by(&placement_cost(Node.self(), &1, chunk_size))
      |> Enum.take(required - length(selected))
    selected ++ additional
  else
    selected
  end
end
```

### Async Replication to Remote Sites

For writes, prioritise local/fast nodes, replicate to remote sites asynchronously:

```elixir
def write_with_async_remote(chunk, data, volume) do
  # Immediate: write to local + nearby nodes (sync)
  local_targets = select_local_targets(chunk, volume)
  :ok = write_sync(local_targets, chunk, data, volume.write_ack)

  # Deferred: replicate to remote sites (async)
  remote_targets = select_remote_targets(chunk, volume)
  spawn_replication(remote_targets, chunk, data)

  :ok
end
```
