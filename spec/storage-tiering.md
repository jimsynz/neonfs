# Storage Tiering

This document describes the storage tier system, drive management, power management, and caching strategies.

## Tier Definitions

| Tier | Typical Media | Use Case |
|------|--------------|----------|
| Hot | NVMe SSD | Active working set |
| Warm | SATA SSD | Recent/moderate access |
| Cold | HDD | Archive, infrequent access |

Linux's page cache provides automatic caching of recently accessed file data, which covers most caching needs for raw (unencrypted, uncompressed) chunks. Application-level caching is only used for transformed data — see the Caching Strategy section below.

## Storage Topology

NeonFS supports heterogeneous storage configurations. Not all tiers need to be present, and different nodes can have different storage configurations.

### Tier Flexibility

Tiers are optional. A cluster can have any subset:

| Configuration | Valid? | Notes |
|---------------|--------|-------|
| Hot only | ✓ | No tiering, just replication |
| Hot + cold | ✓ | Skip warm in promotion/demotion |
| Warm + cold | ✓ | No hot tier available |
| Cold only | ✓ | Archive-only cluster |
| Mixed across nodes | ✓ | Each node contributes what it has |

**Tier availability is discovered at runtime:**

```elixir
defmodule NeonFS.Storage.Topology do
  @doc """
  Returns available tiers across the cluster, ordered hot → warm → cold.
  Only includes tiers that have at least one healthy drive.
  """
  def available_tiers do
    all_drives()
    |> Enum.filter(&healthy?/1)
    |> Enum.map(& &1.tier)
    |> Enum.uniq()
    |> Enum.sort_by(&tier_order/1)
  end

  defp tier_order(:hot), do: 0
  defp tier_order(:warm), do: 1
  defp tier_order(:cold), do: 2

  @doc """
  Returns the next colder tier that actually exists in the cluster.
  Returns nil if already at coldest available tier.
  """
  def next_colder_tier(current_tier) do
    available = available_tiers()
    current_index = Enum.find_index(available, &(&1 == current_tier))

    case current_index do
      nil -> List.first(available)  # Tier doesn't exist, return coldest available
      i -> Enum.at(available, i + 1)  # Next tier, or nil if at end
    end
  end

  @doc """
  Returns the next warmer tier that actually exists in the cluster.
  Returns nil if already at hottest available tier.
  """
  def next_warmer_tier(current_tier) do
    available = available_tiers()
    current_index = Enum.find_index(available, &(&1 == current_tier))

    case current_index do
      nil -> List.last(available)  # Tier doesn't exist, return hottest available
      0 -> nil  # Already at hottest
      i -> Enum.at(available, i - 1)
    end
  end
end
```

**Volume configuration validation:**

When creating a volume, the `initial_tier` is validated against available tiers:

```elixir
def validate_volume_config(config) do
  available = Topology.available_tiers()

  cond do
    config.initial_tier not in available ->
      {:error, {:tier_unavailable, config.initial_tier, available}}

    config.durability.type == :erasure and length(available_drives()) < stripe_width(config) ->
      {:error, :insufficient_drives_for_erasure_coding}

    true ->
      :ok
  end
end
```

### Failure Domains

Redundancy is about placing replicas in different **failure domains**. NeonFS recognises three levels:

| Level | Protects Against | Example |
|-------|------------------|---------|
| Drive | Single drive failure | 2 SSDs on same node |
| Node | Node failure (power, OS, hardware) | 2 different machines |
| Zone | Rack/site failure | Different physical locations |

**Failure domain hierarchy:**

```
Zone (optional)
└── Node
    └── Drive
```

Replicas should be placed to maximize failure domain separation. Losing a higher-level domain loses all lower-level domains within it (node failure loses all drives on that node).

### Chunk Location Tracking

Chunk locations include the drive, not just node and tier:

```elixir
%ChunkLocation{
  node: :neonfs@node1,
  drive_id: "nvme0",           # Specific drive
  tier: :hot,
  stored_at: ~U[...],
  last_verified: ~U[...]
}
```

This enables:
- Drive-level replica placement (redundancy on single node)
- Accurate failure impact assessment (which chunks affected by drive failure)
- Drive-aware read path (avoid failed/slow drives)

### Replica Placement Policy

When placing replicas, maximize failure domain separation:

```elixir
defmodule NeonFS.Placement do
  @doc """
  Select drives for placing `count` replicas of a chunk.
  Maximizes failure domain separation while respecting tier preference.
  """
  def select_replica_targets(count, preferred_tier, opts \\ []) do
    exclude_drives = Keyword.get(opts, :exclude, [])

    candidates = available_drives()
      |> Enum.reject(&(&1.id in exclude_drives))
      |> Enum.filter(&has_capacity?/1)
      |> score_and_sort(preferred_tier)

    select_with_domain_separation(candidates, count)
  end

  defp score_and_sort(drives, preferred_tier) do
    Enum.sort_by(drives, fn drive ->
      {
        # Primary: prefer requested tier (0 = match, 1 = different)
        if(drive.tier == preferred_tier, do: 0, else: 1),

        # Secondary: prefer drives with more free space
        -available_space(drive),

        # Tertiary: prefer lower current I/O load
        current_load(drive)
      }
    end)
  end

  defp select_with_domain_separation(candidates, count) do
    select_with_domain_separation(candidates, count, [], MapSet.new(), MapSet.new())
  end

  defp select_with_domain_separation(_candidates, 0, selected, _used_nodes, _used_drives) do
    Enum.reverse(selected)
  end

  defp select_with_domain_separation([], _remaining, selected, _used_nodes, _used_drives) do
    # Not enough candidates - return what we have
    Enum.reverse(selected)
  end

  defp select_with_domain_separation([candidate | rest], remaining, selected, used_nodes, used_drives) do
    # Score this candidate based on failure domain separation
    node_penalty = if candidate.node in used_nodes, do: 1000, else: 0
    drive_penalty = if candidate.id in used_drives, do: 10000, else: 0  # Should never happen

    # Find best candidate considering domain separation
    all_candidates = [candidate | rest]

    best = Enum.min_by(all_candidates, fn c ->
      node_p = if c.node in used_nodes, do: 1000, else: 0
      {node_p, -available_space(c)}
    end)

    new_selected = [best | selected]
    new_used_nodes = MapSet.put(used_nodes, best.node)
    new_used_drives = MapSet.put(used_drives, best.id)
    remaining_candidates = List.delete(all_candidates, best)

    select_with_domain_separation(
      remaining_candidates,
      remaining - 1,
      new_selected,
      new_used_nodes,
      new_used_drives
    )
  end
end
```

**Placement priority:**

1. **Different nodes** (best redundancy)
2. **Different drives on same node** (if not enough nodes)
3. **Same drive** (only if `replication_factor: 1` or no other option)

**Examples:**

| Cluster | Replication Factor | Placement |
|---------|-------------------|-----------|
| 3 nodes, 1 SSD each | 3 | One replica per node |
| 1 node, 3 SSDs | 3 | One replica per drive |
| 2 nodes, 2 SSDs each | 3 | Node A drive 1, Node B drive 1, Node A drive 2 (or Node B drive 2) |
| 1 node, 1 SSD | 3 | Only 1 replica possible (warning logged) |

**Insufficient failure domains:**

If the requested replication factor exceeds available failure domains:

```elixir
def check_placement_feasibility(replication_factor, preferred_tier) do
  drives = available_drives_for_tier(preferred_tier)
  nodes = drives |> Enum.map(& &1.node) |> Enum.uniq()

  cond do
    length(drives) < replication_factor ->
      {:warning, :insufficient_drives,
        "Requested #{replication_factor} replicas but only #{length(drives)} drives available"}

    length(nodes) < replication_factor ->
      {:warning, :insufficient_nodes,
        "Requested #{replication_factor} replicas but only #{length(nodes)} nodes available. " <>
        "Data is protected against drive failure but not node failure."}

    true ->
      :ok
  end
end
```

Writes proceed with a warning rather than failing - some redundancy is better than none.

### Cross-Tier Replication

Replicas can span tiers when necessary:

```elixir
# Preferred: all replicas on same tier
Chunk on hot tier, replication_factor: 2
  → Node A /mnt/nvme0 (hot)
  → Node B /mnt/nvme0 (hot)

# Fallback: mixed tiers if preferred tier lacks capacity
Chunk on hot tier, replication_factor: 2, but only 1 hot drive has space
  → Node A /mnt/nvme0 (hot)
  → Node B /mnt/ssd0 (warm)  # Fallback to warm
```

The read path prefers hotter replicas, so performance remains optimal when a hot copy exists.

### Zone Awareness (Optional)

For multi-site deployments, nodes can be assigned to zones:

```yaml
# /etc/neonfs/daemon.conf
cluster:
  node_address: neonfs@node1.site-a.example.com
  zone: site-a  # Optional zone identifier
```

When zones are configured, placement prioritises zone separation:

1. **Different zones** (survives site failure)
2. **Different nodes in same zone** (survives node failure)
3. **Different drives on same node** (survives drive failure)

```elixir
defp domain_separation_score(candidate, used_zones, used_nodes, used_drives) do
  zone_penalty = if candidate.zone in used_zones, do: 10000, else: 0
  node_penalty = if candidate.node in used_nodes, do: 1000, else: 0
  drive_penalty = if candidate.id in used_drives, do: 100, else: 0

  zone_penalty + node_penalty + drive_penalty
end
```

## Drive State Tracking

Each node can have multiple drives, including multiple drives at the same tier.

```elixir
%Drive{
  id: drive_id,
  node: :node1,
  path: "/mnt/nvme0",     # Mount point
  tier: :hot,

  state: :active | :spinning_up | :standby,
  last_active: ~U[...],

  capacity: 500_000_000_000,  # 500 GB
  used: 350_000_000_000,      # 350 GB
  reserved: 25_000_000_000,   # 25 GB reserved for incoming replication

  # Performance characteristics (measured or configured)
  read_latency_p50_us: 100,
  write_latency_p50_us: 150,
  iops_limit: 100_000
}
```

**Example: Node with multiple hot drives**

```yaml
storage:
  drives:
    - path: /mnt/nvme0
      tier: hot
    - path: /mnt/nvme1
      tier: hot
    - path: /mnt/ssd0
      tier: warm
    - path: /mnt/hdd0
      tier: cold
    - path: /mnt/hdd1
      tier: cold
```

When writing to a tier with multiple drives, distribute based on:
- Available space
- Current I/O load
- Drive health

## Read Path Optimisation

When locating a chunk for read, prefer sources that minimise latency and avoid spinning up idle drives:

```elixir
def locate_chunk(hash) do
  replicas = Metadata.get_replicas(hash)

  replicas
  |> Enum.sort_by(fn location ->
    {
      media_score(location.tier, location.state),  # SSD always beats HDD
      locality_score(location)                     # Local beats remote (tiebreaker)
    }
  end)
  |> List.first()
end

defp media_score(:hot, _), do: 0          # NVMe SSD - always fast
defp media_score(:warm, _), do: 1         # SATA SSD - always fast
defp media_score(:cold, :active), do: 2   # HDD already spinning
defp media_score(:cold, :spinning_up), do: 3
defp media_score(:cold, :standby), do: 10 # Avoid spin-up if possible
```

Priority order:
1. Local SSD (hot tier)
2. Local SSD (warm tier)
3. Remote SSD
4. Local HDD (already spinning)
5. Remote HDD (already spinning)
6. Local HDD (requires spin up)
7. Remote HDD (requires spin up)

## Drive Power Management

Power management is configured per-drive based on drive characteristics and user preference.

```yaml
storage:
  drives:
    - path: /mnt/nvme0
      tier: hot
      power_management: always_on    # SSDs - no spin-down concept

    - path: /mnt/hdd0
      tier: cold
      power_management: spin_down    # Desktop drive - spin down when idle
      idle_timeout: 30m              # Spin down after 30 minutes idle

    - path: /mnt/hdd1
      tier: cold
      power_management: always_on    # NAS/NVR drive - designed for 24/7 operation
```

**Power management modes:**

| Mode | Behaviour | Use For |
|------|-----------|---------
| `always_on` | Never spin down | SSDs, NAS drives (WD Red, Seagate IronWolf), NVR drives |
| `spin_down` | Spin down after idle timeout | Desktop drives, drives where noise/power matters |

**Spin-down considerations:**

Drives configured for `spin_down` will enter standby after `idle_timeout` with no pending I/O. Before spinning down, check:
- Pending requests in queue
- Scheduled scrub or repair operations

**Drive selection preference:**

When multiple replicas exist, prefer reading from drives that are already active to avoid unnecessary spin-ups:

```elixir
defp drive_preference(drive) do
  case {drive.power_management, drive.state} do
    {:always_on, _} -> 0           # Always prefer always-on drives
    {:spin_down, :active} -> 1     # Already spinning, use it
    {:spin_down, :standby} -> 10   # Avoid spin-up if possible
  end
end
```

If the only replica is on a spun-down drive, it will be spun up—availability always wins over power savings.

## Drive Spin-Up Coordination

When all replicas of a chunk reside on spun-down drives, NeonFS races all available replicas in parallel and serves whichever responds first. This avoids arbitrary waits and naturally handles variable spin-up times across different drives.

### Drive State Machine

Each drive maintains state with locking to coordinate concurrent access:

```elixir
defmodule NeonFS.Storage.DriveState do
  use GenServer

  # States: :active, :idle, :spinning_up, :standby, :spinning_down, :failed
  #
  # Transitions:
  #   :standby -> :spinning_up (on read request)
  #   :spinning_up -> :active (spin-up complete)
  #   :active -> :idle (no I/O for idle_threshold)
  #   :idle -> :spinning_down (idle_timeout elapsed)
  #   :spinning_down -> :standby (spin-down complete)
  #   :idle -> :active (new I/O request)
  #   any -> :failed (drive error)

  def request_read(drive_id) do
    GenServer.call(via(drive_id), :request_read)
  end

  def handle_call(:request_read, _from, %{state: :active} = state) do
    {:reply, {:ok, :ready}, touch_active(state)}
  end

  def handle_call(:request_read, _from, %{state: :idle} = state) do
    {:reply, {:ok, :ready}, %{state | state: :active, last_active: now()}}
  end

  def handle_call(:request_read, from, %{state: :standby} = state) do
    # Start spin-up, caller will wait
    spawn_link(fn -> spin_up_drive(state.drive_id, from) end)
    {:noreply, %{state | state: :spinning_up, waiting: [from | state.waiting]}}
  end

  def handle_call(:request_read, from, %{state: :spinning_up} = state) do
    # Already spinning up, add to waiters
    {:noreply, %{state | waiting: [from | state.waiting]}}
  end

  def handle_info({:spin_up_complete, :ok}, state) do
    # Notify all waiters
    Enum.each(state.waiting, fn from ->
      GenServer.reply(from, {:ok, :ready})
    end)
    {:noreply, %{state | state: :active, waiting: [], last_active: now()}}
  end

  def handle_info({:spin_up_complete, {:error, reason}}, state) do
    Enum.each(state.waiting, fn from ->
      GenServer.reply(from, {:error, reason})
    end)
    {:noreply, %{state | state: :failed, waiting: [], error: reason}}
  end
end
```

### Read Path with Parallel Racing

When reading a chunk, the read path races all spun-down replicas in parallel:

```elixir
defmodule NeonFS.Storage.ChunkReader do
  @spinup_timeout :timer.minutes(2)

  def read_chunk(chunk_hash) do
    locations = ChunkIndex.get_locations(chunk_hash)

    # Partition by drive readiness
    {ready, need_spinup} = Enum.split_with(locations, fn loc ->
      drive_ready?(loc)
    end)

    case ready do
      [best | rest] ->
        # At least one drive is ready - use the best one
        read_from_ready(chunk_hash, best, rest)

      [] ->
        # All drives need spin-up - race them all
        race_spinup_reads(chunk_hash, need_spinup)
    end
  end

  defp drive_ready?(location) do
    case DriveState.get_state(location.drive_id) do
      state when state in [:active, :idle] -> true
      _ -> false
    end
  end

  defp read_from_ready(chunk_hash, primary, fallbacks) do
    case do_read(chunk_hash, primary) do
      {:ok, data} -> {:ok, data}
      {:error, _} -> try_fallbacks(chunk_hash, fallbacks)
    end
  end

  defp race_spinup_reads(_chunk_hash, []) do
    {:error, :no_replicas}
  end

  defp race_spinup_reads(chunk_hash, locations) do
    # Start reads on ALL locations in parallel
    tasks = Enum.map(locations, fn loc ->
      Task.async(fn ->
        case DriveState.request_read(loc.drive_id) do
          {:ok, :ready} -> {loc, do_read(chunk_hash, loc)}
          {:error, reason} -> {loc, {:error, reason}}
        end
      end)
    end)

    # Take the first successful result
    result = await_first_success(tasks)

    # Cancel remaining tasks
    Enum.each(tasks, &Task.shutdown(&1, :brutal_kill))

    result
  end

  defp await_first_success(tasks) do
    receive_results(tasks, MapSet.new(tasks), @spinup_timeout)
  end

  defp receive_results(_tasks, remaining, _timeout) when map_size(remaining) == 0 do
    {:error, :all_replicas_failed}
  end

  defp receive_results(tasks, remaining, timeout) do
    start = System.monotonic_time(:millisecond)

    receive do
      {ref, {_loc, {:ok, data}}} ->
        Process.demonitor(ref, [:flush])
        {:ok, data}

      {ref, {_loc, {:error, _reason}}} ->
        Process.demonitor(ref, [:flush])
        task = Enum.find(tasks, fn t -> t.ref == ref end)
        elapsed = System.monotonic_time(:millisecond) - start
        receive_results(tasks, MapSet.delete(remaining, task), max(0, timeout - elapsed))

      {:DOWN, ref, :process, _pid, reason} ->
        task = Enum.find(tasks, fn t -> t.ref == ref end)
        elapsed = System.monotonic_time(:millisecond) - start
        receive_results(tasks, MapSet.delete(remaining, task), max(0, timeout - elapsed))

    after
      timeout ->
        {:error, :spinup_timeout}
    end
  end

  defp do_read(chunk_hash, location) do
    NeonFS.Blob.read(chunk_hash, location.drive_id)
  end
end
```

### Edge Cases

| Scenario | Behaviour |
|----------|-----------|
| All drives spun down | Race all in parallel, serve first responder |
| One drive fails during spin-up | Continue waiting for others |
| All drives fail spin-up | Return `{:error, :all_replicas_failed}` |
| Spin-up exceeds timeout | Return `{:error, :spinup_timeout}` |
| Drive spins up but chunk not found | Try next drive in race |
| Concurrent reads to same chunk | All readers share the spin-up wait |

### Configuration

```yaml
storage:
  drives:
    - path: /mnt/hdd0
      tier: cold
      power_management: spin_down
      idle_timeout: 30m           # Time before spinning down
      spinup_timeout: 2m          # Max wait for spin-up
      idle_threshold: 5s          # I/O gap before entering idle state
```

## I/O Scheduling

NeonFS uses a GenStage-based scheduler to coordinate all I/O operations. The scheduler determines which operation to execute next based on priority and fairness across volumes.

**Design rationale:**

The BEAM handles waiting processes efficiently—millions of lightweight processes can block without issue. There's no need for admission control or backpressure signalling. Clients simply wait until their operation completes. The scheduler's job is to decide *which* work to do when capacity is available, not to reject work.

### GenStage Pipeline

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  IO.Producer    │────▶│  IO.Worker (1)  │────▶│  Drive I/O      │
│                 │────▶│  IO.Worker (2)  │────▶│  Drive I/O      │
│  Priority queues│────▶│  IO.Worker (N)  │────▶│  Drive I/O      │
│  + WFQ selection│     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
     demand ◀───────────────────┘
```

Workers pull operations from the producer as they complete work. This provides natural flow control—if workers are busy, demand stops and operations queue in the producer.

### Priority Levels

Operations are assigned to priority queues:

| Priority | Operations |
|----------|------------|
| Critical | Repairs for volumes with `repair_priority: :critical`, quorum writes |
| High | Repairs for `repair_priority: :high`, eviction under pressure |
| Normal | Standard reads/writes, promotions, normal repairs |
| Low | Background scrubbing, rebalancing, low-priority repairs |

Higher priority operations are always selected before lower priority ones.

### Weighted Fair Queuing

Within each priority level, volumes get fair access based on configurable weights:

```elixir
defmodule NeonFS.IO.Producer do
  use GenStage

  defstruct [
    :queues,           # %{priority => :queue.queue(operation)}
    :volume_weights,   # %{volume_id => float}
    :volume_vtime,     # %{volume_id => float} - virtual time for WFQ
    :global_vtime,     # Global virtual time
    :pending_demand    # Unfulfilled demand from workers
  ]

  def init(_) do
    {:producer, %__MODULE__{
      queues: %{critical: :queue.new(), high: :queue.new(),
                normal: :queue.new(), low: :queue.new()},
      volume_weights: %{},
      volume_vtime: %{},
      global_vtime: 0,
      pending_demand: 0
    }}
  end

  # Operation submitted
  def handle_cast({:submit, operation}, state) do
    priority = operation.priority
    queues = Map.update!(state.queues, priority, &:queue.in(operation, &1))
    state = %{state | queues: queues}

    {events, state} = dispatch(state, state.pending_demand)
    {:noreply, events, state}
  end

  # Worker wants more work
  def handle_demand(demand, state) do
    {events, state} = dispatch(state, state.pending_demand + demand)
    {:noreply, events, state}
  end

  defp dispatch(state, 0), do: {[], state}
  defp dispatch(state, demand) do
    case select_batch(state, demand, []) do
      {[], state} -> {[], %{state | pending_demand: demand}}
      {events, state} -> {events, %{state | pending_demand: demand - length(events)}}
    end
  end

  defp select_batch(state, 0, acc), do: {Enum.reverse(acc), state}
  defp select_batch(state, remaining, acc) do
    case select_one(state) do
      {:ok, op, state} -> select_batch(state, remaining - 1, [op | acc])
      :empty -> {Enum.reverse(acc), state}
    end
  end

  # Select from highest non-empty priority, using WFQ within priority
  defp select_one(state) do
    Enum.find_value([:critical, :high, :normal, :low], :empty, fn priority ->
      queue = Map.get(state.queues, priority)
      if :queue.is_empty(queue), do: nil, else: select_by_wfq(state, priority)
    end)
  end

  defp select_by_wfq(state, priority) do
    queue = Map.get(state.queues, priority)
    items = :queue.to_list(queue)
    by_volume = Enum.group_by(items, & &1.volume_id)

    # Select volume with lowest virtual finish time
    {vol_id, _vft} = by_volume
      |> Map.keys()
      |> Enum.map(fn vid ->
        weight = Map.get(state.volume_weights, vid, 1.0)
        vtime = Map.get(state.volume_vtime, vid, 0)
        vft = max(state.global_vtime, vtime) + (1.0 / weight)
        {vid, vft}
      end)
      |> Enum.min_by(&elem(&1, 1))

    # Take first item from selected volume, rebuild queue
    [item | rest] = by_volume[vol_id]
    new_items = by_volume |> Map.put(vol_id, rest) |> Map.values() |> List.flatten()
    new_queue = Enum.reduce(new_items, :queue.new(), &:queue.in/2)

    # Update virtual times
    weight = Map.get(state.volume_weights, vol_id, 1.0)
    new_vtime = max(state.global_vtime, Map.get(state.volume_vtime, vol_id, 0)) + (1.0 / weight)

    state = %{state |
      queues: Map.put(state.queues, priority, new_queue),
      volume_vtime: Map.put(state.volume_vtime, vol_id, new_vtime),
      global_vtime: max(state.global_vtime, new_vtime)
    }

    {:ok, item, state}
  end
end
```

### Workers

Workers subscribe to the producer and process operations as they arrive:

```elixir
defmodule NeonFS.IO.Worker do
  use GenStage

  def init(opts) do
    {:consumer, %{}, subscribe_to: [
      {NeonFS.IO.Producer, max_demand: 10, min_demand: 5}
    ]}
  end

  def handle_events(operations, _from, state) do
    Enum.each(operations, &execute_operation/1)
    {:noreply, [], state}
  end

  defp execute_operation(%{type: :read} = op), do: # ...
  defp execute_operation(%{type: :write} = op), do: # ...
  defp execute_operation(%{type: :repair} = op), do: # ...
  defp execute_operation(%{type: :migrate} = op), do: # ...
end
```

### Background Task Yielding

Background operations (scrubbing, rebalancing) yield to foreground work by using lower priority levels. When the system is busy with critical/high/normal operations, low-priority work naturally waits.

For additional throttling during sustained high load:

```elixir
defmodule NeonFS.Background.Scheduler do
  @yield_threshold 0.8  # Pause background work above this load

  def maybe_schedule(task) do
    if current_load() > @yield_threshold do
      Process.send_after(self(), {:retry, task}, backoff_ms())
    else
      NeonFS.IO.Producer.submit(%{task | priority: :low})
    end
  end

  defp current_load do
    # Ratio of pending demand to total worker capacity
    NeonFS.IO.Producer.queue_depth() / NeonFS.IO.Worker.total_capacity()
  end
end
```

### Configuration

```yaml
io_scheduler:
  # Number of worker processes
  workers: 8

  # Default volume weight (higher = more I/O share)
  default_volume_weight: 1.0

  # Worker demand settings
  max_demand: 10
  min_demand: 5

  # Background task throttling
  background_yield_threshold: 0.8
```

Per-volume weights can be configured:

```yaml
volumes:
  documents:
    io_weight: 2.0    # Gets 2x fair share

  media:
    io_weight: 1.0    # Default share

  scratch:
    io_weight: 0.5    # Half share (best-effort)
```

## Tier Capacity and Contention

Tiers have limited capacity. When a tier is full, chunks must be evicted to make room for hotter data.

### Promotion/Demotion Logic

Each chunk tracks access frequency:

```elixir
%ChunkAccessStats{
  hash: "sha256:abc...",
  access_count_1h: 15,      # Accesses in last hour
  access_count_24h: 47,     # Accesses in last 24 hours
  last_access: ~U[...],
  current_tier: :warm,
  preferred_tier: :hot      # Based on access pattern
}
```

### Eviction Under Pressure

When hot tier reaches capacity threshold (e.g., 90%):

```elixir
def evict_from_tier(tier, bytes_needed) do
  candidates = chunks_in_tier(tier)
    |> Enum.sort_by(&coldness_score/1)  # Least accessed first
    |> Stream.take_while(fn _ -> bytes_freed < bytes_needed end)

  for chunk <- candidates do
    demote_chunk(chunk, next_colder_tier(tier))
  end
end

defp coldness_score(chunk) do
  # Lower score = colder = better eviction candidate
  recency = hours_since(chunk.last_access)
  frequency = chunk.access_count_24h

  -recency + (frequency * 10)
end
```

### Promotion Under Contention

When a cold chunk is accessed frequently but hot tier is full:
1. Calculate if this chunk is "hotter" than coldest hot-tier chunk
2. If yes, demote coldest hot-tier chunk, promote this one
3. If no, leave it in place (it will be served from cold tier)

```elixir
def maybe_promote(chunk) do
  if should_promote?(chunk) do
    target_tier = preferred_tier(chunk)

    if tier_has_space?(target_tier) do
      schedule_migration(chunk.hash, chunk.current_tier, target_tier, :promotion)
    else
      coldest = coldest_chunk_in_tier(target_tier)
      if hotter_than?(chunk, coldest) do
        schedule_migration(coldest.hash, target_tier, next_colder_tier(target_tier), :eviction)
        schedule_migration(chunk.hash, chunk.current_tier, target_tier, :promotion)
      end
    end
  end
end
```

## Tier Migration

Tier migrations are multi-step operations that must handle crashes and concurrent access safely. NeonFS uses the intent log + Reactor saga system (see [Metadata - Intent Log](metadata.md#intent-log-transaction-safety-and-write-coordination)) to coordinate migrations.

Note that tier migration may be cross-node: if Node A only has SSD (hot) and needs to demote a chunk to cold tier, the chunk must move to Node B which has HDD (cold).

### Migration as Reactor Saga

```elixir
defmodule NeonFS.Operations.MigrateChunk do
  use Reactor

  input :chunk_hash
  input :from_node
  input :from_tier
  input :to_node
  input :to_tier

  step :copy_to_destination, NeonFS.Steps.CopyChunkToTier do
    argument :chunk_hash, input(:chunk_hash)
    argument :from_node, input(:from_node)
    argument :from_tier, input(:from_tier)
    argument :to_node, input(:to_node)
    argument :to_tier, input(:to_tier)
    # run: copy chunk data to new location (possibly cross-node)
    # undo: delete from new location
  end

  step :add_new_location, NeonFS.Steps.AddChunkLocation do
    argument :chunk_hash, input(:chunk_hash)
    argument :node, input(:to_node)
    argument :tier, input(:to_tier)
    wait_for :copy_to_destination
    # run: add new location to chunk metadata
    # undo: remove new location from metadata
  end

  step :remove_old_location, NeonFS.Steps.RemoveChunkLocation do
    argument :chunk_hash, input(:chunk_hash)
    argument :node, input(:from_node)
    argument :tier, input(:from_tier)
    wait_for :add_new_location
    # run: remove old location from chunk metadata
    # undo: re-add old location to metadata
  end

  # No explicit delete step - GC handles physical cleanup of orphaned chunks

  return :remove_old_location
end
```

### Execution with Intent Log

The intent log provides exclusive access and crash recovery:

```elixir
def migrate_chunk(chunk_hash, from_node, from_tier, to_node, to_tier, reason) do
  intent = %Intent{
    id: UUID.uuid4(),
    operation: :migrate_chunk,
    conflict_key: {:chunk_migration, chunk_hash},
    params: %{
      chunk_hash: chunk_hash,
      from_node: from_node,
      from_tier: from_tier,
      to_node: to_node,
      to_tier: to_tier
    },
    state: :pending,
    started_at: DateTime.utc_now(),
    expires_at: DateTime.add(DateTime.utc_now(), 300, :second)
  }

  case IntentLog.try_acquire(intent) do
    {:error, :conflict, _existing} ->
      # Another migration of this chunk is in progress
      {:error, :migration_in_progress}

    {:ok, intent_id} ->
      result = Reactor.run(NeonFS.Operations.MigrateChunk, %{
        chunk_hash: chunk_hash,
        from_node: from_node,
        from_tier: from_tier,
        to_node: to_node,
        to_tier: to_tier
      })

      case result do
        {:ok, _} ->
          IntentLog.complete(intent_id)
          :ok

        {:error, reason} ->
          IntentLog.fail(intent_id, reason)
          {:error, reason}
      end
  end
end

def schedule_migration(chunk_hash, from_node, from_tier, to_node, to_tier, reason) do
  priority = case reason do
    :eviction_pressure -> :high   # Need space now
    :promotion -> :normal         # Performance optimisation
    :rebalancing -> :low          # Background task
  end

  NeonFS.IO.Producer.submit(%{
    type: :tier_migration,
    chunk_hash: chunk_hash,
    from_node: from_node,
    from_tier: from_tier,
    to_node: to_node,
    to_tier: to_tier,
    priority: priority
  })
end
```

### Read Safety During Migration

During migration, the chunk exists in at least one valid metadata location throughout:

| Phase | Source Location | Dest Location | Reads Work? |
|-------|-----------------|---------------|-------------|
| Before migration | ✓ In metadata | ✗ Not in metadata | Yes (source) |
| After copy | ✓ In metadata | ✓ In metadata | Yes (either) |
| After metadata update | ✗ Not in metadata | ✓ In metadata | Yes (dest) |
| After GC cleanup | ✗ Deleted | ✓ Has data | Yes (dest) |

The key safety property: **metadata is updated before physical deletion**. After `remove_old_location` completes, reads will no longer be directed to the source location. The physical chunk at the source becomes orphaned and is cleaned up by GC.

### Crash Recovery

On node startup (or when any node handles an expired migration intent), recovery checks metadata state:

```elixir
defp recover_intent(%Intent{operation: :migrate_chunk} = intent) do
  %{chunk_hash: hash, from_node: from_node, from_tier: from_tier,
    to_node: to_node, to_tier: to_tier} = intent.params

  in_source_meta? = ChunkIndex.has_location?(hash, from_node, from_tier)
  in_dest_meta? = ChunkIndex.has_location?(hash, to_node, to_tier)

  case {in_source_meta?, in_dest_meta?} do
    {true, false} ->
      # Copy never completed or was rolled back
      # Any orphaned chunk at destination will be cleaned up by GC
      IntentLog.mark_rolled_back(intent.id, :no_changes_made)

    {true, true} ->
      # Copy done, metadata partially updated - finish removing old location
      ChunkIndex.remove_location(hash, from_node, from_tier)
      # Physical chunk at source is now orphaned, GC will clean it up
      IntentLog.complete(intent.id)

    {false, true} ->
      # Migration complete
      IntentLog.complete(intent.id)

    {false, false} ->
      # Chunk not in metadata anywhere - check if it physically exists
      if chunk_exists_anywhere?(hash) do
        # Metadata lost but data exists - needs repair, not migration
        Logger.warning("Chunk #{hash} exists but not in metadata, flagging for repair")
        IntentLog.fail(intent.id, :metadata_inconsistent)
      else
        Logger.error("Chunk #{hash} lost during tier migration")
        IntentLog.fail(intent.id, :chunk_lost)
      end
  end
end
```

Note: Physical chunk cleanup is handled by GC, not by migration recovery. This keeps the migration saga simple and avoids coordination issues with cross-node deletion.

### Configuration

```yaml
tier_migration:
  # Maximum concurrent migrations per node
  max_concurrent: 10

  # Intent TTL for migrations
  intent_ttl: 5m

  # Batch size for pressure-driven evictions
  eviction_batch_size: 100
```

## Caching Strategy

NeonFS uses a two-tier caching approach:

### Linux Page Cache (automatic)

For raw chunk data (unencrypted, uncompressed), Linux's page cache provides automatic, efficient caching:
- Recently read chunks stay in memory
- Kernel manages eviction based on memory pressure
- No application overhead
- Works transparently for all file reads

This handles the common case well. An unencrypted, uncompressed volume with local access needs no application-level caching.

### Application-Level Cache (for transformed data)

Application-level caching is only beneficial when there's expensive transformation work to save:

| Scenario | Why cache? |
|----------|------------|
| Encrypted chunks | Avoid repeated AES decryption |
| Compressed chunks | Avoid repeated zstd decompression |
| Erasure-coded reads | Avoid reconstructing from parity (expensive) |
| Remote chunks | Avoid network round-trips |

For volumes with no encryption, no compression, and local-only access, the application cache provides no benefit over the page cache.

### Cache Implementation

```elixir
defmodule NeonFS.ChunkCache do
  # LRU cache keyed by {hash, transform_state}
  # transform_state = :raw | :decrypted | :decompressed | :reconstructed

  def get(hash, volume) do
    cache_key = {hash, transform_key(volume)}

    case :ets.lookup(@cache_table, cache_key) do
      [{_, data, _timestamp}] ->
        touch(cache_key)
        {:hit, data}
      [] ->
        :miss
    end
  end

  def put(hash, data, volume) do
    if should_cache?(volume) do
      cache_key = {hash, transform_key(volume)}
      :ets.insert(@cache_table, {cache_key, data, now()})
      maybe_evict()
    end
  end

  defp should_cache?(volume) do
    volume.caching.transformed_chunks or
    volume.caching.reconstructed_stripes or
    volume.caching.remote_chunks
  end

  defp transform_key(volume) do
    cond do
      volume.encryption.mode != :none -> :decrypted
      volume.compression.algorithm != :none -> :decompressed
      true -> :raw
    end
  end
end
```

### Cache Eviction

Per-volume memory limits are enforced with LRU eviction:

```elixir
defp maybe_evict do
  current_size = :ets.info(@cache_table, :memory) * :erlang.system_info(:wordsize)

  if current_size > @max_cache_size do
    # Evict oldest entries until under limit
    entries = :ets.tab2list(@cache_table)
    |> Enum.sort_by(fn {_key, _data, timestamp} -> timestamp end)
    |> Enum.take(div(length(entries), 4))  # Evict oldest 25%

    Enum.each(entries, fn {key, _, _} -> :ets.delete(@cache_table, key) end)
  end
end
```

### When NOT to Use Application Caching

For scratch volumes or any volume where:
- `encryption.mode == :none`
- `compression.algorithm == :none`
- Data is primarily local (not erasure-coded, not multi-site)

Set all caching options to false and let Linux handle it.
