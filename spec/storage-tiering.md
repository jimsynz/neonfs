# Storage Tiering

This document describes the storage tier system, drive management, power management, and caching strategies.

## Tier Definitions

| Tier | Typical Media | Use Case |
|------|--------------|----------|
| Hot | NVMe SSD | Active working set |
| Warm | SATA SSD | Recent/moderate access |
| Cold | HDD | Archive, infrequent access |

Linux's page cache provides automatic caching of recently accessed file data, which covers most caching needs for raw (unencrypted, uncompressed) chunks. Application-level caching is only used for transformed data — see the Caching Strategy section below.

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
      promote_chunk(chunk, target_tier)
    else
      coldest = coldest_chunk_in_tier(target_tier)
      if hotter_than?(chunk, coldest) do
        demote_chunk(coldest, next_colder_tier(target_tier))
        promote_chunk(chunk, target_tier)
      end
    end
  end
end
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
