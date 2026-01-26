# Replication and Durability

This document describes write flows, durability guarantees, erasure coding, and garbage collection.

## Write Flow (Replicated Volume)

Writes are streamed — chunks are created and replicated as bytes arrive, with metadata committed only on completion.

```
1. Client opens write stream, receives write_id
2. As bytes arrive:
   a. Chunk engine splits stream into chunks
   b. Each chunk written immediately to local storage
   c. Replication initiated in parallel (pipelined)
   d. Chunks tagged with write_id, state: :uncommitted
3. When client signals completion:
   a. Verify all chunks received and replicated per policy
   b. Atomically: create/update file metadata, mark chunks :committed
   c. Ack to client
4. If client disconnects or aborts:
   a. Mark write_id as :abandoned
   b. Chunks remain uncommitted, eligible for cleanup
```

This pipelining means a large file upload can have chunks replicating to other nodes while still receiving bytes from the client.

## Write Flow (Erasure-Coded Volume)

```
1. Client opens write stream, receives write_id
2. As bytes arrive:
   a. Chunk engine accumulates data chunks
   b. When stripe complete (e.g., 10 data chunks):
      - Compute parity chunks (e.g., 4 parity)
      - Distribute all 14 chunks to different nodes
      - Tag with write_id, state: :uncommitted
3. On completion:
   a. Handle any partial final stripe (pad or use smaller config)
   b. Verify all stripes replicated
   c. Commit metadata
4. On abort: mark abandoned, cleanup later
```

## Uncommitted Chunks and Orphan Cleanup

Chunks from incomplete writes must not leak storage.

```
UncommittedChunk {
  hash: SHA256
  write_id: WriteId
  created_at: DateTime
  ttl: Duration              # Default: 24 hours
  state: :uncommitted | :abandoned
}
```

**Lifecycle:**

```
                    ┌─────────────┐
  write arrives ──▶ │ uncommitted │
                    └─────────────┘
                          │
           ┌──────────────┼──────────────┐
           │              │              │
           ▼              ▼              ▼
    ┌───────────┐  ┌───────────┐  ┌───────────┐
    │ committed │  │ abandoned │  │  expired  │
    │ (success) │  │  (abort)  │  │  (ttl)    │
    └───────────┘  └───────────┘  └───────────┘
           │              │              │
           │              └──────┬───────┘
           │                     │
           ▼                     ▼
      permanent             GC eligible
```

**Message-driven cleanup:**

Rather than periodic sweeps, the system uses message-driven cleanup:

```elixir
defmodule ChunkReaper do
  use GenServer

  # Called when a write is explicitly abandoned
  def handle_cast({:write_abandoned, write_id}, state) do
    schedule_cleanup(write_id, delay: :soon)
    {:noreply, state}
  end

  # Called when TTL expires (via Process.send_after)
  def handle_info({:ttl_expired, write_id}, state) do
    schedule_cleanup(write_id, delay: :soon)
    {:noreply, state}
  end

  # Actual cleanup considers current system state
  def handle_info({:do_cleanup, write_id}, state) do
    if good_time_to_cleanup?() do
      chunks = get_uncommitted_chunks(write_id)
      Enum.each(chunks, &delete_chunk/1)
    else
      # System busy, try again later
      schedule_cleanup(write_id, delay: :later)
    end
    {:noreply, state}
  end

  defp good_time_to_cleanup? do
    # Don't compete with active I/O
    io_pressure = Metrics.current_io_pressure()
    storage_pressure = Metrics.storage_pressure()

    cond do
      storage_pressure > 0.9 -> true   # Need space now
      io_pressure < 0.3 -> true        # System is idle
      true -> false                     # Wait for better time
    end
  end
end
```

This approach:
- Responds immediately to explicit aborts
- Handles TTL expiry via scheduled messages
- Defers actual deletion to low-contention periods
- Prioritises cleanup when storage is tight

## Quorum Configurations

| Policy | Behaviour | Durability | Latency |
|--------|----------|------------|---------|
| `local` | Ack after local write | Lowest | Lowest |
| `quorum` | Ack after W of N confirm (e.g., 2 of 3) | Medium | Medium |
| `all` | Ack after all replicas confirm | Highest | Highest |

## Write Hole Mitigation

The write hole (client ack'd but replication incomplete when primary fails) is addressed by:

1. **Quorum writes**: Don't ack until W of N replicas confirm
2. **Pending write tracking**: Background process monitors incomplete replications
3. **Deadline enforcement**: Writes must complete replication within timeout or alert

## Known Risk: Local Write Acknowledgement

For volumes configured with `write_ack: :local`, there is an acknowledged data loss window:

```
1. Client writes chunk
2. Local node stores chunk, acks to client ✓
3. Background replication begins...
4. Local node fails before replication completes
5. Chunk is lost (client believed write was durable)
```

**This is by design**: Users choosing `:local` explicitly trade durability for write latency. This policy is intended for:
- Scratch/temp data that can be regenerated
- Build artifacts and caches
- Data where speed matters more than durability

**Volume creation should warn users**:
```
$ neonfs volume create scratch --write-ack local

WARNING: write_ack=local means data may be lost if this node fails
before background replication completes. Only use for data that
can be regenerated. Continue? [y/N]
```

**Possible future mitigations** (not currently implemented):
1. **Local WAL**: Write to fast local WAL before ack, replicate from WAL asynchronously. Survives process crash but not disk failure.
2. **Delayed ack**: Ack after min(replication_complete, N milliseconds). Bounds the risk window.
3. **Synchronous local redundancy**: Write to two local drives before ack. Survives single drive failure.

For now, the risk is documented and users must explicitly opt in.

```elixir
%PendingWrite{
  chunk: "sha256:abc123",
  targets: [:node1, :node2, :node3],
  confirmed: [:node1],
  created_at: ~U[...],
  deadline: ~U[...]  # Must complete by this time
}
```

## Erasure Coding Configuration

For storage-efficient durability, use erasure coding rather than low replication factors.

Recommended configurations:

| Config | Overhead | Fault Tolerance | Use Case |
|--------|----------|-----------------|----------|
| 4+2 | 1.5x | 2 failures | Small clusters |
| 10+4 | 1.4x | 4 failures | Large media archives |
| 8+3 | 1.375x | 3 failures | Balanced |

**Read path:**

- Best case: all data chunks available, no decoding needed
- Degraded: some data chunks missing, fetch any K of N (data + parity), decode

**Repair:** More expensive than replication (must read K chunks to rebuild 1) but storage-efficient.

## Partial Stripe Handling

Erasure coding requires a minimum amount of data to form a stripe (e.g., 10 data chunks × 256KB = 2.5MB for a 10+4 config). Files smaller than this, or the tail end of larger files, need special handling.

**Strategy: Hybrid Replication Fallback**

| Scenario | Strategy |
|----------|----------|
| File < min_stripe_size | Replicate using fallback replica count |
| Full stripes in large file | Erasure code normally |
| Partial final stripe | Replicate the remainder |

This avoids:
- Massive overhead from padding small files (50KB → 2.5MB)
- Complexity of packing unrelated files into shared stripes
- Under-protected partial stripes waiting for more data

**Configuration:**

```elixir
%Volume{
  name: "media",
  durability: %{
    type: :erasure,
    data_chunks: 10,
    parity_chunks: 4,

    # Handling data that doesn't fill a stripe
    small_file_strategy: :replicate,    # :replicate | :pad
    small_file_replicas: 3,             # Replica count when using :replicate
    min_stripe_threshold: 2_500_000     # Bytes; below this, use small_file_strategy
  }
}
```

**Example: 5MB file on 10+4 volume (256KB chunks)**

```
File: 5MB total
Stripe capacity: 10 × 256KB = 2.56MB

Stripe 1: chunks 0-9 (2.56MB) → erasure coded (10 data + 4 parity)
Remainder: ~2.44MB (chunks 10-18) → replicated (3 copies each)
```

The remainder doesn't fill a stripe, so it falls back to replication. If the file grows later (append), the replicated chunks can be promoted into a full stripe.

**Alternative: Padding (not recommended)**

Setting `small_file_strategy: :pad` will pad partial stripes with zero-filled chunks. This wastes storage but keeps all data under the same durability model. Only use if consistency of durability model matters more than storage efficiency.

## Garbage Collection

GC uses a simple metadata-walk approach rather than distributed reference counting. This trades some efficiency for correctness and simplicity.

**Approach: Mark and Sweep**

```
1. Mark phase: Walk all committed file metadata, collect referenced chunk hashes
2. Sweep phase: Any chunk not in the referenced set is garbage
3. Delete phase: Remove garbage chunks (with grace period)
```

**Why not reference counting?**

Distributed reference counting (including weighted variants) has failure modes where lost messages can prevent chunks from ever being collected, or worse, cause premature deletion. The coordination required to handle these failures correctly negates the performance benefit.

GC is a background operation—it can afford to be slow and thorough.

**Implementation:**

```elixir
defmodule NeonFS.GarbageCollector do
  def collect(volume) do
    # Phase 1: Build set of all referenced chunks
    referenced = volume
      |> stream_all_files()
      |> Stream.flat_map(& &1.chunks)
      |> MapSet.new()

    # Phase 2: Find unreferenced chunks
    garbage = volume
      |> stream_all_chunks()
      |> Stream.reject(&MapSet.member?(referenced, &1.hash))
      |> Enum.to_list()

    # Phase 3: Delete with grace period
    for chunk <- garbage do
      if chunk.created_at < hours_ago(24) do
        schedule_deletion(chunk, grace_period: hours(1))
      end
    end
  end
end
```

**Safety measures:**

- **Grace period**: Chunks aren't deleted immediately; scheduled for deletion after 1 hour. Allows cancellation if a race condition is detected.
- **Age threshold**: Only chunks older than 24 hours are considered for GC. Recent chunks may be part of in-progress writes.
- **Uncommitted chunk exclusion**: Chunks with `write_id` set (uncommitted) are never collected by GC—handled separately by the ChunkReaper.

**Scheduling:**

- GC runs during low-activity periods (configurable)
- Can be triggered manually or by storage pressure
- Per-volume scheduling: critical volumes checked more frequently

```yaml
gc:
  schedule: "0 3 * * *"      # Daily at 3 AM
  storage_pressure_threshold: 0.85  # Also run if >85% full
  grace_period: 1h
  age_threshold: 24h
```
