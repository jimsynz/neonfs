# Replication and Durability

This document describes write flows, durability guarantees, erasure coding, and garbage collection.

## Replica Placement

Replicas are placed to maximize failure domain separation. The placement policy considers:

1. **Zones** (if configured) - different physical sites
2. **Nodes** - different machines
3. **Drives** - different storage devices

See [Storage Tiering - Replica Placement Policy](storage-tiering.md#replica-placement-policy) for the full algorithm.

**Key points:**

- A single node with multiple drives can provide drive-level redundancy
- Replicas prefer same-tier placement but fall back to other tiers if needed
- If requested replication factor exceeds available failure domains, writes proceed with a warning (some redundancy is better than none)
- Chunk locations track `{node, drive_id, tier}` not just `{node, tier}`

**Examples:**

| Hardware | Replication Factor | Result |
|----------|-------------------|--------|
| 3 nodes × 1 drive | 3 | 1 replica per node (node redundancy) |
| 1 node × 3 drives | 3 | 1 replica per drive (drive redundancy) |
| 2 nodes × 2 drives | 3 | Spread across nodes and drives |
| 1 node × 1 drive | 3 | Only 1 copy possible (warning logged) |

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

## Uncommitted Chunks and Deduplication

Chunks from incomplete writes must not leak storage. Additionally, concurrent writes producing identical content must share chunks rather than creating duplicates (which would break content-addressing and waste storage for large identical uploads).

### Reference-Counted Chunks

Each chunk tracks which active writes are using it, regardless of commit state:

```elixir
%ChunkMeta{
  hash: <<sha256 bytes>>,
  commit_state: :uncommitted | :committed,

  # All in-flight writes using this chunk (created it OR reusing it)
  # GC checks this is empty before deleting any chunk
  active_write_refs: MapSet.new(["write_abc", "write_def"])
}
```

### Chunk Storage Operations

```elixir
defmodule NeonFS.ChunkStore do
  @doc """
  Store a chunk for a write operation.
  - If chunk doesn't exist: create uncommitted with write_id in active_write_refs
  - If chunk exists (committed or not): add write_id to active_write_refs
  This protects the chunk from GC while the write is in progress.
  """
  def store_chunk(hash, data, write_id) do
    case ChunkIndex.lookup(hash) do
      nil ->
        ChunkIndex.create(%ChunkMeta{
          hash: hash,
          commit_state: :uncommitted,
          active_write_refs: MapSet.new([write_id]),
          created_at: DateTime.utc_now()
        }, data)
        {:ok, :created}

      %ChunkMeta{commit_state: :committed} = chunk ->
        # Already committed - add our ref to protect from GC during our write
        ChunkIndex.update(hash, %{
          active_write_refs: MapSet.put(chunk.active_write_refs, write_id)
        })
        {:ok, :existing}

      %ChunkMeta{commit_state: :uncommitted} = chunk ->
        # Another write's uncommitted chunk - share it
        ChunkIndex.update(hash, %{
          active_write_refs: MapSet.put(chunk.active_write_refs, write_id)
        })
        {:ok, :shared}
    end
  end

  @doc """
  Called when a write commits successfully.
  Transitions uncommitted chunks to committed and removes our ref from all chunks.
  """
  def commit_chunks(write_id, chunk_hashes) do
    for hash <- chunk_hashes do
      case ChunkIndex.lookup(hash) do
        %ChunkMeta{commit_state: :uncommitted} = chunk ->
          # Transition to committed and remove our ref
          ChunkIndex.update(hash, %{
            commit_state: :committed,
            active_write_refs: MapSet.delete(chunk.active_write_refs, write_id)
          })

        %ChunkMeta{commit_state: :committed} = chunk ->
          # Already committed - just remove our ref
          ChunkIndex.update(hash, %{
            active_write_refs: MapSet.delete(chunk.active_write_refs, write_id)
          })
      end
    end
  end

  @doc """
  Called when a write aborts.
  Removes write_id from active_write_refs; deletes uncommitted chunk if no refs remain.
  """
  def abort_chunks(write_id, chunk_hashes) do
    for hash <- chunk_hashes do
      case ChunkIndex.lookup(hash) do
        %ChunkMeta{commit_state: :uncommitted} = chunk ->
          new_refs = MapSet.delete(chunk.active_write_refs, write_id)

          if MapSet.size(new_refs) == 0 do
            # Last reference gone - delete immediately
            ChunkIndex.delete(hash)
          else
            # Other writes still need this chunk
            ChunkIndex.update(hash, %{active_write_refs: new_refs})
          end

        %ChunkMeta{commit_state: :committed} = chunk ->
          # Committed chunk - just remove our ref (GC handles deletion later)
          ChunkIndex.update(hash, %{
            active_write_refs: MapSet.delete(chunk.active_write_refs, write_id)
          })
      end
    end
  end
end
```

### Lifecycle

```
                         store_chunk(write_A)
                                │
                                ▼
                    ┌───────────────────────┐
                    │     :uncommitted      │
                    │ active_write_refs: [A]│
                    └───────────────────────┘
                                │
              store_chunk(write_B, same hash)
                                │
                                ▼
                    ┌───────────────────────┐
                    │     :uncommitted      │
                    │ active_write_refs:[A,B]│
                    └───────────────────────┘
                                │
         ┌──────────────────────┼──────────────────────┐
         │                      │                      │
    A commits              A aborts                A aborts
         │                      │                 then B aborts
         ▼                      ▼                      │
┌─────────────────┐  ┌───────────────────────┐         │
│   :committed    │  │     :uncommitted      │         │
│ refs: [B]       │  │ active_write_refs: [B]│         │
│                 │  │                       │         │
│ Referenced by   │  │ B can still commit    │         │
│ FileMeta (A's)  │  │ or abort later        │         │
│ Protected by B  │  │                       │         │
└─────────────────┘  └───────────────────────┘         │
         │                      │                      │
    B commits              B commits                   │
         │                      │                      │
         ▼                      ▼                      ▼
┌─────────────────┐  ┌─────────────────┐    ┌─────────────────┐
│   :committed    │  │   :committed    │    │ refs: []        │
│ refs: []        │  │ refs: []        │    │                 │
│                 │  │                 │    │ DELETED         │
│ Referenced by   │  │ Referenced by   │    │ (no refs left)  │
│ FileMeta (A+B)  │  │ FileMeta (B's)  │    └─────────────────┘
│ GC eligible if  │  │ GC eligible if  │
│ files deleted   │  │ files deleted   │
└─────────────────┘  └─────────────────┘
```

### Why No Grace Period?

Previous designs used a TTL/grace period before deleting orphaned uncommitted chunks. This is unnecessary because:

1. **Intent log handles crashes**: The intent log (see [metadata.md](metadata.md#intent-log-transaction-safety-and-write-coordination)) ensures that crashed writes are properly recovered and aborted on node restart
2. **Reference counting is precise**: A chunk is only deleted when *all* referencing writes have explicitly committed or aborted
3. **Late-arriving writes recreate if needed**: If a write arrives after a chunk was deleted, it simply creates the chunk fresh

The grace period was defending against incomplete cleanup, but the intent log already guarantees cleanup completes.

### Edge Cases

| Scenario | Behaviour |
|----------|-----------|
| A creates chunk, B shares, A commits | Chunk committed with refs: [B]; B's commit removes its ref |
| A creates chunk, B shares, A aborts | Chunk kept for B (active_write_refs: [B]) |
| A creates chunk, B shares, both abort | Chunk deleted when second abort removes last ref |
| A creates chunk, commits before B starts | B adds ref to committed chunk, protects from GC |
| GC runs while write in progress | Chunk protected by active_write_refs, not deleted |
| Crash during write | Intent log recovery calls abort_chunks on restart |
| C arrives after chunk deleted | C creates chunk fresh (normal path) |

### Integration with Intent Log

Write operations are wrapped with intent log acquisition (see [metadata.md](metadata.md#intent-log-transaction-safety-and-write-coordination)). The intent log ensures:

1. **Exclusive access**: Only one write can modify a file at a time
2. **Crash recovery**: Incomplete writes are detected and aborted on restart
3. **Proper cleanup**: `abort_chunks` is always called for failed/crashed writes

```elixir
# In intent log recovery (runs on node startup)
def recover_intent(%Intent{operation: :write_file, state: :pending} = intent) do
  # Write was in progress when node crashed - abort it
  chunk_hashes = get_chunks_for_write(intent.id)
  ChunkStore.abort_chunks(intent.id, chunk_hashes)
  IntentLog.mark_rolled_back(intent.id, :recovered_after_crash)
end
```

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

### Reading from Erasure-Coded Files

Files on erasure-coded volumes reference stripes with byte ranges (see [Data Model - File References for Erasure-Coded Volumes](data-model.md#file-references-for-erasure-coded-volumes)). The read path handles both full and partial stripes:

```elixir
def read_file(file_meta, offset, length) do
  case file_meta.stripes do
    nil ->
      # Replicated volume - read chunks directly
      read_from_chunks(file_meta.chunks, offset, length)

    stripes ->
      # Erasure-coded volume - read from stripes
      read_from_stripes(stripes, offset, length, file_meta.size)
  end
end

defp read_from_stripes(stripes, offset, length, file_size) do
  end_offset = min(offset + length, file_size)

  # Find stripes that overlap the requested range
  relevant_stripes = Enum.filter(stripes, fn %{byte_range: {start, end_}} ->
    start < end_offset and end_ > offset
  end)

  # Read from each stripe, respecting byte boundaries
  data = Enum.map(relevant_stripes, fn %{stripe_id: id, byte_range: {start, end_}} ->
    # Calculate offset within this stripe
    stripe_offset = max(0, offset - start)
    stripe_length = min(end_ - start - stripe_offset, end_offset - max(offset, start))

    read_stripe(id, stripe_offset, stripe_length)
  end)

  IO.iodata_to_binary(data)
end
```

### Reading from Stripes (Healthy and Degraded)

Stripe reads must respect the `data_bytes` boundary to avoid returning padding:

```elixir
def read_stripe(stripe_id, offset_in_stripe, length) do
  stripe = Metadata.get_stripe(stripe_id)

  # Can't read past data_bytes (rest is padding)
  max_readable = max(0, stripe.data_bytes - offset_in_stripe)
  actual_length = min(length, max_readable)

  if actual_length <= 0 do
    <<>>  # Requested range is entirely in padding region
  else
    raw_data = case calculate_stripe_state(stripe) do
      :healthy -> read_stripe_healthy(stripe, offset_in_stripe, actual_length)
      :degraded -> read_stripe_degraded(stripe, offset_in_stripe, actual_length)
      :critical -> {:error, :insufficient_chunks}
    end

    case raw_data do
      {:error, _} = err -> err
      data -> binary_part(data, 0, actual_length)  # Defensive truncation
    end
  end
end

defp read_stripe_healthy(stripe, offset, length) do
  # All data chunks available - read directly without decoding
  # Calculate which data chunks contain the requested range
  chunk_size = stripe.config.chunk_size
  start_chunk = div(offset, chunk_size)
  end_chunk = div(offset + length - 1, chunk_size)

  data_chunks = start_chunk..end_chunk
    |> Enum.map(fn idx -> Enum.at(stripe.chunks, idx) end)
    |> Enum.map(&ChunkStore.read/1)
    |> IO.iodata_to_binary()

  # Extract the exact range from concatenated chunks
  chunk_offset = rem(offset, chunk_size)
  binary_part(data_chunks, chunk_offset, length)
end

defp read_stripe_degraded(stripe, offset, length) do
  # Some chunks missing - need Reed-Solomon reconstruction
  available = stripe.chunks
    |> Enum.with_index()
    |> Enum.filter(fn {hash, _idx} -> chunk_available?(hash) end)

  k = stripe.config.data_chunks

  if length(available) < k do
    {:error, :insufficient_chunks}
  else
    # Fetch K chunks (mix of data and parity as available)
    chunks_to_fetch = Enum.take(available, k)
    chunk_data = Enum.map(chunks_to_fetch, fn {hash, idx} ->
      {idx, ChunkStore.read(hash)}
    end)

    # Reconstruct all data chunks (including padding)
    all_data = ReedSolomon.decode(chunk_data, stripe.config)

    # Extract just the requested range
    binary_part(all_data, offset, length)
  end
end
```

### Stripe State Calculation

```elixir
def calculate_stripe_state(stripe) do
  available_count = Enum.count(stripe.chunks, &chunk_available?/1)
  k = stripe.config.data_chunks
  n = k + stripe.config.parity_chunks

  cond do
    available_count == n -> :healthy   # All chunks present
    available_count >= k -> :degraded  # Can reconstruct
    true -> :critical                  # Data loss - cannot reconstruct
  end
end
```

### Write Validation

When finalising a file write, validate stripe consistency:

```elixir
def validate_file_stripes(file_meta) do
  total_data_bytes = file_meta.stripes
    |> Enum.map(fn %{stripe_id: id} -> Metadata.get_stripe(id).data_bytes end)
    |> Enum.sum()

  if total_data_bytes != file_meta.size do
    {:error, :stripe_size_mismatch,
      "Stripe data_bytes (#{total_data_bytes}) != file size (#{file_meta.size})"}
  else
    :ok
  end
end
```

### Edge Cases

| Scenario | Handling |
|----------|----------|
| Read spans full and partial stripes | Each stripe read respects its own `data_bytes` |
| Read entirely within padding region | Returns empty binary |
| Degraded partial stripe | Reconstruct full stripe data, truncate to `data_bytes` |
| Read beyond file size | Truncated to `file_meta.size` at top level |
| Stripe with 0 available chunks | Returns `{:error, :insufficient_chunks}` |

## Partial Stripe Handling

Erasure coding requires a minimum amount of data to form a stripe (e.g., 10 data chunks × 256KB = 2.5MB for a 10+4 config). Files smaller than this, or the tail end of larger files, need special handling.

**Strategy: Pad and Mark as Partial**

All data on erasure-coded volumes uses erasure coding, even for small files. Partial stripes are padded with zeros to form complete stripes:

| Scenario | Handling |
|----------|----------|
| File < stripe capacity | Pad to fill stripe, mark as partial |
| Full stripes in large file | Erasure code normally |
| Partial final stripe | Pad to fill stripe, mark as partial |

**Why padding instead of fallback replication:**

- **Consistent durability**: Users who choose "10+4 erasure coding" get 4-failure tolerance for all data, not just large files
- **No semantic surprises**: Small files have the same fault tolerance as large files
- **Future-proof format**: Partial stripes can later be compacted (packing multiple small files together) without changing the on-disk format

**Tradeoff**: A 50KB file on a 10+4 volume wastes ~2.5MB in padding. This is the price of consistent semantics. For workloads with many small files where storage efficiency matters more than consistent durability, consider using replication instead of erasure coding.

**Stripe metadata:**

```elixir
%Stripe{
  id: stripe_id,
  config: %{data_chunks: 10, parity_chunks: 4},
  chunks: [...],  # All 14 chunk hashes

  # Partial stripe tracking
  partial: true,
  data_bytes: 52_428,        # Actual data (before padding)
  padded_bytes: 2_568_532,   # Zero-fill padding

  state: :healthy | :degraded | :critical
}
```

**Example: 50KB file on 10+4 volume (256KB chunks)**

```
File: 50KB total
Stripe capacity: 10 × 256KB = 2.56MB

Stripe 1:
  - Chunk 0: 50KB data + 206KB padding (256KB total)
  - Chunks 1-9: 256KB padding each
  - Chunks 10-13: parity (computed including padding)
  - Marked as: partial, data_bytes: 51200
```

**Future optimisation: Compaction**

A future compaction process could pack multiple partial stripes together, reclaiming wasted space:

1. Identify partial stripes on the same volume
2. Combine their data into fewer, fuller stripes
3. Update file metadata to point to new stripes
4. Delete old partial stripes

This is deferred to post-initial implementation. The current padding approach ensures the on-disk format already supports compaction without migration.

## Garbage Collection

GC handles **committed chunks** that are no longer referenced by any file AND have no active writes using them. Uncommitted chunks are handled separately via reference counting (see above).

**Approach: Mark and Sweep with Active Write Protection**

```
1. Mark phase: Walk all committed file metadata, collect referenced chunk hashes
2. Sweep phase: Any committed chunk not in the referenced set AND with empty
   active_write_refs is garbage
3. Delete phase: Remove garbage chunks immediately (no grace period needed)
```

**Why no grace period?**

The `active_write_refs` mechanism provides precise protection:
- Any write that reuses a committed chunk adds itself to `active_write_refs` before using the chunk
- GC checks `active_write_refs` is empty before deleting
- This eliminates the race condition between GC and in-flight writes without needing a grace period

**Why not reference counting for committed chunks?**

Distributed reference counting (including weighted variants) has failure modes where lost messages can prevent chunks from ever being collected, or worse, cause premature deletion. The coordination required to handle these failures correctly negates the performance benefit.

The `active_write_refs` approach is simpler: it only tracks in-flight writes (a small, bounded set) rather than all references. File-to-chunk references are handled by mark-and-sweep.

**Implementation:**

```elixir
defmodule NeonFS.GarbageCollector do
  def collect(volume) do
    # Phase 1: Build set of all referenced chunks from committed files
    referenced = volume
      |> stream_all_files()
      |> Stream.flat_map(& &1.chunks)
      |> MapSet.new()

    # Phase 2: Find unreferenced committed chunks with no active writes
    garbage = volume
      |> stream_committed_chunks()
      |> Stream.reject(fn chunk ->
        MapSet.member?(referenced, chunk.hash) or
        MapSet.size(chunk.active_write_refs) > 0
      end)
      |> Enum.to_list()

    # Phase 3: Delete immediately (active_write_refs protects in-flight writes)
    for chunk <- garbage do
      ChunkIndex.delete(chunk.hash)
    end
  end
end
```

**Safety measures:**

- **Active write protection**: Chunks with non-empty `active_write_refs` are never deleted, even if unreferenced by files. This handles the race where a write is about to commit and reference the chunk.
- **Committed chunks only**: GC only considers chunks with `commit_state: :committed`. Uncommitted chunks are managed by write operations via reference counting.

**Scheduling:**

- GC runs during low-activity periods (configurable)
- Can be triggered manually or by storage pressure
- Per-volume scheduling: critical volumes checked more frequently

```yaml
# Global defaults (can be overridden per-volume)
gc:
  schedule: "0 3 * * *"              # Daily at 3 AM
  storage_pressure_threshold: 0.85  # Also run if >85% full
```
