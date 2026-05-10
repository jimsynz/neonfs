defmodule NeonFS.Core.ReplicaRepair do
  @moduledoc """
  Walks `ChunkIndex` for one volume and reconciles each chunk's
  replica count to its target. Under-replicated chunks get fresh
  replicas via `Replication.replicate_chunk/4`; over-replicated
  chunks have the excess deleted via `BlobStore.delete_chunk/3`.
  Exactly-replicated chunks are skipped.

  This is the data-plane primitive for the replica-repair worker
  (issue #687). The Job-runner integration, periodic scheduler,
  membership-change auto-trigger, CLI surface, and peer-cluster
  integration test all land in dependent sub-issues
  (#707 / #708 / #709 / #710).

  ## Resumability

  `repair_volume/2` accepts a `:cursor` option (an integer offset
  into the volume's chunk list) so the runner from #707 can drive
  the walk in batched cycles without monopolising the
  BackgroundWorker pool. The returned `:next_cursor` is either a
  resumable offset or `:done`.

  ## Errors

  Per-chunk failures are collected into the result's `:errors`
  list rather than aborting the pass — one unreachable replica
  shouldn't stall the entire repair. `{:error, _}` is reserved for
  pass-level failures (volume lookup, cursor out of range).

  ## Telemetry

    * `[:neonfs, :replica_repair, :chunk_repaired]` —
      Metadata: `chunk_hash`, `direction` (`:added` | `:removed`),
      `from_replicas`, `to_replicas`.
    * `[:neonfs, :replica_repair, :error]` —
      Metadata: `chunk_hash`, `reason`.
  """

  require Logger

  alias NeonFS.Core.{BlobStore, ChunkIndex, Replication, VolumeRegistry}

  @default_batch_size 100
  @rpc_timeout 30_000

  @type repair_result :: %{
          added: non_neg_integer(),
          removed: non_neg_integer(),
          errors: [{binary(), term()}],
          next_cursor: non_neg_integer() | :done
        }

  @doc """
  Walks `volume_id`'s chunks, reconciling each to its
  `target_replicas`. Returns the count of replicas added / removed
  in this pass plus any per-chunk errors.

  ## Options

    * `:batch_size` (default 100) — chunks per cycle.
    * `:cursor` (default 0) — opaque resume offset.
  """
  @spec repair_volume(binary(), keyword()) :: {:ok, repair_result()} | {:error, term()}
  def repair_volume(volume_id, opts \\ []) when is_binary(volume_id) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    cursor = Keyword.get(opts, :cursor, 0)

    with {:ok, volume} <- VolumeRegistry.get(volume_id) do
      chunks = ChunkIndex.get_chunks_for_volume(volume_id)
      total = length(chunks)
      batch = chunks |> Enum.drop(cursor) |> Enum.take(batch_size)
      next_cursor = if cursor + batch_size >= total, do: :done, else: cursor + batch_size

      result =
        batch
        |> Enum.reduce(%{added: 0, removed: 0, errors: []}, fn chunk, acc ->
          reconcile_chunk(chunk, volume, acc)
        end)
        |> Map.put(:next_cursor, next_cursor)

      {:ok, result}
    end
  end

  @doc """
  Reconcile a specific list of `chunk_hashes` for `volume_id` — same
  per-chunk logic as `repair_volume/2`, but driven by an explicit set
  rather than a full-volume scan. Anti-entropy uses this so one
  detected divergence doesn't trigger a volume-wide rebalance.

  Chunks that aren't in `ChunkIndex` for `volume_id` are collected
  into `:errors` as `{hash, :not_found}` rather than aborting — a
  missing-from-index chunk is itself a useful signal anti-entropy
  surfaces to telemetry.

  Returns the same `{added, removed, errors}` shape; `:next_cursor`
  is always `:done` because this is a one-shot pass over the supplied
  list (no resumption — the caller decides the batch size).
  """
  @spec repair_chunks(binary(), [binary()]) :: {:ok, repair_result()} | {:error, term()}
  def repair_chunks(volume_id, chunk_hashes)
      when is_binary(volume_id) and is_list(chunk_hashes) do
    with {:ok, volume} <- VolumeRegistry.get(volume_id) do
      result =
        chunk_hashes
        |> Enum.reduce(
          %{added: 0, removed: 0, errors: []},
          &reduce_chunk(volume_id, volume, &1, &2)
        )
        |> Map.put(:next_cursor, :done)

      {:ok, result}
    end
  end

  defp reduce_chunk(volume_id, volume, hash, acc) do
    case ChunkIndex.get(volume_id, hash) do
      {:ok, chunk} -> reconcile_chunk(chunk, volume, acc)
      {:error, reason} -> record_lookup_error(hash, reason, acc)
    end
  end

  defp record_lookup_error(hash, reason, acc) do
    emit_error(hash, reason)
    %{acc | errors: [{hash, reason} | acc.errors]}
  end

  defp reconcile_chunk(%{target_replicas: target} = chunk, volume, acc) do
    current = length(chunk.locations)

    cond do
      current == target -> acc
      current < target -> add_replicas(chunk, volume, target - current, acc)
      current > target -> drop_replicas(chunk, current - target, acc)
    end
  end

  defp add_replicas(chunk, volume, count, acc) do
    case source_chunk_data(chunk) do
      {:ok, data} ->
        existing_nodes = Enum.map(chunk.locations, & &1.node)

        case Replication.replicate_chunk(chunk.hash, data, volume, exclude_nodes: existing_nodes) do
          {:ok, replicated_locations} ->
            combined = Enum.uniq(chunk.locations ++ replicated_locations)
            ChunkIndex.update_locations(chunk.hash, combined)
            added = length(combined) - length(chunk.locations)
            emit_repaired(chunk.hash, :added, length(chunk.locations), length(combined))
            %{acc | added: acc.added + added}

          {:error, reason} ->
            emit_error(chunk.hash, reason)
            %{acc | errors: [{chunk.hash, reason} | acc.errors]}
        end

      {:error, reason} ->
        emit_error(chunk.hash, reason)
        %{acc | errors: [{chunk.hash, reason} | acc.errors]}
    end
    |> with_count_unused(count)
  end

  # Suppress unused-variable warnings while leaving `count` readable in
  # the caller; the actual added count derives from the locations diff.
  defp with_count_unused(acc, _count), do: acc

  defp drop_replicas(chunk, count, acc) do
    excess = pick_excess_replicas(chunk.locations, count)
    keeper_locations = chunk.locations -- excess

    Enum.reduce(excess, {acc, keeper_locations}, fn location, {inner_acc, kept} ->
      case rpc_delete_chunk(location.node, chunk.hash, location.drive_id) do
        {:ok, _bytes_freed} ->
          new_kept = kept
          ChunkIndex.update_locations(chunk.hash, new_kept)
          emit_repaired(chunk.hash, :removed, length(chunk.locations), length(new_kept))
          {%{inner_acc | removed: inner_acc.removed + 1}, new_kept}

        {:error, reason} ->
          emit_error(chunk.hash, reason)
          {%{inner_acc | errors: [{chunk.hash, reason} | inner_acc.errors]}, kept}
      end
    end)
    |> elem(0)
  end

  # Currently picks the first `count` locations. A future revision
  # can prefer drives in `:draining` / `:failed` state, then
  # least-recently-accessed — `select_replication_targets/2` already
  # has the hooks for the inverse direction. Out of scope per
  # issue #687.
  defp pick_excess_replicas(locations, count), do: Enum.take(locations, count)

  defp source_chunk_data(chunk) do
    case Enum.find(chunk.locations, &(&1.node == Node.self())) do
      nil -> remote_read(chunk)
      local -> BlobStore.read_chunk(chunk.hash, local.drive_id)
    end
  end

  defp remote_read(%{locations: []}), do: {:error, :no_replicas}

  defp remote_read(%{locations: [first | _]} = chunk) do
    rpc_read_chunk(first.node, chunk.hash, first.drive_id)
  end

  defp rpc_read_chunk(node, hash, drive_id) do
    if node == Node.self() do
      BlobStore.read_chunk(hash, drive_id)
    else
      case :rpc.call(node, BlobStore, :read_chunk, [hash, drive_id, []], @rpc_timeout) do
        {:ok, _data} = ok -> ok
        {:error, _reason} = err -> err
        {:badrpc, reason} -> {:error, {:badrpc, reason}}
      end
    end
  end

  defp rpc_delete_chunk(node, hash, drive_id) do
    if node == Node.self() do
      BlobStore.delete_chunk(hash, drive_id, [])
    else
      case :rpc.call(node, BlobStore, :delete_chunk, [hash, drive_id, []], @rpc_timeout) do
        {:ok, _bytes} = ok -> ok
        {:error, _reason} = err -> err
        {:badrpc, reason} -> {:error, {:badrpc, reason}}
      end
    end
  end

  defp emit_repaired(hash, direction, from, to) do
    :telemetry.execute(
      [:neonfs, :replica_repair, :chunk_repaired],
      %{from_replicas: from, to_replicas: to},
      %{chunk_hash: hash, direction: direction, from_replicas: from, to_replicas: to}
    )
  end

  defp emit_error(hash, reason) do
    :telemetry.execute(
      [:neonfs, :replica_repair, :error],
      %{},
      %{chunk_hash: hash, reason: reason}
    )
  end
end
