defmodule NeonFS.Core.Job.Runners.VolumeAntiEntropy do
  @moduledoc """
  Per-volume anti-entropy runner (#921). Catches replica divergence
  the read path and `ReplicaRepair` wouldn't trigger on their own
  — silently-diverged metadata between replica drives.

  ## Algorithm (bootstrap-as-truth iteration, per #920's design call)

  The Ra-replicated bootstrap pointer is the source of truth. For
  each volume:

  1. Walk the canonical `chunk_index` in fixed-size batches. Every
     entry's chunk hash is on the canonical-set.
  2. For each chunk, ask every declared `location.node`'s `BlobStore`
     whether the chunk exists locally on that drive (cross-node via
     `:rpc.call/4`). Unreachable peers are skipped with telemetry —
     anti-entropy doesn't block on a single down peer.
  3. Any node listed in `chunk.locations` that reports the chunk as
     missing is a divergence. Accumulate the hash.
  4. At end of pass, enqueue `ReplicaRepair.repair_chunks/2` with the
     accumulated hashes. ReplicaRepair already knows how to re-pull
     and re-record locations.

  ## Why not classical Merkle?

  Classical Merkle anti-entropy is the answer when no replica has
  authority (Cassandra / Dynamo). NeonFS has a Ra-replicated
  bootstrap pointer, so there's a single canonical answer at any
  point in time — replicas are reconciled against the canonical, not
  against each other. See #920 for the full design record.

  ## Scope

  The pass enumerates two canonical sets:

  1. `chunk_index` data chunks — each entry's `locations` is its
     canonical replica set.
  2. Index-tree pages — `file_index`, `chunk_index`, and
     `stripe_index` internal/leaf nodes reachable from the
     volume's segment, walked via
     `MetadataReader.list_referenced_chunks/2` (#955). The
     canonical replica set is the volume's bootstrap
     `drive_locations` (tree pages don't have per-chunk locations
     because they're written by `MetadataWriter` to a single drive
     and the rest of the replica set catches up via this pass).

  Both classes feed `ReplicaRepair.repair_chunks/2` once at end
  of pass.

  ## Params

  - `:volume_id` (required) — volume to reconcile.
  - `:batch_size` (optional, default 100) — entries per `step/1`
    call (across both classes).

  ## Telemetry

  - `[:neonfs, :volume_anti_entropy, :checked]` —
    `%{count}`, `%{volume_id}` — entries examined this batch.
  - `[:neonfs, :volume_anti_entropy, :divergence]` —
    `%{count}`, `%{volume_id, hashes}` — entries needing repair.
  - `[:neonfs, :volume_anti_entropy, :peer_unreachable]` —
    `%{}`, `%{volume_id, peer_node, reason}` — skipped a peer.
  - `[:neonfs, :volume_anti_entropy, :complete]` —
    `%{checked, divergent, repair_added, repair_removed, repair_errors}`,
    `%{volume_id}` — end-of-pass summary.
  """

  @behaviour NeonFS.Core.Job.Runner

  require Logger

  alias NeonFS.Core.{BlobStore, ChunkIndex, ReplicaRepair}
  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.RaSupervisor
  alias NeonFS.Core.Volume.MetadataReader

  @default_batch_size 100
  @rpc_timeout 5_000

  @impl NeonFS.Core.Job.Runner
  def label, do: "volume-anti-entropy"

  @impl NeonFS.Core.Job.Runner
  def step(job) do
    volume_id = Map.fetch!(job.params, :volume_id)
    batch_size = job.params[:batch_size] || @default_batch_size

    state = job.state || %{}
    state = Map.put_new_lazy(state, :hashes, fn -> load_hashes(volume_id) end)
    state = Map.put_new_lazy(state, :tree_locations, fn -> load_tree_locations(volume_id) end)
    state = Map.put_new(state, :cursor, 0)
    state = Map.put_new(state, :checked, 0)
    state = Map.put_new(state, :divergent_hashes, [])

    case Enum.drop(state.hashes, state.cursor) |> Enum.take(batch_size) do
      [] ->
        complete(job, volume_id, state)

      batch ->
        {checked_inc, divergent_inc} = examine_batch(volume_id, batch, state)

        new_state = %{
          state
          | cursor: state.cursor + length(batch),
            checked: state.checked + checked_inc,
            divergent_hashes: state.divergent_hashes ++ divergent_inc
        }

        :telemetry.execute(
          [:neonfs, :volume_anti_entropy, :checked],
          %{count: checked_inc},
          %{volume_id: volume_id}
        )

        progress = %{
          total: length(state.hashes),
          completed: new_state.cursor,
          description: "Anti-entropy: walking chunk_index"
        }

        {:continue, %{job | state: new_state, progress: progress}}
    end
  end

  # ─── Internals ──────────────────────────────────────────────────────

  # `load_hashes/1` returns a tagged list. Data-chunk entries carry
  # only the hash because the canonical locations are looked up
  # fresh per-examine (chunk locations can change mid-pass).
  # Tree-page entries use the volume's bootstrap `drive_locations`
  # snapshot, which is loaded once into state alongside the hash
  # list — tree pages don't have per-entry locations.
  defp load_hashes(volume_id) do
    chunk_entries =
      volume_id
      |> ChunkIndex.get_chunks_for_volume()
      |> Enum.map(&{:chunk, &1.hash})

    chunk_entries ++ Enum.map(load_tree_page_hashes(volume_id), &{:tree_page, &1})
  end

  defp load_tree_page_hashes(volume_id) do
    case MetadataReader.list_referenced_chunks(volume_id) do
      {:ok, hashes} ->
        hashes

      {:error, reason} ->
        Logger.warning(
          "VolumeAntiEntropy: skipping tree-page enumeration for #{volume_id}: " <>
            inspect(reason)
        )

        []
    end
  end

  defp load_tree_locations(volume_id) do
    case RaSupervisor.local_query(&MetadataStateMachine.get_volume_root(&1, volume_id)) do
      {:ok, %{drive_locations: locs}} when is_list(locs) -> locs
      _ -> []
    end
  end

  defp examine_batch(volume_id, hashes, state) do
    Enum.reduce(hashes, {0, []}, fn entry, acc -> examine_one(volume_id, entry, state, acc) end)
  end

  defp examine_one(volume_id, {:chunk, hash}, _state, {checked, divergent}) do
    case ChunkIndex.get(volume_id, hash) do
      {:ok, chunk} -> record_examination(volume_id, chunk, {checked, divergent})
      # Chunk vanished from the index between load and examine
      # (race vs concurrent delete). Skip without counting.
      {:error, _} -> {checked, divergent}
    end
  end

  defp examine_one(volume_id, {:tree_page, hash}, state, {checked, divergent}) do
    # Tree pages diverge against the volume's bootstrap
    # drive_locations rather than a per-chunk locations list.
    if tree_page_divergent?(volume_id, hash, state.tree_locations) do
      {checked + 1, [hash | divergent]}
    else
      {checked + 1, divergent}
    end
  end

  defp tree_page_divergent?(volume_id, hash, locations) do
    Enum.any?(locations, fn location ->
      not chunk_present_on?(volume_id, hash, location)
    end)
  end

  defp record_examination(volume_id, chunk, {checked, divergent}) do
    if chunk_divergent?(volume_id, chunk) do
      {checked + 1, [chunk.hash | divergent]}
    else
      {checked + 1, divergent}
    end
  end

  defp chunk_divergent?(volume_id, chunk) do
    Enum.any?(chunk.locations, fn location ->
      not chunk_present_on?(volume_id, chunk.hash, location)
    end)
  end

  defp chunk_present_on?(_volume_id, hash, %{node: target_node, drive_id: drive_id}) do
    if target_node == node() do
      BlobStore.chunk_exists?(hash, drive_id)
    else
      remote_chunk_exists?(target_node, hash, drive_id)
    end
  end

  defp remote_chunk_exists?(target_node, hash, drive_id) do
    case :rpc.call(target_node, BlobStore, :chunk_exists?, [hash, drive_id], @rpc_timeout) do
      true ->
        true

      false ->
        false

      {:badrpc, reason} ->
        :telemetry.execute(
          [:neonfs, :volume_anti_entropy, :peer_unreachable],
          %{},
          %{peer_node: target_node, reason: reason}
        )

        # Skip-on-unreachable per design — don't fail the pass.
        # Treat as "present" so we don't queue a spurious repair.
        true

      other ->
        Logger.warning(
          "VolumeAntiEntropy: unexpected chunk_exists?/2 reply from " <>
            inspect(target_node) <> ": " <> inspect(other)
        )

        true
    end
  end

  defp complete(job, volume_id, state) do
    divergent_hashes = Enum.uniq(state.divergent_hashes)

    repair_result =
      if divergent_hashes == [] do
        %{added: 0, removed: 0, errors: []}
      else
        :telemetry.execute(
          [:neonfs, :volume_anti_entropy, :divergence],
          %{count: length(divergent_hashes)},
          %{volume_id: volume_id, hashes: divergent_hashes}
        )

        case ReplicaRepair.repair_chunks(volume_id, divergent_hashes) do
          {:ok, result} ->
            result

          {:error, reason} ->
            Logger.warning("VolumeAntiEntropy: ReplicaRepair.repair_chunks failed",
              volume_id: volume_id,
              reason: inspect(reason)
            )

            %{added: 0, removed: 0, errors: [{:repair_call, reason}]}
        end
      end

    :telemetry.execute(
      [:neonfs, :volume_anti_entropy, :complete],
      %{
        checked: state.checked,
        divergent: length(divergent_hashes),
        repair_added: repair_result.added,
        repair_removed: repair_result.removed,
        repair_errors: length(repair_result.errors)
      },
      %{volume_id: volume_id}
    )

    final_state =
      Map.merge(state, %{
        divergent_hashes: divergent_hashes,
        repair_added: repair_result.added,
        repair_removed: repair_result.removed,
        repair_errors: repair_result.errors
      })

    {:complete, %{job | state: final_state, progress: %{job.progress | description: "Complete"}}}
  end
end
