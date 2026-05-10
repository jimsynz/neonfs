defmodule NeonFS.Core.Job.Runners.DriveEvacuation do
  @moduledoc """
  Job runner for drive evacuation.

  Processes chunks in batches, either pruning over-replicated copies or migrating
  under-replicated ones. Each `step/1` call processes one batch of chunks.

  The `list_by_drive/2` query naturally skips already-evacuated chunks (they no
  longer have a location on the evacuating drive), making resume after restart
  idempotent.
  """

  @behaviour NeonFS.Core.Job.Runner

  require Logger

  alias NeonFS.Core.{
    BlobStore,
    ChunkIndex,
    ChunkMeta,
    Drive,
    DriveManager,
    DriveRegistry,
    TierMigration,
    VolumeRegistry
  }

  alias NeonFS.Core.DriveEvacuation

  @default_batch_size 100
  @stale_batch_threshold 3

  @impl NeonFS.Core.Job.Runner
  def label, do: "drive-evacuation"

  @impl NeonFS.Core.Job.Runner
  def step(job) do
    node = job.params.node
    drive_id = job.params.drive_id

    remaining = list_remaining_chunks(node, drive_id)

    case remaining do
      [] ->
        complete_evacuation(job)

      chunks ->
        process_batch(job, chunks)
    end
  end

  @impl NeonFS.Core.Job.Runner
  def on_cancel(job) do
    node = job.params.node
    drive_id = job.params.drive_id
    DriveEvacuation.restore_active(node, drive_id)

    Logger.info("Evacuation cancelled, restored to active", drive_id: drive_id)
  end

  ## Private — Batch processing

  defp process_batch(job, remaining) do
    batch = Enum.take(remaining, batch_size())
    any_tier = job.params.any_tier

    results =
      Enum.map(batch, fn chunk ->
        process_chunk(job.params, chunk, any_tier)
      end)

    successes = Enum.count(results, &match?(:ok, &1))
    failures = Enum.count(results, &match?({:error, _}, &1))
    last_error = find_last_error(results)

    completed = job.progress.completed + successes
    total = max(job.progress.total, completed + length(remaining) - successes)
    stale_batches = update_stale_count(job.state, successes, failures)

    if failures > 0 do
      Logger.warning("Evacuation batch had failures, will retry",
        drive_id: job.params.drive_id,
        failure_count: failures,
        last_error: inspect(last_error)
      )
    end

    updated = %{
      job
      | progress: %{
          total: total,
          completed: completed,
          description: build_description(completed, total, last_error)
        },
        state:
          job.state
          |> Map.put(:last_batch_at, DateTime.utc_now())
          |> Map.put(:stale_batches, stale_batches)
          |> Map.put(:last_error, last_error)
    }

    :telemetry.execute(
      [:neonfs, :evacuation, :progress],
      %{evacuated: completed, total: total, batch_failures: failures},
      %{
        drive_id: job.params.drive_id,
        node: job.params.node,
        stale_batches: stale_batches
      }
    )

    if stale_batches >= @stale_batch_threshold do
      Logger.warning(
        "Evacuation made no progress for #{stale_batches} consecutive batches, failing job",
        drive_id: job.params.drive_id,
        last_error: inspect(last_error)
      )

      {:error, {:no_progress, last_error}, updated}
    else
      {:continue, updated}
    end
  end

  defp update_stale_count(state, 0 = _successes, failures) when failures > 0 do
    Map.get(state, :stale_batches, 0) + 1
  end

  defp update_stale_count(_state, _successes, _failures), do: 0

  defp find_last_error(results) do
    results
    |> Enum.reverse()
    |> Enum.find_value(fn
      {:error, reason} -> reason
      _ -> nil
    end)
  end

  defp build_description(completed, total, nil),
    do: "Evacuating chunks (#{completed}/#{total})"

  defp build_description(completed, total, last_error) do
    "Evacuating chunks (#{completed}/#{total}) — last error: #{normalise_evac_reason(last_error)}"
  end

  @doc false
  @spec normalise_evac_reason(term()) :: String.t()
  def normalise_evac_reason({:migration_failed, reason, target_drive}),
    do: "#{normalise_evac_reason(reason)} on #{target_drive}"

  def normalise_evac_reason(:no_target_drives), do: "no eligible target drives"
  def normalise_evac_reason(:chunk_not_found), do: "chunk not found"
  def normalise_evac_reason(:no_progress), do: "no progress"
  def normalise_evac_reason({:no_progress, inner}), do: normalise_evac_reason(inner)
  def normalise_evac_reason({:rpc_error, _}), do: "rpc error"
  def normalise_evac_reason({:verification_failed, _}), do: "chunk verification failed"
  def normalise_evac_reason({:write_failed, posix}), do: "write failed: #{describe_posix(posix)}"
  def normalise_evac_reason({:read_failed, posix}), do: "read failed: #{describe_posix(posix)}"
  def normalise_evac_reason(:eacces), do: "permission denied"
  def normalise_evac_reason(:enospc), do: "no space on target drive"
  def normalise_evac_reason(:erofs), do: "target drive is read-only"
  def normalise_evac_reason(:enoent), do: "file not found"
  def normalise_evac_reason(nil), do: "unknown error"
  def normalise_evac_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  def normalise_evac_reason(reason), do: inspect(reason)

  defp describe_posix(:eacces), do: "permission denied"
  defp describe_posix(:enospc), do: "no space"
  defp describe_posix(:erofs), do: "read-only filesystem"
  defp describe_posix(:enoent), do: "file not found"
  defp describe_posix(p) when is_atom(p), do: Atom.to_string(p)
  defp describe_posix(p), do: inspect(p)

  defp process_chunk(params, chunk, any_tier) do
    node = params.node
    drive_id = params.drive_id

    target_replicas = target_replicas_for_chunk(chunk)
    other_locations = Enum.reject(chunk.locations, &(&1.node == node and &1.drive_id == drive_id))

    cond do
      # Erasure-coded chunks always need migration
      chunk.stripe_id != nil ->
        migrate_chunk(params, chunk, any_tier)

      # Over-replicated: just delete the copy on the evacuating drive
      length(other_locations) >= target_replicas ->
        delete_chunk_copy(params, chunk)

      # Under-replicated: migrate to another drive
      true ->
        migrate_chunk(params, chunk, any_tier)
    end
  rescue
    error ->
      Logger.warning("Evacuation error processing chunk",
        chunk_hash: Base.encode16(chunk.hash, case: :lower),
        error: inspect(error)
      )

      {:error, error}
  end

  defp delete_chunk_copy(params, chunk) do
    node = params.node
    drive_id = params.drive_id

    # Remove the location from metadata first
    updated_locations =
      Enum.reject(chunk.locations, fn loc ->
        loc.node == node and loc.drive_id == drive_id
      end)

    ChunkIndex.update_locations(chunk.hash, updated_locations)

    delete_opts = BlobStore.codec_opts_for_chunk(chunk)

    delete_result =
      if node == Node.self() do
        BlobStore.delete_chunk(chunk.hash, drive_id, delete_opts)
      else
        :rpc.call(node, BlobStore, :delete_chunk, [chunk.hash, drive_id, delete_opts], 30_000)
        |> handle_rpc_result()
      end

    case delete_result do
      {:ok, _bytes_freed} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to delete chunk copy",
          drive_id: drive_id,
          reason: inspect(reason)
        )

        :ok
    end
  end

  defp migrate_chunk(params, chunk, any_tier) do
    node = params.node
    drive_id = params.drive_id

    location =
      Enum.find(chunk.locations, fn loc ->
        loc.node == node and loc.drive_id == drive_id
      end)

    source_tier = if location, do: location.tier, else: :hot

    case select_target_drive(params, source_tier, any_tier) do
      {:ok, target} ->
        migration_params = %{
          chunk_hash: chunk.hash,
          source_drive: drive_id,
          source_node: node,
          source_tier: source_tier,
          target_drive: target.id,
          target_node: target.node,
          target_tier: target.tier
        }

        case TierMigration.run_migration(migration_params) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, {:migration_failed, reason, target.id}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Private — Target drive selection

  defp select_target_drive(params, source_tier, any_tier) do
    node = params.node
    drive_id = params.drive_id

    if any_tier do
      select_any_tier_drive(node, drive_id, source_tier)
    else
      select_same_tier_drive(node, drive_id, source_tier)
    end
  end

  defp select_same_tier_drive(evac_node, evac_drive_id, tier) do
    # Try local node first
    case DriveRegistry.select_drive(tier) do
      {:ok, drive} when not (drive.node == evac_node and drive.id == evac_drive_id) ->
        {:ok, drive}

      _ ->
        # Fall back to cluster-wide search
        select_from_cluster(evac_node, evac_drive_id, fn d -> d.tier == tier end)
    end
  end

  defp select_any_tier_drive(evac_node, evac_drive_id, preferred_tier) do
    all_drives =
      DriveRegistry.list_drives()
      |> Enum.filter(fn d ->
        d.state not in [:draining] and
          not (d.node == evac_node and d.id == evac_drive_id)
      end)

    # Prefer same tier, then sort by usage ratio
    case Enum.filter(all_drives, &(&1.tier == preferred_tier)) do
      [_ | _] = same_tier ->
        {:ok, Enum.min_by(same_tier, &Drive.usage_ratio/1)}

      [] ->
        case all_drives do
          [_ | _] -> {:ok, Enum.min_by(all_drives, &Drive.usage_ratio/1)}
          [] -> {:error, :no_target_drives}
        end
    end
  end

  defp select_from_cluster(evac_node, evac_drive_id, filter_fn) do
    candidates =
      DriveRegistry.list_drives()
      |> Enum.filter(fn d ->
        filter_fn.(d) and d.state not in [:draining] and
          not (d.node == evac_node and d.id == evac_drive_id)
      end)

    case candidates do
      [] -> {:error, :no_target_drives}
      drives -> {:ok, Enum.min_by(drives, &Drive.usage_ratio/1)}
    end
  end

  ## Private — Completion

  defp complete_evacuation(job) do
    node = job.params.node
    drive_id = job.params.drive_id

    # Verify drive is truly empty
    has_data =
      if node == Node.self() do
        BlobStore.drive_has_data?(drive_id)
      else
        case :rpc.call(node, BlobStore, :drive_has_data?, [drive_id], 10_000) do
          {:ok, result} -> result
          {:badrpc, _} -> true
        end
      end

    case has_data do
      {:ok, false} ->
        deregister_drive(node, drive_id)

        :telemetry.execute(
          [:neonfs, :evacuation, :completed],
          %{},
          %{drive_id: drive_id, node: node}
        )

        Logger.info("Evacuation completed, drive deregistered",
          drive_id: drive_id,
          node: node
        )

        {:complete,
         %{job | progress: %{job.progress | description: "Complete — drive deregistered"}}}

      {:ok, true} ->
        Logger.warning("Evacuation finished but drive still has data, leaving as draining",
          drive_id: drive_id
        )

        {:complete, %{job | progress: %{job.progress | description: "Complete — data remains"}}}

      _ ->
        # Treat unexpected results defensively
        Logger.warning("Could not verify drive is empty, leaving as draining",
          drive_id: drive_id
        )

        {:complete, %{job | progress: %{job.progress | description: "Complete — unverified"}}}
    end
  end

  defp deregister_drive(node, drive_id) do
    if node == Node.self() do
      DriveManager.remove_drive(drive_id, force: true)
    else
      :rpc.call(node, DriveManager, :remove_drive, [drive_id, [force: true]], 30_000)
    end
  rescue
    error ->
      Logger.warning("Failed to deregister drive",
        drive_id: drive_id,
        error: inspect(error)
      )
  end

  ## Private — Helpers

  defp list_remaining_chunks(node, drive_id) do
    if node == Node.self() do
      ChunkIndex.list_by_drive(node, drive_id)
    else
      case :rpc.call(node, ChunkIndex, :list_by_drive, [node, drive_id], 30_000) do
        result when is_list(result) ->
          result

        {:badrpc, reason} ->
          Logger.warning("Failed to list chunks on node", node: node, reason: inspect(reason))
          []
      end
    end
  end

  defp target_replicas_for_chunk(chunk) do
    case ChunkMeta.any_volume_id(chunk) do
      nil ->
        3

      volume_id ->
        case VolumeRegistry.get(volume_id) do
          {:ok, volume} -> volume.durability.factor
          _ -> 3
        end
    end
  end

  defp handle_rpc_result({:badrpc, reason}), do: {:error, {:rpc_error, reason}}
  defp handle_rpc_result(result), do: result

  defp batch_size do
    Application.get_env(:neonfs_core, :evacuation_batch_size, @default_batch_size)
  end
end
