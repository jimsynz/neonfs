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
    Drive,
    DriveManager,
    DriveRegistry,
    FileIndex,
    TierMigration,
    VolumeRegistry
  }

  alias NeonFS.Core.DriveEvacuation

  @default_batch_size 100

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

    Logger.info("Evacuation cancelled for drive #{drive_id}, restored to :active")
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

    completed = job.progress.completed + successes
    total = max(job.progress.total, completed + length(remaining) - successes)

    if failures > 0 do
      Logger.warning(
        "Evacuation #{job.params.drive_id}: #{failures} chunk(s) failed in batch, will retry"
      )
    end

    updated = %{
      job
      | progress: %{
          total: total,
          completed: completed,
          description: "Evacuating chunks (#{completed}/#{total})"
        },
        state: Map.put(job.state, :last_batch_at, DateTime.utc_now())
    }

    :telemetry.execute(
      [:neonfs, :evacuation, :progress],
      %{evacuated: completed, total: total, batch_failures: failures},
      %{drive_id: job.params.drive_id, node: job.params.node}
    )

    {:continue, updated}
  end

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
      Logger.warning(
        "Evacuation: error processing chunk #{Base.encode16(chunk.hash, case: :lower)}: " <>
          "#{inspect(error)}"
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

    # Delete the physical data
    delete_result =
      if node == Node.self() do
        BlobStore.delete_chunk(chunk.hash, drive_id)
      else
        :rpc.call(node, BlobStore, :delete_chunk, [chunk.hash, drive_id], 30_000)
        |> handle_rpc_result()
      end

    case delete_result do
      {:ok, _bytes_freed} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to delete chunk copy on #{drive_id}: #{inspect(reason)}")
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
          {:error, reason} -> {:error, reason}
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

        Logger.info("Evacuation completed for drive #{drive_id} on #{node}, drive deregistered")

        {:complete,
         %{job | progress: %{job.progress | description: "Complete — drive deregistered"}}}

      {:ok, true} ->
        Logger.warning(
          "Evacuation finished but drive #{drive_id} still has data, leaving as :draining"
        )

        {:complete, %{job | progress: %{job.progress | description: "Complete — data remains"}}}

      _ ->
        # Treat unexpected results defensively
        Logger.warning("Could not verify drive #{drive_id} is empty, leaving as :draining")
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
      Logger.warning("Failed to deregister drive #{drive_id}: #{inspect(error)}")
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
          Logger.warning("Failed to list chunks on #{node}: #{inspect(reason)}")
          []
      end
    end
  end

  defp target_replicas_for_chunk(chunk) do
    volume_id = get_volume_for_chunk(chunk)

    case volume_id do
      nil ->
        3

      vid ->
        case VolumeRegistry.get(vid) do
          {:ok, volume} -> volume.durability.factor
          _ -> 3
        end
    end
  end

  defp get_volume_for_chunk(chunk) do
    FileIndex.list_all()
    |> Enum.find_value(fn file ->
      if chunk.hash in file.chunks, do: file.volume_id
    end)
  rescue
    _ -> nil
  end

  defp handle_rpc_result({:badrpc, reason}), do: {:error, {:rpc_error, reason}}
  defp handle_rpc_result(result), do: result

  defp batch_size do
    Application.get_env(:neonfs_core, :evacuation_batch_size, @default_batch_size)
  end
end
