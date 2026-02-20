defmodule NeonFS.Core.Job.Runners.ClusterRebalance do
  @moduledoc """
  Job runner for cluster-wide storage rebalancing.

  Each `step/1` call processes one batch of chunk migrations for a single tier.
  Source and target drives are re-evaluated every step so the algorithm naturally
  adapts as migrations shift usage ratios.

  Chunks stay within their tier — no cross-tier moves.
  """

  @behaviour NeonFS.Core.Job.Runner

  require Logger

  alias NeonFS.Core.Blob.Native
  alias NeonFS.Core.{ChunkIndex, DriveRegistry, TierMigration}

  @impl NeonFS.Core.Job.Runner
  def label, do: "cluster-rebalance"

  @impl NeonFS.Core.Job.Runner
  def step(job) do
    tiers = job.params.tiers
    state = ensure_state(job.state)
    current_index = state.current_tier_index

    if current_index >= length(tiers) do
      complete_rebalance(job, state)
    else
      tier = Enum.at(tiers, current_index)
      process_tier(job, state, tier)
    end
  end

  @impl NeonFS.Core.Job.Runner
  def on_cancel(job) do
    tiers = job.params.tiers
    Logger.info("Rebalance cancelled (tiers: #{inspect(tiers)})")
    :ok
  end

  ## Private — Tier processing

  defp process_tier(job, state, tier) do
    threshold = job.params.threshold
    batch_size = job.params.batch_size
    drives = eligible_drives(tier)

    case select_source_and_target(drives, threshold) do
      :balanced ->
        advance_tier(job, state, tier)

      {:ok, source, target} ->
        migrate_batch(job, state, tier, source, target, batch_size)
    end
  end

  defp select_source_and_target(drives, _threshold) when length(drives) < 2, do: :balanced

  defp select_source_and_target(drives, threshold) do
    avg_ratio = weighted_average(drives)
    above = Enum.filter(drives, fn {_d, _cap, ratio} -> ratio > avg_ratio end)
    below = Enum.filter(drives, fn {_d, _cap, ratio} -> ratio < avg_ratio end)

    with [_ | _] <- above,
         [_ | _] <- below do
      {source, _cap, source_ratio} = Enum.max_by(above, fn {_d, _c, r} -> r end)
      {target, _cap, target_ratio} = Enum.min_by(below, fn {_d, _c, r} -> r end)

      if source_ratio - target_ratio >= threshold do
        {:ok, source, target}
      else
        :balanced
      end
    else
      _ -> :balanced
    end
  end

  defp migrate_batch(job, state, tier, source, target, batch_size) do
    chunks = ChunkIndex.list_by_drive(source.node, source.id)
    batch = Enum.take(chunks, batch_size)

    if batch == [] do
      advance_tier(job, state, tier)
    else
      do_migrate_batch(job, state, tier, source, target, batch)
    end
  end

  defp do_migrate_batch(job, state, tier, source, target, batch) do
    results = Enum.map(batch, &migrate_chunk(&1, source, target))
    successes = Enum.count(results, &match?(:ok, &1))
    failures = Enum.count(results, &match?({:error, _}, &1))
    bytes_moved_estimate = successes * avg_chunk_size(batch)

    new_state = %{
      state
      | bytes_moved: state.bytes_moved + bytes_moved_estimate,
        chunks_migrated: state.chunks_migrated + successes,
        last_batch_at: DateTime.utc_now()
    }

    completed = job.progress.completed + successes
    total = max(job.progress.total, completed)

    if failures > 0 do
      Logger.warning("Rebalance #{tier}: #{failures} chunk(s) failed in batch, will retry")
    end

    updated = %{
      job
      | progress: %{
          total: total,
          completed: completed,
          description: "Rebalancing #{tier} (#{completed}/#{total} chunks)"
        },
        state: state_to_map(new_state)
    }

    :telemetry.execute(
      [:neonfs, :rebalance, :progress],
      %{moved: completed, total: total, batch_failures: failures},
      %{tier: tier}
    )

    {:continue, updated}
  end

  defp migrate_chunk(chunk, source, target) do
    source_tier = extract_source_tier(chunk, source)

    migration_params = %{
      chunk_hash: chunk.hash,
      source_drive: source.id,
      source_node: source.node,
      source_tier: source_tier,
      target_drive: target.id,
      target_node: target.node,
      target_tier: target.tier
    }

    case TierMigration.run_migration(migration_params) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  ## Private — State management

  defp ensure_state(state) when is_map(state) do
    %{
      current_tier_index: Map.get(state, :current_tier_index, 0),
      bytes_moved: Map.get(state, :bytes_moved, 0),
      chunks_migrated: Map.get(state, :chunks_migrated, 0),
      tiers_completed: Map.get(state, :tiers_completed, []),
      last_batch_at: Map.get(state, :last_batch_at)
    }
  end

  defp state_to_map(state) do
    %{
      current_tier_index: state.current_tier_index,
      bytes_moved: state.bytes_moved,
      chunks_migrated: state.chunks_migrated,
      tiers_completed: state.tiers_completed,
      last_batch_at: state.last_batch_at
    }
  end

  ## Private — Tier advancement and completion

  defp advance_tier(job, state, tier) do
    new_state = %{
      state
      | current_tier_index: state.current_tier_index + 1,
        tiers_completed: [tier | state.tiers_completed]
    }

    updated = %{job | state: state_to_map(new_state)}

    {:continue, updated}
  end

  defp complete_rebalance(job, state) do
    :telemetry.execute(
      [:neonfs, :rebalance, :completed],
      %{},
      %{
        tiers: state.tiers_completed,
        bytes_moved: state.bytes_moved,
        chunks_migrated: state.chunks_migrated
      }
    )

    Logger.info(
      "Rebalance completed: #{state.chunks_migrated} chunks moved " <>
        "(#{format_bytes(state.bytes_moved)}), tiers: #{inspect(state.tiers_completed)}"
    )

    updated = %{
      job
      | progress: %{
          job.progress
          | description:
              "Complete — #{state.chunks_migrated} chunks moved across #{length(state.tiers_completed)} tier(s)"
        },
        state: state_to_map(state)
    }

    {:complete, updated}
  end

  ## Private — Drive helpers

  defp eligible_drives(tier) do
    DriveRegistry.list_drives()
    |> Enum.filter(&(&1.tier == tier and &1.state in [:active, :standby]))
    |> Enum.reduce([], fn drive, acc ->
      case effective_capacity(drive) do
        nil ->
          acc

        cap when cap > 0 ->
          ratio = drive.used_bytes / cap
          [{drive, cap, ratio} | acc]

        _ ->
          acc
      end
    end)
    |> Enum.reverse()
  end

  defp effective_capacity(%{capacity_bytes: cap}) when cap > 0, do: cap

  defp effective_capacity(%{path: path}) do
    case Native.filesystem_info(path) do
      {:ok, {total_bytes, _available, _used}} when total_bytes > 0 -> total_bytes
      _ -> nil
    end
  end

  defp weighted_average(drives) do
    {total_used, total_cap} =
      Enum.reduce(drives, {0, 0}, fn {drive, cap, _ratio}, {used_acc, cap_acc} ->
        {used_acc + drive.used_bytes, cap_acc + cap}
      end)

    if total_cap > 0, do: total_used / total_cap, else: 0.0
  end

  defp extract_source_tier(chunk, source_drive) do
    location =
      Enum.find(chunk.locations, fn loc ->
        loc.node == source_drive.node and loc.drive_id == source_drive.id
      end)

    if location, do: location.tier, else: source_drive.tier
  end

  defp avg_chunk_size([]), do: 65_536

  defp avg_chunk_size(batch) do
    total = Enum.reduce(batch, 0, fn chunk, acc -> acc + chunk.stored_size end)
    max(1, div(total, length(batch)))
  end

  defp format_bytes(bytes) when bytes >= 1_073_741_824 do
    "#{Float.round(bytes / 1_073_741_824, 2)} GiB"
  end

  defp format_bytes(bytes) when bytes >= 1_048_576 do
    "#{Float.round(bytes / 1_048_576, 2)} MiB"
  end

  defp format_bytes(bytes) when bytes >= 1024 do
    "#{Float.round(bytes / 1024, 2)} KiB"
  end

  defp format_bytes(bytes), do: "#{bytes} B"
end
