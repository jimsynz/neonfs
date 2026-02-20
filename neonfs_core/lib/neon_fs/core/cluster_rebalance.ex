defmodule NeonFS.Core.ClusterRebalance do
  @moduledoc """
  Orchestrates cluster-wide storage rebalancing across drives within each tier.

  After adding drives, evacuating drives, or uneven write patterns, storage
  usage across drives within a tier can become imbalanced. This module moves
  chunks from overfull drives to underfull drives until usage ratios converge
  within a configurable tolerance.

  ## Usage

      {:ok, job} = ClusterRebalance.start_rebalance(threshold: 0.10)
      {:ok, status} = ClusterRebalance.rebalance_status()
  """

  require Logger

  alias NeonFS.Core.Blob.Native
  alias NeonFS.Core.{DriveRegistry, JobTracker}
  alias NeonFS.Core.Job.Runners.ClusterRebalance, as: RebalanceRunner

  @default_threshold 0.10
  @default_batch_size 50

  @doc """
  Starts a cluster-wide rebalance operation.

  Pre-flight checks:
  1. Determine tiers to rebalance (all or specific)
  2. At least 2 eligible (non-draining) drives per tier
  3. At least one tier has spread above threshold
  4. No existing rebalance job running

  ## Options

    * `:tier` — specific tier to rebalance (default: all tiers)
    * `:threshold` — balance tolerance as a float 0.0–1.0 (default: `0.10`)
    * `:batch_size` — chunks per migration batch (default: `50`)
  """
  @spec start_rebalance(keyword()) :: {:ok, NeonFS.Core.Job.t()} | {:error, term()}
  def start_rebalance(opts \\ []) do
    threshold = Keyword.get(opts, :threshold, @default_threshold)
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    tier_filter = Keyword.get(opts, :tier)

    with {:ok, candidate_tiers} <- discover_tiers(tier_filter),
         {:ok, tiers_with_drives} <- validate_eligible_drives(candidate_tiers),
         {:ok, imbalanced_tiers} <- check_spread(tiers_with_drives, threshold),
         :ok <- check_no_existing_rebalance() do
      total_estimate = estimate_total_chunks(imbalanced_tiers, threshold)
      tier_names = Enum.map(imbalanced_tiers, fn {tier, _drives} -> tier end)

      params = %{
        tiers: tier_names,
        threshold: threshold,
        batch_size: batch_size
      }

      case JobTracker.create(RebalanceRunner, params) do
        {:ok, job} ->
          job = %{
            job
            | progress: %{
                total: total_estimate,
                completed: 0,
                description: "Rebalancing #{length(tier_names)} tier(s)"
              }
          }

          Logger.info(
            "Started cluster rebalance for tiers #{inspect(tier_names)} " <>
              "(threshold: #{threshold}, batch_size: #{batch_size}, " <>
              "estimated chunks: #{total_estimate})"
          )

          {:ok, job}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Returns the status of an active or recent rebalance operation.
  """
  @spec rebalance_status() :: {:ok, map()} | {:error, :no_rebalance}
  def rebalance_status do
    jobs = JobTracker.list(type: RebalanceRunner)

    case Enum.find(jobs, fn job -> job.status in [:pending, :running] end) || List.first(jobs) do
      nil ->
        {:error, :no_rebalance}

      job ->
        {:ok,
         %{
           job_id: job.id,
           status: job.status,
           progress: job.progress,
           tiers: job.params[:tiers] || [],
           threshold: job.params[:threshold],
           state: job.state
         }}
    end
  end

  ## Private

  defp discover_tiers(nil) do
    drives = DriveRegistry.list_drives()
    tiers = drives |> Enum.map(& &1.tier) |> Enum.uniq()

    case tiers do
      [] -> {:error, :no_drives}
      tiers -> {:ok, tiers}
    end
  end

  defp discover_tiers(tier) when is_atom(tier) do
    drives = DriveRegistry.list_drives()

    if Enum.any?(drives, &(&1.tier == tier)) do
      {:ok, [tier]}
    else
      {:error, :tier_not_found}
    end
  end

  defp validate_eligible_drives(tiers) do
    all_drives = DriveRegistry.list_drives()

    results =
      Enum.reduce_while(tiers, {:ok, []}, fn tier, {:ok, acc} ->
        eligible =
          all_drives
          |> Enum.filter(&(&1.tier == tier and &1.state in [:active, :standby]))
          |> resolve_effective_capacities()

        if length(eligible) >= 2 do
          {:cont, {:ok, [{tier, eligible} | acc]}}
        else
          {:cont, {:ok, acc}}
        end
      end)

    case results do
      {:ok, []} -> {:error, :insufficient_drives}
      {:ok, tiers_with_drives} -> {:ok, Enum.reverse(tiers_with_drives)}
    end
  end

  defp check_spread(tiers_with_drives, threshold) do
    imbalanced =
      Enum.filter(tiers_with_drives, fn {_tier, drives} ->
        ratios = Enum.map(drives, &effective_ratio_from_pair/1)

        ratios = Enum.reject(ratios, &is_nil/1)

        case ratios do
          [] -> false
          _ -> Enum.max(ratios) - Enum.min(ratios) >= threshold
        end
      end)

    case imbalanced do
      [] -> {:error, :already_balanced}
      tiers -> {:ok, tiers}
    end
  end

  defp check_no_existing_rebalance do
    jobs = JobTracker.list(type: RebalanceRunner)

    if Enum.any?(jobs, fn job -> job.status in [:pending, :running] end) do
      {:error, :rebalance_already_running}
    else
      :ok
    end
  end

  defp estimate_total_chunks(tiers_with_drives, threshold) do
    Enum.reduce(tiers_with_drives, 0, fn {_tier, drives}, acc ->
      overfull_bytes = sum_overfull_bytes(drives, threshold)

      # Estimate ~64KB average chunk size
      avg_chunk_size = 65_536
      acc + max(1, div(overfull_bytes, avg_chunk_size))
    end)
  end

  defp sum_overfull_bytes(drives, threshold) do
    total_used = Enum.reduce(drives, 0, fn {drive, _cap}, sum -> sum + drive.used_bytes end)
    total_cap = Enum.reduce(drives, 0, fn {_drive, cap}, sum -> sum + cap end)
    avg_ratio = if total_cap > 0, do: total_used / total_cap, else: 0.0

    Enum.reduce(drives, 0, fn {drive, eff_cap}, sum ->
      excess = max(0, drive.used_bytes - trunc(eff_cap * avg_ratio))
      ratio = if eff_cap > 0, do: drive.used_bytes / eff_cap, else: 0.0

      if ratio - avg_ratio >= threshold / 2, do: sum + excess, else: sum
    end)
  end

  defp resolve_effective_capacities(drives) do
    Enum.reduce(drives, [], fn drive, acc ->
      case effective_capacity(drive) do
        nil -> acc
        cap -> [{drive, cap} | acc]
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

  defp effective_ratio_from_pair({drive, eff_cap}) when eff_cap > 0 do
    drive.used_bytes / eff_cap
  end

  defp effective_ratio_from_pair(_), do: nil
end
