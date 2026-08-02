defmodule NeonFS.CLI.Handler.Drives do
  @moduledoc """
  CLI command handlers for storage devices: adding/removing drives,
  listing them cluster-wide, evacuating a drive, cluster rebalancing,
  and aggregate storage-capacity stats.

  Extracted from `NeonFS.CLI.Handler`. `NeonFS.CLI.Handler`
  delegates its `handle_add_drive`/`handle_remove_drive`/
  `handle_list_drives`/`handle_evacuate_drive`/`handle_evacuation_status`/
  `handle_rebalance`/`handle_rebalance_status`/`handle_storage_stats` RPC
  entry points here, so the CLI wire contract is unchanged. Job-returning
  commands render via the shared `NeonFS.CLI.Handler.Common.job_to_map/1`.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.{
    ClusterRebalance,
    DriveEvacuation,
    DriveManager,
    ReplicaAudit,
    StorageMetrics
  }

  alias NeonFS.Error.NotFound

  @doc """
  Adds a drive to the local node.
  """
  @spec handle_add_drive(map()) :: {:ok, map()} | {:error, term()}
  def handle_add_drive(config) when is_map(config) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case DriveManager.add_drive(config) do
        {:ok, drive_map} -> {:ok, drive_map}
        {:error, reason} -> {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Removes a drive from the local node (optionally forced).
  """
  @spec handle_remove_drive(String.t(), boolean()) :: {:ok, map()} | {:error, term()}
  def handle_remove_drive(drive_id, force \\ false) when is_binary(drive_id) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case DriveManager.remove_drive(drive_id, force: force) do
        :ok -> {:ok, %{}}
        {:error, reason} -> {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Lists drives across the cluster, optionally filtered by `"node"`.
  """
  @spec handle_list_drives(map()) :: {:ok, [map()]}
  def handle_list_drives(filters \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      opts =
        case Map.get(filters, "node") do
          nil -> []
          node_name -> [node: String.to_atom(node_name)]
        end

      {:ok, DriveManager.list_all_drives(opts)}
    end
  end

  @doc """
  Starts evacuation of all data from a drive (prefers a same-tier
  target, falls back to any tier).

  `opts` accepts `"force"` to override a below-`min_copies` replica
  finding.
  """
  @spec handle_evacuate_drive(String.t(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def handle_evacuate_drive(node_name, drive_id, opts \\ %{})
      when is_binary(node_name) and is_binary(drive_id) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      node = String.to_atom(node_name)

      case DriveEvacuation.start_evacuation(node, drive_id, force: truthy?(opts, "force")) do
        {:ok, job} -> {:ok, job_to_map(job)}
        {:error, reason} -> {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Returns a `:draining` drive to `:active` so it can serve writes and be
  evacuated again.
  """
  @spec handle_resume_drive(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def handle_resume_drive(node_name, drive_id)
      when is_binary(node_name) and is_binary(drive_id) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      # The two expected refusals get their own wording: `wrap_error/1` would
      # otherwise `inspect/1` the tuple and hand the operator
      # `"{:not_draining, :active}"`, which says what happened but not what to
      # do about it.
      case DriveEvacuation.resume_drive(String.to_atom(node_name), drive_id) do
        {:ok, result} ->
          {:ok, Map.update!(result, :node, &Atom.to_string/1)}

        {:error, {:not_draining, state}} ->
          {:error,
           wrap_error("drive '#{drive_id}' is #{state}, not draining — nothing to resume")}

        {:error, {:evacuation_running, job_id}} ->
          {:error,
           wrap_error(
             "evacuation job #{job_id} is still running for drive '#{drive_id}'; " <>
               "wait for it to finish or cancel it first"
           )}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Reports replication health: which volumes sit below their `min_copies`
  floor, and which drives hold the sole copy of anything.
  """
  @spec handle_replica_status() :: {:ok, map()} | {:error, term()}
  def handle_replica_status do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case ReplicaAudit.audit() do
        {:ok, report} ->
          {:ok,
           %{
             volumes: Enum.map(report.volumes, &serialise_volume_replication/1),
             under_replicated: Enum.map(report.under_replicated, & &1.volume_name),
             sole_copy_drives:
               Enum.map(report.sole_copy_drives, fn drive ->
                 %{
                   node: Atom.to_string(drive.node),
                   drive_id: drive.drive_id,
                   chunk_count: drive.chunk_count
                 }
               end)
           }}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Returns the evacuation status for a drive.
  """
  @spec handle_evacuation_status(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_evacuation_status(drive_id) when is_binary(drive_id) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case DriveEvacuation.evacuation_status(drive_id) do
        {:ok, status} ->
          {:ok,
           %{
             job_id: status.job_id,
             status: Atom.to_string(status.status),
             progress_total: status.progress.total,
             progress_completed: status.progress.completed,
             progress_description: status.progress.description,
             drive_id: status.drive_id,
             node: if(status.node, do: Atom.to_string(status.node))
           }}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Starts a cluster-wide rebalance operation.
  """
  @spec handle_rebalance(map()) :: {:ok, map()} | {:error, term()}
  def handle_rebalance(opts \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      rebalance_opts =
        []
        |> parse_tier_opt(Map.get(opts, "tier"))
        |> parse_float_opt(:threshold, Map.get(opts, "threshold"))
        |> parse_int_opt(:batch_size, Map.get(opts, "batch_size"))

      case ClusterRebalance.start_rebalance(rebalance_opts) do
        {:ok, job} -> {:ok, job_to_map(job)}
        {:error, reason} -> {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Returns the status of an active or recent rebalance operation.
  """
  @spec handle_rebalance_status() :: {:ok, map()} | {:error, term()}
  def handle_rebalance_status do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case ClusterRebalance.rebalance_status() do
        {:ok, status} ->
          {:ok,
           %{
             job_id: status.job_id,
             status: Atom.to_string(status.status),
             progress_total: status.progress.total,
             progress_completed: status.progress.completed,
             progress_description: status.progress.description,
             tiers: Enum.map(status.tiers, &Atom.to_string/1),
             threshold: status.threshold
           }}

        {:error, :no_rebalance} ->
          {:error, NotFound.exception(message: "No rebalance in progress")}
      end
    end
  end

  @doc """
  Returns cluster-wide storage capacity information.
  """
  @spec handle_storage_stats() :: {:ok, map()}
  def handle_storage_stats do
    set_cli_metadata()

    with :ok <- require_cluster() do
      stats = StorageMetrics.cluster_capacity()

      drives =
        Enum.map(stats.drives, fn d ->
          %{
            node: Atom.to_string(d.node),
            drive_id: d.drive_id,
            tier: Atom.to_string(d.tier),
            capacity_bytes: serialise_capacity(d.capacity_bytes),
            used_bytes: d.used_bytes,
            available_bytes: serialise_capacity(d.available_bytes),
            state: Atom.to_string(d.state)
          }
        end)

      {:ok,
       %{
         drives: drives,
         total_capacity: serialise_capacity(stats.total_capacity),
         total_used: stats.total_used,
         total_available: serialise_capacity(stats.total_available)
       }}
    end
  end

  # Private

  # The CLI sends booleans as ETF atoms, but a JSON-ish caller may send
  # the string form.
  defp truthy?(opts, key) do
    Map.get(opts, key) in [true, "true"]
  end

  defp serialise_volume_replication(volume) do
    %{
      volume_id: volume.volume_id,
      volume_name: volume.volume_name,
      system: volume.system?,
      min_copies: volume.min_copies,
      chunk_count: volume.chunk_count,
      below_min_copies: volume.below_min_copies,
      zero_copies: volume.zero_copies,
      least_copies: volume.least_copies
    }
  end

  defp parse_tier_opt(opts, nil), do: opts

  defp parse_tier_opt(opts, tier) when is_binary(tier) do
    Keyword.put(opts, :tier, String.to_existing_atom(tier))
  rescue
    ArgumentError -> opts
  end

  defp parse_float_opt(opts, _key, nil), do: opts

  defp parse_float_opt(opts, key, value) when is_binary(value) do
    case Float.parse(value) do
      {f, ""} -> Keyword.put(opts, key, f)
      _ -> opts
    end
  end

  defp parse_float_opt(opts, key, value) when is_float(value) do
    Keyword.put(opts, key, value)
  end

  defp parse_float_opt(opts, _key, _value), do: opts

  defp parse_int_opt(opts, _key, nil), do: opts

  defp parse_int_opt(opts, key, value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 -> Keyword.put(opts, key, n)
      _ -> opts
    end
  end

  defp parse_int_opt(opts, key, value) when is_integer(value) and value > 0 do
    Keyword.put(opts, key, value)
  end

  defp parse_int_opt(opts, _key, _value), do: opts

  defp serialise_capacity(:unlimited), do: "unlimited"
  defp serialise_capacity(n) when is_integer(n), do: n
end
