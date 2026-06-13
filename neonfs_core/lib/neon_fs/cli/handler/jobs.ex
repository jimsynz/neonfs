defmodule NeonFS.CLI.Handler.Jobs do
  @moduledoc """
  CLI command handlers for background jobs and the worker pool: listing
  and inspecting jobs (cluster-wide or local), cancelling them, and
  reading/reconfiguring the per-node `BackgroundWorker`.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `handle_list_jobs`/`handle_get_job`/`handle_cancel_job`/
  `handle_worker_status`/`handle_worker_configure` RPC entry points here,
  so the CLI wire contract is unchanged. Job rendering uses the shared
  `NeonFS.CLI.Handler.Common.job_to_map/1`.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Cluster.State
  alias NeonFS.Core.{BackgroundWorker, JobTracker, ServiceRegistry}
  alias NeonFS.Error.{Invalid, InvalidConfig, NotFound}

  @worker_config_keys %{
    "max_concurrent" => :max_concurrent,
    "max_per_minute" => :max_per_minute,
    "drive_concurrency" => :drive_concurrency
  }

  @doc """
  Lists jobs, optionally cluster-wide and filtered by status/type.
  """
  @spec handle_list_jobs(map()) :: {:ok, [map()]}
  def handle_list_jobs(filters \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      cluster_wide = Map.get(filters, "cluster", true)
      parsed_filters = parse_job_filters(filters)

      jobs =
        if cluster_wide do
          JobTracker.list_cluster(parsed_filters)
        else
          JobTracker.list(parsed_filters)
        end

      {:ok, Enum.map(jobs, &job_to_map/1)}
    end
  end

  @doc """
  Gets a job by ID.
  """
  @spec handle_get_job(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_get_job(job_id) when is_binary(job_id) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case JobTracker.get(job_id) do
        {:ok, job} -> {:ok, job_to_map(job)}
        {:error, :not_found} -> {:error, NotFound.exception(message: "Job not found: #{job_id}")}
      end
    end
  end

  @doc """
  Cancels a running or pending job.
  """
  @spec handle_cancel_job(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_cancel_job(job_id) when is_binary(job_id) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case JobTracker.cancel(job_id) do
        :ok ->
          {:ok, %{}}

        {:error, :not_found} ->
          {:error, NotFound.exception(message: "Job not found: #{job_id}")}

        {:error, :already_terminal} ->
          {:error, Invalid.exception(message: "Job already finished: #{job_id}")}
      end
    end
  end

  @doc """
  Returns background worker status across the cluster.
  """
  @spec handle_worker_status() :: {:ok, [map()]}
  def handle_worker_status do
    set_cli_metadata()

    with :ok <- require_cluster() do
      local_status = worker_status_map(Node.self(), BackgroundWorker.status())
      remote_statuses = collect_remote_worker_statuses()

      statuses =
        [local_status | remote_statuses]
        |> Enum.sort_by(& &1.node)

      {:ok, statuses}
    end
  end

  @doc """
  Reconfigures the background worker with new settings, validating that
  values are positive integers and persisting them to `cluster.json`.
  """
  @spec handle_worker_configure(map()) :: {:ok, map()} | {:error, term()}
  def handle_worker_configure(config) when is_map(config) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, changes} <- validate_worker_config(config),
         :ok <- BackgroundWorker.reconfigure(changes),
         :ok <- persist_worker_config(config) do
      status = BackgroundWorker.status()

      {:ok,
       %{
         max_concurrent: status.max_concurrent,
         max_per_minute: status.max_per_minute,
         drive_concurrency: status.drive_concurrency
       }}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  # Private — worker config

  defp validate_worker_config(config) do
    changes =
      config
      |> Map.take(Map.keys(@worker_config_keys))
      |> Enum.reduce_while([], fn {key, value}, acc ->
        case validate_positive_integer(key, value) do
          {:ok, int_value} ->
            {:cont, [{@worker_config_keys[key], int_value} | acc]}

          {:error, _} = err ->
            {:halt, err}
        end
      end)

    case changes do
      {:error, _} = err ->
        err

      list when is_list(list) and list == [] ->
        {:error, InvalidConfig.exception(reason: "no valid settings provided")}

      list when is_list(list) ->
        {:ok, list}
    end
  end

  defp validate_positive_integer(_key, value) when is_integer(value) and value > 0 do
    {:ok, value}
  end

  defp validate_positive_integer(key, value) when is_integer(value) do
    {:error,
     InvalidConfig.exception(
       field: String.to_atom(key),
       reason: "must be a positive integer, got: #{value}"
     )}
  end

  defp validate_positive_integer(key, value) do
    {:error,
     InvalidConfig.exception(
       field: String.to_atom(key),
       reason: "must be a positive integer, got: #{inspect(value)}"
     )}
  end

  defp persist_worker_config(config) do
    json_config =
      config
      |> Map.take(Map.keys(@worker_config_keys))

    State.update_worker_config(json_config)
  end

  # Private — job filters

  defp parse_job_filters(filters) do
    []
    |> parse_job_status_filter(Map.get(filters, "status"))
    |> parse_job_type_filter(Map.get(filters, "type"))
  end

  defp parse_job_status_filter(opts, nil), do: opts

  defp parse_job_status_filter(opts, status) when is_binary(status) do
    Keyword.put(opts, :status, String.to_existing_atom(status))
  rescue
    ArgumentError -> opts
  end

  defp parse_job_status_filter(opts, statuses) when is_list(statuses) do
    atoms =
      statuses
      |> Enum.map(fn s ->
        try do
          String.to_existing_atom(s)
        rescue
          ArgumentError -> nil
        end
      end)
      |> Enum.reject(&is_nil/1)

    if atoms != [], do: Keyword.put(opts, :status, atoms), else: opts
  end

  defp parse_job_status_filter(opts, _), do: opts

  defp parse_job_type_filter(opts, nil), do: opts

  defp parse_job_type_filter(opts, type_label) when is_binary(type_label) do
    # Find runner module by label
    # We search known runners; extensible as new runners are added
    known_runners = [
      NeonFS.Core.Job.Runners.ClusterRebalance,
      NeonFS.Core.Job.Runners.DriveEvacuation,
      NeonFS.Core.Job.Runners.GarbageCollection,
      NeonFS.Core.Job.Runners.KeyRotation,
      NeonFS.Core.Job.Runners.Scrub,
      NeonFS.Core.Job.Runners.VolumeAntiEntropy
    ]

    case Enum.find(known_runners, fn mod -> mod.label() == type_label end) do
      nil -> opts
      mod -> Keyword.put(opts, :type, mod)
    end
  end

  defp parse_job_type_filter(opts, _), do: opts

  # Private — worker status

  defp worker_status_map(node, status) do
    %{
      node: Atom.to_string(node),
      max_concurrent: status.max_concurrent,
      max_per_minute: status.max_per_minute,
      drive_concurrency: status.drive_concurrency,
      queued: status.queued,
      running: status.running,
      completed_total: status.completed_total,
      by_priority: %{
        high: status.by_priority[:high] || 0,
        normal: status.by_priority[:normal] || 0,
        low: status.by_priority[:low] || 0
      }
    }
  end

  defp collect_remote_worker_statuses do
    for node <- ServiceRegistry.connected_nodes_by_type(:core), reduce: [] do
      acc ->
        case safe_remote_worker_status(node) do
          {:ok, status} -> [worker_status_map(node, status) | acc]
          _ -> acc
        end
    end
  end

  defp safe_remote_worker_status(node) do
    case :erpc.call(node, BackgroundWorker, :status, [], 5_000) do
      status when is_map(status) -> {:ok, status}
      other -> {:error, other}
    end
  catch
    :exit, _ -> {:error, :unreachable}
  end
end
