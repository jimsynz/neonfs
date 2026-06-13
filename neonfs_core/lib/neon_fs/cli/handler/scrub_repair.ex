defmodule NeonFS.CLI.Handler.ScrubRepair do
  @moduledoc """
  CLI command handlers for cluster integrity jobs: scrub (start/status)
  and replica repair (start/status), each optionally scoped to a single
  volume.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates the matching RPC entry points here. Job-returning commands
  render via the shared `NeonFS.CLI.Handler.Common.job_to_map/1`.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.Job.Runners.{ReplicaRepair, Scrub}
  alias NeonFS.Core.{JobTracker, ReplicaRepairScheduler}

  @doc """
  Starts an integrity scrub job, optionally scoped to one volume.
  """
  @spec handle_scrub_start(map()) :: {:ok, map()} | {:error, term()}
  def handle_scrub_start(opts \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, params} <- resolve_scrub_params(opts),
         {:ok, job} <- JobTracker.create(Scrub, params) do
      {:ok, job_to_map(job)}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns recent scrub jobs across the cluster, most recent first.
  """
  @spec handle_scrub_status() :: {:ok, [map()]}
  def handle_scrub_status do
    set_cli_metadata()

    with :ok <- require_cluster() do
      jobs = JobTracker.list_cluster(type: Scrub)
      {:ok, Enum.map(jobs, &job_to_map/1)}
    end
  end

  @doc """
  Starts a replica-repair pass — every volume (no `"volume"` opt) or a
  single volume — with the scheduler's dedupe semantics.
  """
  @spec handle_repair_start(map()) :: {:ok, [map()]} | {:error, term()}
  def handle_repair_start(opts \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, scope} <- resolve_repair_scope(opts) do
      case ReplicaRepairScheduler.trigger_now(scope) do
        {:ok, jobs} -> {:ok, Enum.map(jobs, &job_to_map/1)}
        {:skipped, :already_running} -> {:ok, []}
      end
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns recent replica-repair jobs across the cluster, optionally
  filtered by volume.
  """
  @spec handle_repair_status(map()) :: {:ok, [map()]} | {:error, term()}
  def handle_repair_status(opts \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      jobs =
        JobTracker.list_cluster(type: ReplicaRepair)
        |> filter_by_volume(opts)

      {:ok, Enum.map(jobs, &job_to_map/1)}
    end
  end

  # Private

  defp resolve_scrub_params(%{"volume" => volume_name}) when is_binary(volume_name) do
    with {:ok, volume} <- fetch_volume(volume_name), do: {:ok, %{volume_id: volume.id}}
  end

  defp resolve_scrub_params(_), do: {:ok, %{}}

  # Resolve `:all` (no volume) or a single-volume scope from the CLI opts.
  defp resolve_repair_scope(%{"volume" => volume_name}) when is_binary(volume_name) do
    with {:ok, volume} <- fetch_volume(volume_name), do: {:ok, volume.id}
  end

  defp resolve_repair_scope(_), do: {:ok, :all}

  defp filter_by_volume(jobs, %{"volume" => volume_name}) when is_binary(volume_name) do
    case fetch_volume(volume_name) do
      {:ok, volume} ->
        Enum.filter(jobs, fn job ->
          (job.params[:volume_id] || job.params["volume_id"]) == volume.id
        end)

      _ ->
        []
    end
  end

  defp filter_by_volume(jobs, _), do: jobs
end
