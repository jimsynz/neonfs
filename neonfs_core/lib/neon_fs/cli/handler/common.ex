defmodule NeonFS.CLI.Handler.Common do
  @moduledoc """
  Cross-cutting helpers shared by the CLI command-group handlers.

  `NeonFS.CLI.Handler` and the per-group modules it delegates to
  (`NeonFS.CLI.Handler.S3`, …) `import` this module so the common
  request preamble (`set_cli_metadata/0`, `require_cluster/0`), the
  error-normalisation (`wrap_error/1`), and the shared `job_to_map/1`
  serialiser (every operational command that returns a background job
  renders it the same way) live in one place rather than being copied
  into every group. Extracted while splitting the handler per #1203.
  """

  alias NeonFS.Cluster.State
  alias NeonFS.Core.{Job, VolumeRegistry}
  alias NeonFS.Error.{Internal, NotFound, Unavailable, VolumeNotFound}

  @doc """
  Returns `:ok` when a cluster has been initialised, or a `NotFound`
  error instructing the operator to init/join first.
  """
  @spec require_cluster() :: :ok | {:error, NotFound.t()}
  def require_cluster do
    if State.exists?() do
      :ok
    else
      {:error,
       NotFound.exception(
         message:
           "Cluster not initialised. Run 'neonfs cluster init' or 'neonfs cluster join' first."
       )}
    end
  end

  @doc """
  Tags the calling process's logger metadata with the `:cli` component
  and a fresh request id, so every command's logs are correlatable.
  """
  @spec set_cli_metadata() :: :ok
  def set_cli_metadata do
    Logger.metadata(
      component: :cli,
      request_id: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    )
  end

  @doc """
  Wraps a legacy error reason into a structured `NeonFS.Error`.
  Already-structured errors (Splode exceptions with a class field) pass
  through unchanged.
  """
  @spec wrap_error(term()) :: Exception.t()
  def wrap_error(%{__exception__: true, class: _} = error), do: error

  def wrap_error(reason) when is_binary(reason),
    do: Internal.exception(message: reason)

  def wrap_error(reason) when is_atom(reason),
    do: Internal.exception(message: Atom.to_string(reason))

  def wrap_error(reason),
    do: Internal.exception(message: inspect(reason))

  @doc """
  Renders a `NeonFS.Core.Job` as a serialisable map for CLI output.
  Shared by every command group that kicks off or queries a background
  job (drives, GC, scrub, snapshots, backup, repair, jobs).
  """
  @spec job_to_map(Job.t()) :: map()
  def job_to_map(%Job{} = job) do
    %{
      id: job.id,
      type: job.type.label(),
      node: Atom.to_string(job.node),
      status: Atom.to_string(job.status),
      progress_total: job.progress.total,
      progress_completed: job.progress.completed,
      progress_description: job.progress.description,
      params: serialise_params(job.params),
      error: if(job.error, do: inspect(job.error)),
      created_at: DateTime.to_iso8601(job.created_at),
      started_at: if(job.started_at, do: DateTime.to_iso8601(job.started_at)),
      updated_at: DateTime.to_iso8601(job.updated_at),
      completed_at: if(job.completed_at, do: DateTime.to_iso8601(job.completed_at))
    }
  end

  defp serialise_params(params) when is_map(params) do
    Map.new(params, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), serialise_param_value(v)}
      {k, v} -> {to_string(k), serialise_param_value(v)}
    end)
  end

  defp serialise_param_value(v) when is_atom(v), do: Atom.to_string(v)
  defp serialise_param_value(v) when is_binary(v), do: v
  defp serialise_param_value(v) when is_number(v), do: v
  defp serialise_param_value(v), do: inspect(v)

  @doc """
  Resolves a volume by name, returning a `VolumeNotFound` error when it
  doesn't exist. The standard volume lookup used by nearly every
  volume-scoped CLI command.
  """
  @spec fetch_volume(String.t()) :: {:ok, struct()} | {:error, VolumeNotFound.t()}
  def fetch_volume(volume_name) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, VolumeNotFound.exception(volume_name: volume_name)}
    end
  end

  @doc """
  Local node uptime in whole seconds (BEAM wall-clock). Shared by the
  cluster-status and node-list commands.
  """
  @spec get_uptime() :: non_neg_integer()
  def get_uptime do
    {uptime_ms, _} = :erlang.statistics(:wall_clock)
    div(uptime_ms, 1000)
  end

  @doc """
  Loads the persisted cluster state, normalising load failures into a
  structured `Unavailable` error. Shared by the cluster-lifecycle and
  CA-rotation commands that need the cluster name or id.
  """
  @spec load_cluster_state() :: {:ok, State.t()} | {:error, Unavailable.t()}
  def load_cluster_state do
    case State.load() do
      {:ok, state} ->
        {:ok, state}

      {:error, reason} ->
        {:error, Unavailable.exception(message: "Cannot load cluster state: #{inspect(reason)}")}
    end
  end
end
