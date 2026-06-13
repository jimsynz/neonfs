defmodule NeonFS.CLI.Handler.Maintenance do
  @moduledoc """
  CLI command handlers for per-volume background maintenance: garbage
  collection, scrub, and anti-entropy — each with an immediate-trigger,
  a schedule-interval setter, and a status view, plus the cluster-wide
  GC trigger/status.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates the matching RPC entry points here. Job-returning commands
  render via the shared `NeonFS.CLI.Handler.Common.job_to_map/1`.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.Job.Runners.{GarbageCollection, Scrub, VolumeAntiEntropy}
  alias NeonFS.Core.JobTracker
  alias NeonFS.Core.Volume.{MetadataReader, MetadataWriter}
  alias NeonFS.Error.Invalid

  @minimum_interval_ms 60_000

  @doc """
  Triggers a cluster-wide GC job, optionally scoped to one volume.
  """
  @spec handle_gc_collect(map()) :: {:ok, map()} | {:error, term()}
  def handle_gc_collect(opts \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, params} <- resolve_gc_params(opts),
         {:ok, job} <- JobTracker.create(GarbageCollection, params) do
      {:ok, job_to_map(job)}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns recent garbage-collection jobs across the cluster.
  """
  @spec handle_gc_status() :: {:ok, [map()]}
  def handle_gc_status do
    set_cli_metadata()

    with :ok <- require_cluster() do
      jobs = JobTracker.list_cluster(type: GarbageCollection)
      {:ok, Enum.map(jobs, &job_to_map/1)}
    end
  end

  @doc """
  Triggers an immediate GC job for the named volume.
  """
  @spec handle_volume_gc_now(binary()) :: {:ok, map()} | {:error, term()}
  def handle_volume_gc_now(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, job} <- JobTracker.create(GarbageCollection, %{volume_id: volume.id}) do
      {:ok, job_to_map(job)}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Updates the GC schedule interval for the named volume (min 1 minute).
  """
  @spec handle_volume_gc_set_interval(binary(), pos_integer()) :: {:ok, map()} | {:error, term()}
  def handle_volume_gc_set_interval(volume_name, interval_ms)
      when is_binary(volume_name) and is_integer(interval_ms) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         :ok <- validate_interval(interval_ms),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, segment, _} <- MetadataReader.resolve_segment_for_write(volume.id, []),
         existing = Map.get(segment.schedules, :gc, %{interval_ms: interval_ms, last_run: nil}),
         updated = %{existing | interval_ms: interval_ms},
         {:ok, _} <- MetadataWriter.update_schedule(volume.id, :gc, updated, []) do
      {:ok, %{volume_id: volume.id, volume_name: volume_name, schedule: schedule_to_map(updated)}}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns the GC schedule + latest GC job for the named volume.
  """
  @spec handle_volume_gc_status(binary()) :: {:ok, map()} | {:error, term()}
  def handle_volume_gc_status(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, segment, _} <- MetadataReader.resolve_segment_for_write(volume.id, []) do
      schedule = Map.get(segment.schedules, :gc)
      latest_job = latest_volume_job(GarbageCollection, volume.id)

      {:ok,
       %{
         volume_id: volume.id,
         volume_name: volume_name,
         schedule: schedule_to_map(schedule),
         next_run_due_at: next_run_due_at(schedule),
         latest_job: latest_job && job_to_map(latest_job)
       }}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Triggers an immediate scrub job for the named volume.
  """
  @spec handle_volume_scrub_now(binary()) :: {:ok, map()} | {:error, term()}
  def handle_volume_scrub_now(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, job} <- JobTracker.create(Scrub, %{volume_id: volume.id}) do
      {:ok, job_to_map(job)}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Updates the scrub schedule interval for the named volume (min 1 minute).
  """
  @spec handle_volume_scrub_set_interval(binary(), pos_integer()) ::
          {:ok, map()} | {:error, term()}
  def handle_volume_scrub_set_interval(volume_name, interval_ms)
      when is_binary(volume_name) and is_integer(interval_ms) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         :ok <- validate_interval(interval_ms),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, segment, _} <- MetadataReader.resolve_segment_for_write(volume.id, []),
         existing = Map.get(segment.schedules, :scrub, %{interval_ms: interval_ms, last_run: nil}),
         updated = %{existing | interval_ms: interval_ms},
         {:ok, _} <- MetadataWriter.update_schedule(volume.id, :scrub, updated, []) do
      {:ok, %{volume_id: volume.id, volume_name: volume_name, schedule: schedule_to_map(updated)}}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns the scrub schedule + latest scrub job for the named volume.
  """
  @spec handle_volume_scrub_status(binary()) :: {:ok, map()} | {:error, term()}
  def handle_volume_scrub_status(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, segment, _} <- MetadataReader.resolve_segment_for_write(volume.id, []) do
      schedule = Map.get(segment.schedules, :scrub)
      latest_job = latest_volume_job(Scrub, volume.id)

      {:ok,
       %{
         volume_id: volume.id,
         volume_name: volume_name,
         schedule: schedule_to_map(schedule),
         next_run_due_at: next_run_due_at(schedule),
         latest_job: latest_job && job_to_map(latest_job)
       }}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Triggers an immediate anti-entropy job for the named volume (#922).
  """
  @spec handle_volume_anti_entropy_now(binary()) :: {:ok, map()} | {:error, term()}
  def handle_volume_anti_entropy_now(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, job} <- JobTracker.create(VolumeAntiEntropy, %{volume_id: volume.id}) do
      {:ok, job_to_map(job)}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Updates the anti-entropy schedule interval for the named volume (#922).
  """
  @spec handle_volume_anti_entropy_set_interval(binary(), pos_integer()) ::
          {:ok, map()} | {:error, term()}
  def handle_volume_anti_entropy_set_interval(volume_name, interval_ms)
      when is_binary(volume_name) and is_integer(interval_ms) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         :ok <- validate_interval(interval_ms),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, segment, _} <- MetadataReader.resolve_segment_for_write(volume.id, []),
         existing =
           Map.get(segment.schedules, :anti_entropy, %{interval_ms: interval_ms, last_run: nil}),
         updated = %{existing | interval_ms: interval_ms},
         {:ok, _} <- MetadataWriter.update_schedule(volume.id, :anti_entropy, updated, []) do
      {:ok, %{volume_id: volume.id, volume_name: volume_name, schedule: schedule_to_map(updated)}}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns the anti-entropy schedule + latest job for the named volume (#922).
  """
  @spec handle_volume_anti_entropy_status(binary()) :: {:ok, map()} | {:error, term()}
  def handle_volume_anti_entropy_status(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, segment, _} <- MetadataReader.resolve_segment_for_write(volume.id, []) do
      schedule = Map.get(segment.schedules, :anti_entropy)
      latest_job = latest_volume_job(VolumeAntiEntropy, volume.id)

      {:ok,
       %{
         volume_id: volume.id,
         volume_name: volume_name,
         schedule: schedule_to_map(schedule),
         next_run_due_at: next_run_due_at(schedule),
         latest_job: latest_job && job_to_map(latest_job)
       }}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  # Private

  defp resolve_gc_params(%{"volume" => volume_name}) when is_binary(volume_name) do
    with {:ok, volume} <- fetch_volume(volume_name), do: {:ok, %{volume_id: volume.id}}
  end

  defp resolve_gc_params(_), do: {:ok, %{}}

  defp validate_interval(interval_ms)
       when is_integer(interval_ms) and interval_ms >= @minimum_interval_ms,
       do: :ok

  defp validate_interval(_),
    do:
      {:error,
       Invalid.exception(
         message: "interval_ms must be at least #{@minimum_interval_ms} (1 minute)"
       )}

  defp schedule_to_map(nil), do: nil

  defp schedule_to_map(%{interval_ms: interval, last_run: last}),
    do: %{interval_ms: interval, last_run: last}

  defp next_run_due_at(nil), do: nil
  defp next_run_due_at(%{last_run: nil}), do: :immediately

  defp next_run_due_at(%{interval_ms: interval, last_run: %DateTime{} = last}) do
    DateTime.add(last, interval, :millisecond)
  end

  defp latest_volume_job(runner, volume_id) do
    JobTracker.list_cluster(type: runner)
    |> Enum.filter(fn job -> Map.get(job.params || %{}, :volume_id) == volume_id end)
    |> Enum.sort_by(fn job -> job.updated_at end, {:desc, DateTime})
    |> List.first()
  end
end
