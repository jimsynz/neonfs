defmodule NeonFS.Core.TelemetryPoller do
  @moduledoc """
  Periodic gauge emitter for point-in-time metrics.
  """

  @default_period_ms 15_000

  @doc "Starts the telemetry poller."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    period =
      Keyword.get(
        opts,
        :period,
        Application.get_env(:neonfs_core, :telemetry_poller_interval_ms, @default_period_ms)
      )

    runtime_opts =
      Keyword.take(opts, [
        :background_worker_mod,
        :chunk_cache_mod,
        :ra_mod,
        :ra_supervisor_mod,
        :storage_metrics_mod,
        :telemetry_mod
      ])

    Application.put_env(:neonfs_core, :telemetry_poller_runtime_opts, runtime_opts)

    :telemetry_poller.start_link(
      measurements: measurement_specs(),
      period: period
    )
  end

  @doc false
  @spec measure_cache() :: :ok
  def measure_cache, do: measure_cache(runtime_opts())

  @spec measure_cache(keyword()) :: :ok
  def measure_cache(opts) do
    with_measurement(fn ->
      chunk_cache_mod = Keyword.get(opts, :chunk_cache_mod, NeonFS.Core.ChunkCache)
      telemetry_mod = Keyword.get(opts, :telemetry_mod, :telemetry)

      stats = chunk_cache_mod.stats()

      memory_used =
        case Map.fetch(stats, :memory_used) do
          {:ok, value} when is_integer(value) and value >= 0 -> value
          _ -> 0
        end

      entry_count = cache_entry_count(chunk_cache_mod)

      telemetry_mod.execute(
        [:neonfs, :cache, :size],
        %{bytes: memory_used, entry_count: entry_count},
        %{}
      )
    end)
  end

  @doc false
  @spec measure_cluster() :: :ok
  def measure_cluster, do: measure_cluster(runtime_opts())

  @spec measure_cluster(keyword()) :: :ok
  def measure_cluster(opts) do
    with_measurement(fn ->
      telemetry_mod = Keyword.get(opts, :telemetry_mod, :telemetry)

      telemetry_mod.execute(
        [:neonfs, :cluster, :nodes],
        %{count: length(Node.list()) + 1},
        %{}
      )

      %{leader: leader, term: term} = ra_state(opts)

      telemetry_mod.execute(
        [:neonfs, :cluster, :ra],
        %{leader: leader, term: term},
        %{}
      )
    end)
  end

  @doc false
  @spec measure_storage() :: :ok
  def measure_storage, do: measure_storage(runtime_opts())

  @spec measure_storage(keyword()) :: :ok
  def measure_storage(opts) do
    with_measurement(fn ->
      storage_metrics_mod = Keyword.get(opts, :storage_metrics_mod, NeonFS.Core.StorageMetrics)
      telemetry_mod = Keyword.get(opts, :telemetry_mod, :telemetry)

      storage_metrics_mod.cluster_capacity()
      |> Map.get(:drives, [])
      |> Enum.each(fn drive ->
        metadata = %{
          drive_id: Map.get(drive, :drive_id) || Map.get(drive, :id),
          node: drive.node,
          tier: drive.tier
        }

        telemetry_mod.execute(
          [:neonfs, :storage, :utilisation],
          %{capacity: drive.capacity_bytes, reserved: 0, used: drive.used_bytes},
          metadata
        )

        telemetry_mod.execute(
          [:neonfs, :storage, :drive_state],
          %{state: drive_state_value(drive.state)},
          metadata
        )
      end)
    end)
  end

  @doc false
  @spec measure_worker() :: :ok
  def measure_worker, do: measure_worker(runtime_opts())

  @spec measure_worker(keyword()) :: :ok
  def measure_worker(opts) do
    with_measurement(fn ->
      background_worker_mod =
        Keyword.get(opts, :background_worker_mod, NeonFS.Core.BackgroundWorker)

      telemetry_mod = Keyword.get(opts, :telemetry_mod, :telemetry)

      by_priority =
        background_worker_mod.status()
        |> Map.get(:by_priority, %{})

      for priority <- [:high, :normal, :low] do
        count = worker_queue_depth(by_priority, priority)

        telemetry_mod.execute(
          [:neonfs, :worker, :queue_depth],
          %{count: count},
          %{priority: priority}
        )
      end
    end)
  end

  defp cache_entry_count(chunk_cache_mod) do
    if function_exported?(chunk_cache_mod, :entry_count, 0) do
      chunk_cache_mod.entry_count()
    else
      0
    end
  end

  defp drive_state_value(:standby), do: 0
  defp drive_state_value(_), do: 1

  defp worker_queue_depth(by_priority, priority) do
    case Map.fetch(by_priority, priority) do
      {:ok, value} when is_integer(value) and value >= 0 -> value
      _ -> 0
    end
  end

  defp measurement_specs do
    [
      {__MODULE__, :measure_storage, []},
      {__MODULE__, :measure_cache, []},
      {__MODULE__, :measure_cluster, []},
      {__MODULE__, :measure_worker, []}
    ]
  end

  defp runtime_opts do
    Application.get_env(:neonfs_core, :telemetry_poller_runtime_opts, [])
  end

  defp ra_state(opts) do
    ra_mod = Keyword.get(opts, :ra_mod, :ra)
    ra_supervisor_mod = Keyword.get(opts, :ra_supervisor_mod, NeonFS.Core.RaSupervisor)

    server_id = ra_supervisor_mod.server_id()
    metrics = ra_mod.key_metrics(server_id)

    %{
      leader: if(Map.get(metrics, :membership) == :leader, do: 1, else: 0),
      term: term_value(Map.get(metrics, :term))
    }
  rescue
    _ -> %{leader: 0, term: 0}
  catch
    :exit, _ -> %{leader: 0, term: 0}
  end

  defp term_value(term) when is_integer(term) and term >= 0, do: term
  defp term_value(_), do: 0

  defp with_measurement(fun) do
    fun.()
    :telemetry.execute([:neonfs, :telemetry_poller, :measurement], %{status: :ok}, %{})
    :ok
  rescue
    _ ->
      :telemetry.execute([:neonfs, :telemetry_poller, :measurement], %{status: :error}, %{})
      :ok
  catch
    :exit, _ ->
      :telemetry.execute([:neonfs, :telemetry_poller, :measurement], %{status: :error}, %{})
      :ok
  end
end
