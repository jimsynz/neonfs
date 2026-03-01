defmodule NeonFS.Core.HealthCheck do
  @moduledoc """
  Aggregates subsystem health into a single report.
  """

  alias NeonFS.Core.{
    ChunkCache,
    ClockMonitor,
    DriveRegistry,
    RaSupervisor,
    ServiceRegistry,
    StorageMetrics,
    VolumeRegistry
  }

  @default_timeout_ms 5_000
  @default_cache_max_memory 268_435_456

  @type status :: :healthy | :degraded | :unhealthy
  @type subsystem_report :: %{required(:status) => status(), optional(atom()) => term()}

  @doc """
  Returns a health report for all core subsystems.
  """
  @spec check() :: map()
  def check, do: check([])

  @doc """
  Returns a health report with optional runtime overrides.

  Supported options:
  - `:timeout_ms` — per-subsystem timeout (default: `5000`)
  - `:checks` — custom subsystem checks for tests
  """
  @spec check(keyword()) :: map()
  def check(opts) when is_list(opts) do
    timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
    checks = Keyword.get(opts, :checks, default_checks(opts))
    subsystem_reports = run_checks(checks, timeout_ms)

    %{
      checked_at: DateTime.utc_now(),
      checks: subsystem_reports,
      node: Node.self(),
      status: overall_status(subsystem_reports)
    }
  end

  defp default_checks(opts) do
    cache_max_memory = Keyword.get(opts, :cache_max_memory, configured_cache_max_memory())
    chunk_cache_mod = Keyword.get(opts, :chunk_cache_mod, ChunkCache)
    clock_monitor_mod = Keyword.get(opts, :clock_monitor_mod, ClockMonitor)
    drive_registry_mod = Keyword.get(opts, :drive_registry_mod, DriveRegistry)
    node_lister = Keyword.get(opts, :node_lister, &default_node_lister/0)
    service_registry_mod = Keyword.get(opts, :service_registry_mod, ServiceRegistry)
    storage_metrics_mod = Keyword.get(opts, :storage_metrics_mod, StorageMetrics)
    time_fetcher = Keyword.get(opts, :time_fetcher, &default_time_fetcher/1)
    volume_registry_mod = Keyword.get(opts, :volume_registry_mod, VolumeRegistry)

    [
      cache: fn -> check_cache(chunk_cache_mod, cache_max_memory) end,
      clock: fn -> check_clock(clock_monitor_mod, node_lister, time_fetcher) end,
      drives: fn -> check_drives(drive_registry_mod) end,
      ra: fn -> check_ra() end,
      service_registry: fn -> check_service_registry(service_registry_mod) end,
      storage: fn -> check_storage(storage_metrics_mod) end,
      volumes: fn -> check_volumes(service_registry_mod, volume_registry_mod) end
    ]
  end

  defp run_checks(checks, timeout_ms) do
    checks
    |> Enum.zip(
      Task.async_stream(
        checks,
        fn {name, fun} -> {name, safe_check(fun)} end,
        ordered: true,
        on_timeout: :kill_task,
        timeout: timeout_ms
      )
    )
    |> Enum.reduce(%{}, fn
      {{name, _fun}, {:ok, {_name, report}}}, acc ->
        Map.put(acc, name, report)

      {{name, _fun}, {:exit, :timeout}}, acc ->
        Map.put(acc, name, %{status: :unhealthy, reason: :timeout})

      {{name, _fun}, {:exit, reason}}, acc ->
        Map.put(acc, name, %{status: :unhealthy, reason: {:exit, inspect(reason)}})
    end)
  end

  defp safe_check(fun) do
    case fun.() do
      %{status: status} = report when status in [:healthy, :degraded, :unhealthy] ->
        report

      report ->
        %{status: :unhealthy, reason: {:invalid_response, inspect(report)}}
    end
  rescue
    error ->
      %{status: :unhealthy, reason: {:exception, Exception.message(error)}}
  catch
    kind, reason ->
      %{status: :unhealthy, reason: {kind, inspect(reason)}}
  end

  defp check_cache(chunk_cache_mod, cache_max_memory) do
    if Process.whereis(chunk_cache_mod) do
      stats = chunk_cache_mod.stats()
      memory_used = Map.get(stats, :memory_used, 0)

      status =
        cond do
          memory_used > cache_max_memory -> :unhealthy
          memory_used > trunc(cache_max_memory * 0.9) -> :degraded
          true -> :healthy
        end

      %{
        status: status,
        entry_count: cache_entry_count(chunk_cache_mod),
        max_memory_bytes: cache_max_memory,
        memory_used_bytes: memory_used
      }
    else
      %{status: :unhealthy, reason: :not_running}
    end
  end

  defp check_clock(clock_monitor_mod, node_lister, time_fetcher) do
    if Process.whereis(clock_monitor_mod) do
      probes = Enum.map(node_lister.(), &probe_clock_skew(&1, time_fetcher))
      max_skew_ms = max_probe_skew_ms(probes)
      unreachable_nodes = Enum.count(probes, &(&1.status == :error))
      quarantined_nodes = clock_monitor_mod.quarantined_nodes()

      %{
        status:
          classify_clock_status(max_skew_ms, unreachable_nodes, length(probes), quarantined_nodes),
        max_skew_ms: max_skew_ms,
        probes: probes,
        quarantined_nodes: quarantined_nodes
      }
    else
      %{status: :unhealthy, reason: :not_running}
    end
  end

  defp check_drives(drive_registry_mod) do
    drives = drive_registry_mod.list_drives()
    reports = Enum.map(drives, &drive_health/1)
    unhealthy = Enum.count(reports, &(&1.status == :unhealthy))
    degraded = Enum.count(reports, &(&1.status == :degraded))

    status =
      cond do
        reports == [] -> :unhealthy
        unhealthy == length(reports) -> :unhealthy
        unhealthy > 0 or degraded > 0 -> :degraded
        true -> :healthy
      end

    %{
      status: status,
      degraded_count: degraded,
      drives: reports,
      total_count: length(reports),
      unhealthy_count: unhealthy
    }
  end

  defp check_ra do
    case :ra.members(RaSupervisor.server_id(), 1_000) do
      {:ok, members, leader} ->
        this_node_member = RaSupervisor.server_id() in members

        %{
          status: if(this_node_member, do: :healthy, else: :unhealthy),
          leader: leader,
          member_count: length(members),
          this_node_member: this_node_member
        }

      {:timeout, _node} ->
        %{status: :unhealthy, reason: :timeout}

      {:error, reason} ->
        %{status: :unhealthy, reason: reason}
    end
  rescue
    _ -> %{status: :unhealthy, reason: :not_available}
  end

  defp check_service_registry(service_registry_mod) do
    if Process.whereis(service_registry_mod) do
      services = service_registry_mod.list()
      core_nodes = Enum.count(services, &(&1.type == :core))

      %{
        status: :healthy,
        core_nodes: core_nodes,
        service_count: length(services)
      }
    else
      %{status: :unhealthy, reason: :not_running}
    end
  end

  defp check_storage(storage_metrics_mod) do
    metrics = storage_metrics_mod.cluster_capacity()
    drives = Map.get(metrics, :drives, [])
    utilisation = storage_utilisation(metrics)
    writable_drives = Enum.count(drives, &writable_drive?/1)
    unknown_state_drives = Enum.count(drives, &unknown_drive_state?/1)

    status =
      cond do
        drives == [] -> :unhealthy
        writable_drives == 0 -> :unhealthy
        utilisation && utilisation > 0.95 -> :unhealthy
        unknown_state_drives > 0 -> :degraded
        utilisation && utilisation > 0.85 -> :degraded
        true -> :healthy
      end

    %{
      status: status,
      drive_count: length(drives),
      total_capacity_bytes: metrics.total_capacity,
      total_used_bytes: metrics.total_used,
      utilisation_ratio: utilisation,
      writable_drive_count: writable_drives
    }
  rescue
    _ -> %{status: :unhealthy, reason: :not_available}
  end

  defp check_volumes(service_registry_mod, volume_registry_mod) do
    volumes = volume_registry_mod.list(include_system: true)
    core_count = core_node_count(service_registry_mod)

    degraded_volumes =
      Enum.filter(volumes, fn volume ->
        get_in(volume, [:durability, :factor]) > core_count
      end)

    status =
      cond do
        degraded_volumes == [] -> :healthy
        length(degraded_volumes) == length(volumes) -> :unhealthy
        true -> :degraded
      end

    %{
      status: status,
      core_nodes: core_count,
      degraded_redundancy_count: length(degraded_volumes),
      volume_count: length(volumes)
    }
  rescue
    _ -> %{status: :unhealthy, reason: :not_available}
  end

  defp overall_status(subsystem_reports) do
    statuses = subsystem_reports |> Map.values() |> Enum.map(& &1.status)

    cond do
      :unhealthy in statuses -> :unhealthy
      :degraded in statuses -> :degraded
      true -> :healthy
    end
  end

  defp cache_entry_count(chunk_cache_mod) do
    if function_exported?(chunk_cache_mod, :entry_count, 0),
      do: chunk_cache_mod.entry_count(),
      else: nil
  end

  defp configured_cache_max_memory do
    Application.get_env(:neonfs_core, :chunk_cache_max_memory, @default_cache_max_memory)
  end

  defp core_node_count(service_registry_mod) do
    if Process.whereis(service_registry_mod) do
      service_registry_mod.list_by_type(:core) |> length() |> max(1)
    else
      max(length(Node.list()) + 1, 1)
    end
  rescue
    _ -> max(length(Node.list()) + 1, 1)
  end

  defp default_node_lister, do: Node.list()

  defp default_time_fetcher(node) do
    if node == Node.self() do
      {:ok, System.system_time(:millisecond)}
    else
      case :rpc.call(node, System, :system_time, [:millisecond], 1_000) do
        {:badrpc, reason} -> {:error, reason}
        time when is_integer(time) -> {:ok, time}
      end
    end
  end

  defp drive_health(drive) do
    local_drive = drive.node == Node.self()

    path_status =
      cond do
        not local_drive -> :remote
        File.dir?(drive.path) -> :accessible
        true -> :missing
      end

    status =
      cond do
        unknown_drive_state?(drive) -> :unhealthy
        path_status == :missing -> :unhealthy
        drive.state == :draining -> :degraded
        true -> :healthy
      end

    %{
      id: drive.id,
      node: drive.node,
      path_status: path_status,
      state: drive.state,
      status: status
    }
  end

  defp probe_clock_skew(node, time_fetcher) do
    t1 = System.system_time(:millisecond)

    case time_fetcher.(node) do
      {:ok, remote_ms} ->
        t2 = System.system_time(:millisecond)
        estimated_local_ms = t1 + div(t2 - t1, 2)
        %{node: node, skew_ms: abs(remote_ms - estimated_local_ms), status: :ok}

      {:error, reason} ->
        %{node: node, reason: inspect(reason), status: :error}
    end
  end

  defp max_probe_skew_ms(probes) do
    probes
    |> Enum.filter(&(&1.status == :ok))
    |> Enum.map(& &1.skew_ms)
    |> Enum.max(fn -> 0 end)
  end

  defp classify_clock_status(max_skew_ms, unreachable_nodes, probe_count, quarantined_nodes) do
    cond do
      max_skew_ms > 500 -> :unhealthy
      unreachable_nodes > 0 and unreachable_nodes == probe_count and probe_count > 0 -> :unhealthy
      max_skew_ms > 100 -> :degraded
      unreachable_nodes > 0 -> :degraded
      quarantined_nodes != [] -> :degraded
      true -> :healthy
    end
  end

  defp storage_utilisation(%{total_capacity: :unlimited}), do: nil
  defp storage_utilisation(%{total_capacity: 0}), do: nil

  defp storage_utilisation(%{total_capacity: total_capacity, total_used: total_used})
       when is_integer(total_capacity) and is_integer(total_used) do
    total_used / total_capacity
  end

  defp storage_utilisation(_), do: nil

  defp unknown_drive_state?(drive), do: drive.state not in [:active, :standby, :draining]
  defp writable_drive?(drive), do: drive.state in [:active, :standby]
end
