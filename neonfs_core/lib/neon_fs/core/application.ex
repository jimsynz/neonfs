defmodule NeonFS.Core.Application do
  @moduledoc """
  Application callback module for NeonFS Core.

  Starts the core supervision tree which manages:
  - Blob storage (content-addressed storage)
  - Chunk metadata index
  - File metadata index
  - Volume registry
  """

  use Application
  require Logger

  alias NeonFS.Cluster.{Formation, State}
  alias NeonFS.Core.{DriveConfig, HealthCheck, Persistence}

  @impl true
  def start(_type, _args) do
    Logger.metadata(node_name: node())
    load_config_from_cluster_state()

    # Run orphan detection BEFORE the supervisor starts children.
    # After supervisor init, sibling processes (Persistence, Ra, BlobStore)
    # create files that would trigger false positives in orphan detection.
    cache_orphan_detection_result()

    # Start the core supervision tree
    result = NeonFS.Core.Supervisor.start_link([])

    if Application.get_env(:neonfs_core, :start_children?, true) do
      HealthCheck.register_checks()
    end

    result
  end

  @impl true
  def stop(_state) do
    # Snapshot all metadata tables before the supervision tree shuts down.
    # This must happen here because ETS tables are destroyed when their
    # owning GenServers terminate, and the supervisor shuts down children
    # in reverse order (Persistence last, but ETS tables are already gone).
    Logger.info("Application stopping, triggering final snapshot")

    case Persistence.snapshot_now() do
      :ok ->
        Logger.info("Final snapshot completed successfully")

      {:error, reason} ->
        Logger.error("Final snapshot failed", reason: inspect(reason))
    end

    :ok
  end

  defp cache_orphan_detection_result do
    if Application.get_env(:neonfs_core, :auto_bootstrap, false) and not State.exists?() do
      result = Formation.orphaned_data_detected?()
      Application.put_env(:neonfs_core, :orphaned_data_at_startup, result)
    end
  end

  defp load_config_from_cluster_state do
    case State.load() do
      {:ok, state} ->
        load_drives_config(state.drives)
        load_gc_config(state.gc)
        load_metrics_config(state.metrics)
        load_peer_config(state)
        load_scrub_config(state.scrub)
        load_worker_config(state.worker)

      {:error, :not_found} ->
        Logger.info("No cluster.json found, using default configuration")

      {:error, :invalid_json} ->
        Logger.warning("cluster.json contains invalid JSON, using default configuration")

      {:error, {:validation_failed, errors}} ->
        messages = Enum.map_join(errors, "; ", &"#{&1.field}: #{&1.message}")

        Logger.warning("cluster.json validation failed, using default configuration",
          errors: messages
        )

      {:error, reason} ->
        Logger.warning("Failed to load cluster.json, using default configuration",
          reason: inspect(reason)
        )
    end
  end

  defp load_drives_config([_ | _] = drives) do
    parsed = Enum.map(drives, &parse_drive_from_json/1)
    Logger.info("Loaded drives from cluster.json", count: length(parsed))
    Application.put_env(:neonfs_core, :drives, parsed)
  end

  defp load_drives_config(_), do: :ok

  defp load_gc_config(gc_config) when is_map(gc_config) and gc_config != %{} do
    if interval = gc_config["interval_ms"],
      do: Application.put_env(:neonfs_core, :gc_interval_ms, interval)

    if threshold = gc_config["pressure_threshold"],
      do: Application.put_env(:neonfs_core, :gc_pressure_threshold, threshold)

    if pressure_interval = gc_config["pressure_check_interval_ms"],
      do: Application.put_env(:neonfs_core, :gc_pressure_check_interval_ms, pressure_interval)

    Logger.info("Loaded GC config from cluster.json")
  end

  defp load_gc_config(_), do: :ok

  defp load_peer_config(state) do
    Application.put_env(:neonfs_client, :peer_connect_timeout, state.peer_connect_timeout)
    Application.put_env(:neonfs_client, :peer_sync_interval, state.peer_sync_interval)
    Application.put_env(:neonfs_core, :min_peers_for_operation, state.min_peers_for_operation)
    Application.put_env(:neonfs_core, :startup_peer_timeout, state.startup_peer_timeout)
  end

  defp load_metrics_config(metrics_config)
       when is_map(metrics_config) and metrics_config != %{} do
    if is_boolean(metrics_config["enabled"]),
      do: Application.put_env(:neonfs_core, :metrics_enabled, metrics_config["enabled"])

    if is_integer(metrics_config["port"]) and metrics_config["port"] > 0,
      do: Application.put_env(:neonfs_core, :metrics_port, metrics_config["port"])

    if is_binary(metrics_config["bind"]) and metrics_config["bind"] != "",
      do: Application.put_env(:neonfs_core, :metrics_bind, metrics_config["bind"])

    if is_integer(metrics_config["poll_interval_ms"]) and metrics_config["poll_interval_ms"] > 0,
      do:
        Application.put_env(
          :neonfs_core,
          :telemetry_poller_interval_ms,
          metrics_config["poll_interval_ms"]
        )

    Logger.info("Loaded metrics config from cluster.json")
  end

  defp load_metrics_config(_), do: :ok

  defp load_scrub_config(scrub_config) when is_map(scrub_config) and scrub_config != %{} do
    if interval = scrub_config["check_interval_ms"],
      do: Application.put_env(:neonfs_core, :scrub_check_interval_ms, interval)

    Logger.info("Loaded scrub config from cluster.json")
  end

  defp load_scrub_config(_), do: :ok

  defp load_worker_config(worker_config) when is_map(worker_config) and worker_config != %{} do
    if max_c = worker_config["max_concurrent"],
      do: Application.put_env(:neonfs_core, :worker_max_concurrent, max_c)

    if max_m = worker_config["max_per_minute"],
      do: Application.put_env(:neonfs_core, :worker_max_per_minute, max_m)

    if drive_c = worker_config["drive_concurrency"],
      do: Application.put_env(:neonfs_core, :worker_drive_concurrency, drive_c)

    Logger.info("Loaded worker config from cluster.json")
  end

  defp load_worker_config(_), do: :ok

  defp parse_drive_from_json(drive) do
    capacity_str = to_string(drive["capacity"] || drive[:capacity] || "0")

    capacity =
      case DriveConfig.parse_capacity(capacity_str) do
        {:ok, bytes} -> bytes
        {:error, _} -> 0
      end

    %{
      id: to_string(drive["id"] || drive[:id]),
      path: to_string(drive["path"] || drive[:path]),
      tier: parse_tier(drive["tier"] || drive[:tier]),
      capacity: capacity
    }
  end

  defp parse_tier(tier) when is_atom(tier), do: tier
  defp parse_tier(tier) when is_binary(tier), do: String.to_existing_atom(tier)
  defp parse_tier(_), do: :hot
end
