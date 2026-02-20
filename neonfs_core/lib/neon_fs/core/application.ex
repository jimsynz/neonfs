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

  alias NeonFS.Cluster.State
  alias NeonFS.Core.{DriveConfig, Persistence}

  @impl true
  def start(_type, _args) do
    load_config_from_cluster_state()

    # Start the core supervision tree
    NeonFS.Core.Supervisor.start_link([])
  end

  @impl true
  def stop(_state) do
    # Snapshot all metadata tables before the supervision tree shuts down.
    # This must happen here because ETS tables are destroyed when their
    # owning GenServers terminate, and the supervisor shuts down children
    # in reverse order (Persistence last, but ETS tables are already gone).
    Logger.info("Application stopping, triggering final snapshot...")

    case Persistence.snapshot_now() do
      :ok ->
        Logger.info("Final snapshot completed successfully")

      {:error, reason} ->
        Logger.error("Final snapshot failed: #{inspect(reason)}")
    end

    :ok
  end

  defp load_config_from_cluster_state do
    case State.load() do
      {:ok, state} ->
        load_drives_config(state.drives)
        load_worker_config(state.worker)

      _ ->
        :ok
    end
  end

  defp load_drives_config([_ | _] = drives) do
    parsed = Enum.map(drives, &parse_drive_from_json/1)
    Logger.info("Loaded #{length(parsed)} drive(s) from cluster.json")
    Application.put_env(:neonfs_core, :drives, parsed)
  end

  defp load_drives_config(_), do: :ok

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
