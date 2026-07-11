defmodule NeonFS.NFS.ExportManager do
  @moduledoc """
  Manages the NFS server lifecycle and mirrors the cluster's export set.

  Exports are cluster state (#1175): a volume is exported when its
  `nfs_export` flag is set via `neonfs nfs export`, which writes the
  core volume registry. Every NFS node serves every export, so any node
  behind a load balancer can answer for any exported volume. This
  process keeps a local mirror of that set by:

  - resyncing from core on startup (retrying until core is reachable),
  - subscribing to volume lifecycle events and resyncing on change,
  - resyncing periodically as a safety net for missed events.

  ## Server Lifecycle

  1. ExportManager starts, reads bind address from config
  2. Starts an `NFSServer.RPC.Server` listener bound to the BEAM
     NFSv3 + Mount handler programs.
  3. Mirrors the cluster export set (resync + events)
  4. On shutdown, stops the listener

  ## Export Model

  NFS uses a virtual root that lists all exported volumes as top-level
  directories. `NeonFS.NFS.MountBackend` reads the mirrored export set
  via `list_exports/0` and `get_export/1`.
  """

  use GenServer
  require Logger

  alias NeonFS.Client.Router
  alias NeonFS.Events.Envelope
  alias NeonFS.Events.{VolumeCreated, VolumeDeleted, VolumeUpdated}
  alias NeonFS.NFS.Application, as: NFSApp
  alias NeonFS.NFS.{ExportInfo, MetadataCache}
  alias NFSServer.Mount.Handler, as: MountHandler
  alias NFSServer.RPC.Server, as: RPCServer

  defmodule State do
    @moduledoc false
    defstruct [
      :nfs_server,
      :resync_timer,
      exports: %{}
    ]

    @type t :: %__MODULE__{
            nfs_server: pid() | nil,
            resync_timer: reference() | nil,
            exports: %{String.t() => ExportInfo.t()}
          }
  end

  ## Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Get the export for a volume name, if the volume is exported.
  """
  @spec get_export(String.t()) :: {:ok, ExportInfo.t()} | {:error, :not_found}
  def get_export(volume_name) do
    GenServer.call(__MODULE__, {:get_export, volume_name})
  end

  @doc """
  List all exported volumes.
  """
  @spec list_exports() :: [ExportInfo.t()]
  def list_exports do
    GenServer.call(__MODULE__, :list_exports)
  end

  ## GenServer Callbacks

  @impl true
  def init(_opts) do
    Logger.metadata(component: :nfs)

    state = %State{}

    # Start server in a continue callback so supervisor doesn't block
    {:ok, state, {:continue, :start_server}}
  end

  @impl true
  def handle_continue(:start_server, state) do
    case start_nfs_server(state) do
      {:ok, new_state} ->
        subscribe_volume_events()
        send(self(), :resync)
        {:noreply, new_state}

      {:error, reason} ->
        Logger.error("Failed to start NFS server", reason: inspect(reason))
        {:noreply, state}
    end
  end

  @impl true
  def handle_call({:get_export, volume_name}, _from, state) do
    case Map.fetch(state.exports, volume_name) do
      {:ok, export} -> {:reply, {:ok, export}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:list_exports, _from, state) do
    {:reply, Map.values(state.exports), state}
  end

  @impl true
  def handle_info(:resync, state), do: {:noreply, resync(state)}

  def handle_info({:neonfs_event, %Envelope{event: %event_mod{}}}, state)
      when event_mod in [VolumeCreated, VolumeUpdated, VolumeDeleted] do
    {:noreply, resync(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Private Helpers

  defp resync(state) do
    state
    |> cancel_resync_timer()
    |> apply_cluster_exports()
    |> schedule_resync()
  end

  defp apply_cluster_exports(state) do
    case fetch_cluster_exports() do
      {:ok, desired} ->
        reconcile_cache_subscriptions(state.exports, desired)
        emit_sync_telemetry(state.exports, desired)
        %{state | exports: desired}

      {:error, reason} ->
        Logger.debug("NFS export resync failed, will retry", reason: inspect(reason))
        state
    end
  end

  defp fetch_cluster_exports do
    case Router.call(NeonFS.Core, :list_volumes, []) do
      {:ok, volumes} ->
        exports =
          volumes
          |> Enum.filter(& &1.nfs_export)
          |> Map.new(fn volume ->
            {volume.name,
             ExportInfo.new(
               volume_id: volume.id,
               volume_name: volume.name,
               exported_at: volume.updated_at,
               allowed_ips: Map.get(volume, :nfs_allowed_ips, []),
               root_squash: Map.get(volume, :nfs_root_squash, true),
               write_ack: Map.get(volume, :write_ack, :local)
             )}
          end)

        {:ok, exports}

      {:error, reason} ->
        {:error, reason}
    end
  catch
    :exit, reason -> {:error, reason}
  end

  defp reconcile_cache_subscriptions(current, desired) do
    Enum.each(desired, fn {name, export} ->
      unless Map.has_key?(current, name) do
        Logger.info("Serving NFS export", volume_name: name)
        MetadataCache.subscribe_volume(name, export.volume_id)
      end
    end)

    Enum.each(current, fn {name, export} ->
      unless Map.has_key?(desired, name) do
        Logger.info("Stopped serving NFS export", volume_name: name)
        MetadataCache.unsubscribe_volume(name, export.volume_id)
      end
    end)
  end

  defp emit_sync_telemetry(current, desired) do
    :telemetry.execute(
      [:neonfs, :nfs, :exports, :synced],
      %{count: map_size(desired)},
      %{
        added: Map.keys(desired) -- Map.keys(current),
        removed: Map.keys(current) -- Map.keys(desired)
      }
    )
  end

  # The Events registry runs under the neonfs_client application
  # supervisor; without it (client children suppressed in unit tests)
  # the periodic resync alone keeps the mirror converging.
  defp subscribe_volume_events do
    NeonFS.Events.subscribe_volumes()
  rescue
    ArgumentError ->
      Logger.warning("Events registry unavailable; relying on periodic export resync")
  end

  defp cancel_resync_timer(%State{resync_timer: nil} = state), do: state

  defp cancel_resync_timer(%State{resync_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | resync_timer: nil}
  end

  defp schedule_resync(state) do
    interval = Application.get_env(:neonfs_nfs, :export_resync_interval, 60_000)
    %{state | resync_timer: Process.send_after(self(), :resync, interval)}
  end

  # Start the native-BEAM NFSv3 + Mount listener (`NFSServer.RPC.Server`).
  # Each TCP connection gets its own accept-loop process; the handler
  # modules dispatch statelessly from there. No per-connection
  # GenServer to monitor.
  defp start_nfs_server(state) do
    {ip, port} = parse_bind_address(nfs_bind_address())

    # 100_003 = NFSv3, 100_005 = MOUNT3. Portmapper auto-registers
    # inside `RPCServer.init/1`. The MOUNT backend is
    # `NeonFS.NFS.MountBackend`; the NFSv3 handler is the
    # backend-bound shim from `NFSApp.bound_nfsv3_handler/0`.
    programs = %{
      100_003 => %{3 => NFSApp.bound_nfsv3_handler()},
      100_005 => %{3 => MountHandler.with_backend(NeonFS.NFS.MountBackend)}
    }

    case RPCServer.start_link(
           bind: ip,
           port: port,
           programs: programs,
           drain_timeout: drain_deadline_ms(),
           name: NeonFS.NFS.RPCServer
         ) do
      {:ok, pid} ->
        Logger.info("NFS server started", bind_address: nfs_bind_address())
        {:ok, %{state | nfs_server: pid}}

      {:error, reason} ->
        Logger.error("Failed to start NFS server", reason: inspect(reason))
        {:error, {:nfs_bind_failed, reason}}
    end
  end

  # On shutdown the RPC listener lets in-flight RPCs settle for up to this
  # long before connection processes are killed (#1383). Default leaves
  # headroom under the systemd `TimeoutStopSec=45` budget (#1385).
  defp drain_deadline_ms do
    Application.get_env(:neonfs_nfs, :drain_deadline_ms, 25_000)
  end

  defp parse_bind_address(bind_address) do
    [host, port_str] = String.split(bind_address, ":", parts: 2)
    {host, String.to_integer(port_str)}
  end

  defp nfs_bind_address do
    host = Application.get_env(:neonfs_nfs, :bind_address, "127.0.0.1")
    port = Application.get_env(:neonfs_nfs, :port, 2049)
    "#{host}:#{port}"
  end
end
