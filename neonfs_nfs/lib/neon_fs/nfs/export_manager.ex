defmodule NeonFS.NFS.ExportManager do
  @moduledoc """
  Manages NFS server lifecycle and volume exports.

  Coordinates:
  - Starting/stopping the NFS TCP listener (via Rust NIF)
  - Starting the Handler GenServer under ExportSupervisor
  - Tracking which volumes are exported (available via NFS)
  - Auto-exporting configured volumes on startup

  ## Server Lifecycle

  1. ExportManager starts, reads bind address from config
  2. Starts Handler GenServer under ExportSupervisor
  3. Calls `Native.start_nfs_server/2` with handler PID
  4. Auto-exports configured volumes
  5. On shutdown, stops NFS server and handler

  ## Export Model

  NFS uses a virtual root that lists all exported volumes as top-level
  directories. Exporting a volume registers it in the handler's volume
  mapping so clients can access it. Unexporting removes the mapping.
  """

  use GenServer
  require Logger

  alias NeonFS.NFS.{ExportInfo, ExportSupervisor, Handler, MetadataCache, Native}

  defmodule State do
    @moduledoc false
    defstruct [
      :nfs_server,
      :handler_pid,
      exports: %{}
    ]

    @type t :: %__MODULE__{
            nfs_server: reference() | nil,
            handler_pid: pid() | nil,
            exports: %{String.t() => ExportInfo.t()}
          }
  end

  ## Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Export a volume, making it available via NFS.

  The volume will appear as a directory under the NFS virtual root.
  """
  @spec export(String.t()) :: {:ok, String.t()} | {:error, term()}
  def export(volume_name) do
    GenServer.call(__MODULE__, {:export, volume_name})
  end

  @doc """
  Unexport a volume, removing it from NFS.
  """
  @spec unexport(String.t()) :: :ok | {:error, term()}
  def unexport(export_id) do
    GenServer.call(__MODULE__, {:unexport, export_id})
  end

  @doc """
  List all exported volumes.
  """
  @spec list_exports() :: [ExportInfo.t()]
  def list_exports do
    GenServer.call(__MODULE__, :list_exports)
  end

  @doc """
  Get the handler PID (for testing/debugging).
  """
  @spec handler_pid() :: pid() | nil
  def handler_pid do
    GenServer.call(__MODULE__, :handler_pid)
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
        auto_export_volumes(new_state)
        {:noreply, new_state}

      {:error, reason} ->
        Logger.error("Failed to start NFS server", reason: inspect(reason))
        {:noreply, state}
    end
  end

  @impl true
  def handle_call({:export, volume_name}, _from, state) do
    if Map.has_key?(state.exports, volume_name) do
      existing = Map.get(state.exports, volume_name)
      {:reply, {:ok, existing.id}, state}
    else
      export_id = generate_export_id()

      export_info =
        ExportInfo.new(
          id: export_id,
          volume_name: volume_name,
          exported_at: DateTime.utc_now()
        )

      new_exports = Map.put(state.exports, volume_name, export_info)
      new_state = %{state | exports: new_exports}

      subscribe_cache_events(volume_name)

      Logger.info("Exported volume via NFS", volume_name: volume_name, export_id: export_id)

      {:reply, {:ok, export_id}, new_state}
    end
  end

  @impl true
  def handle_call({:unexport, export_id}, _from, state) do
    case find_export_by_id(state.exports, export_id) do
      {volume_name, _export_info} ->
        new_exports = Map.delete(state.exports, volume_name)
        new_state = %{state | exports: new_exports}

        Logger.info("Unexported volume from NFS",
          volume_name: volume_name,
          export_id: export_id
        )

        {:reply, :ok, new_state}

      nil ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:list_exports, _from, state) do
    {:reply, Map.values(state.exports), state}
  end

  @impl true
  def handle_call(:handler_pid, _from, state) do
    {:reply, state.handler_pid, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    if pid == state.handler_pid do
      Logger.warning("NFS handler crashed, restarting", reason: inspect(reason))

      case restart_handler(state) do
        {:ok, new_state} ->
          {:noreply, new_state}

        {:error, _reason} ->
          {:noreply, %{state | handler_pid: nil}}
      end
    else
      {:noreply, state}
    end
  end

  ## Private Helpers

  defp start_nfs_server(state) do
    bind_address = nfs_bind_address()

    with {:ok, handler_pid} <- ExportSupervisor.start_handler([]),
         _ <- Process.monitor(handler_pid),
         {:ok, nfs_server} <- start_native_server(bind_address, handler_pid),
         :ok <- Handler.set_nfs_server(handler_pid, nfs_server) do
      Logger.info("NFS server started", bind_address: bind_address)

      {:ok, %{state | nfs_server: nfs_server, handler_pid: handler_pid}}
    else
      {:error, reason} ->
        Logger.error("Failed to start NFS server", reason: inspect(reason))
        {:error, reason}
    end
  end

  defp start_native_server(bind_address, handler_pid) do
    case Native.start_nfs_server(bind_address, handler_pid) do
      {:ok, server} -> {:ok, server}
      {:error, reason} -> {:error, {:nfs_bind_failed, reason}}
    end
  rescue
    e in ArgumentError ->
      {:error, {:nfs_nif_error, Exception.message(e)}}
  catch
    :error, :nif_not_loaded ->
      {:error, :nif_not_loaded}
  end

  defp restart_handler(state) do
    case ExportSupervisor.start_handler([]) do
      {:ok, handler_pid} ->
        Process.monitor(handler_pid)

        if state.nfs_server do
          Handler.set_nfs_server(handler_pid, state.nfs_server)
        end

        {:ok, %{state | handler_pid: handler_pid}}

      {:error, reason} ->
        Logger.error("Failed to restart NFS handler", reason: inspect(reason))
        {:error, reason}
    end
  end

  defp auto_export_volumes(_state) do
    volumes = Application.get_env(:neonfs_nfs, :auto_export_volumes, [])

    Enum.each(volumes, fn volume_name ->
      case GenServer.call(self(), {:export, volume_name}) do
        {:ok, _id} ->
          :ok

        {:error, reason} ->
          Logger.warning("Failed to auto-export volume",
            volume_name: volume_name,
            reason: inspect(reason)
          )
      end
    end)
  end

  defp subscribe_cache_events(volume_name) do
    case NeonFS.Client.core_call(NeonFS.Core.VolumeRegistry, :get_by_name, [volume_name]) do
      {:ok, volume} ->
        MetadataCache.subscribe_volume(volume_name, volume.id)

      {:error, reason} ->
        Logger.warning("Could not subscribe to cache events for volume",
          volume_name: volume_name,
          reason: inspect(reason)
        )
    end
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp nfs_bind_address do
    host = Application.get_env(:neonfs_nfs, :bind_address, "0.0.0.0")
    port = Application.get_env(:neonfs_nfs, :port, 2049)
    "#{host}:#{port}"
  end

  defp generate_export_id do
    "export_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end

  defp find_export_by_id(exports, export_id) do
    Enum.find_value(exports, fn {volume_name, export_info} ->
      if export_info.id == export_id do
        {volume_name, export_info}
      end
    end)
  end
end
