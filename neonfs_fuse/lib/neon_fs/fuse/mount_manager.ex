defmodule NeonFS.FUSE.MountManager do
  @moduledoc """
  Manages FUSE mount lifecycle.

  Coordinates mounting and unmounting volumes, tracking active mounts,
  and managing handler processes for each mount.

  ## Mount Lifecycle

  1. Validate mount point (exists, is directory, not already mounted)
  2. Verify volume exists in VolumeRegistry
  3. Start Handler GenServer for this mount
  4. Call Native.mount/3 with handler PID
  5. Track mount info in state
  6. Monitor handler process for crashes

  ## Crash Handling

  If a handler process crashes, the mount is automatically cleaned up
  and removed from the active mounts list.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.FUSE.{MountInfo, MountSupervisor, Native}

  @type mount_id :: String.t()

  defmodule State do
    @moduledoc false
    defstruct mounts: %{}, mount_points: %{}

    @type t :: %__MODULE__{
            mounts: %{String.t() => MountInfo.t()},
            mount_points: %{String.t() => String.t()}
          }
  end

  ## Client API

  @doc """
  Start the mount manager GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Mount a volume at the specified mount point.

  ## Options
  - `:auto_unmount` - Automatically unmount when process exits (default: true)
  - `:allow_other` - Allow other users to access (default: false)
  - `:allow_root` - Allow root to access (default: false)
  - `:ro` - Mount read-only (default: false)

  Returns `{:ok, mount_id}` on success.
  """
  @spec mount(String.t(), String.t(), keyword()) ::
          {:ok, mount_id()} | {:error, term()}
  def mount(volume_name, mount_point, opts \\ []) do
    GenServer.call(__MODULE__, {:mount, volume_name, mount_point, opts})
  end

  @doc """
  Unmount a mounted volume.

  Returns `:ok` on success.
  """
  @spec unmount(mount_id()) :: :ok | {:error, term()}
  def unmount(mount_id) do
    GenServer.call(__MODULE__, {:unmount, mount_id})
  end

  @doc """
  List all active mounts.

  Returns a list of MountInfo structs.
  """
  @spec list_mounts() :: [MountInfo.t()]
  def list_mounts do
    GenServer.call(__MODULE__, :list_mounts)
  end

  @doc """
  Get mount info by mount ID.

  Returns `{:ok, mount_info}` if found, `{:error, :not_found}` otherwise.
  """
  @spec get_mount(mount_id()) :: {:ok, MountInfo.t()} | {:error, :not_found}
  def get_mount(mount_id) do
    GenServer.call(__MODULE__, {:get_mount, mount_id})
  end

  @doc """
  Get mount info by mount point path.

  Returns `{:ok, mount_info}` if found, `{:error, :not_found}` otherwise.
  """
  @spec get_mount_by_path(String.t()) :: {:ok, MountInfo.t()} | {:error, :not_found}
  def get_mount_by_path(mount_point) do
    GenServer.call(__MODULE__, {:get_mount_by_path, mount_point})
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    {:ok, %State{}}
  end

  @impl true
  def handle_call({:mount, volume_name, mount_point, opts}, _from, state) do
    with :ok <- validate_mount_point(mount_point),
         :ok <- check_not_mounted(mount_point, state),
         {:ok, volume} <- get_volume(volume_name) do
      mount_filesystem(volume_name, volume.id, mount_point, opts, state)
    else
      {:error, reason} = error ->
        Logger.error("Failed to mount #{volume_name} at #{mount_point}: #{inspect(reason)}")
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:unmount, mount_id}, _from, state) do
    case Map.fetch(state.mounts, mount_id) do
      {:ok, mount_info} ->
        case unmount_filesystem(mount_info) do
          :ok ->
            new_state = remove_mount(state, mount_id)
            {:reply, :ok, new_state}

          {:error, _reason} = error ->
            {:reply, error, state}
        end

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:list_mounts, _from, state) do
    mounts = Map.values(state.mounts)
    {:reply, mounts, state}
  end

  @impl true
  def handle_call({:get_mount, mount_id}, _from, state) do
    case Map.fetch(state.mounts, mount_id) do
      {:ok, mount_info} -> {:reply, {:ok, mount_info}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:get_mount_by_path, mount_point}, _from, state) do
    normalized = Path.expand(mount_point)

    case Map.fetch(state.mount_points, normalized) do
      {:ok, mount_id} ->
        mount_info = Map.fetch!(state.mounts, mount_id)
        {:reply, {:ok, mount_info}, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    # Handler process crashed, clean up the mount
    case find_mount_by_handler(state, pid) do
      {:ok, mount_id, mount_info} ->
        Logger.warning(
          "Handler for mount #{mount_id} (#{mount_info.volume_name}) crashed: #{inspect(reason)}"
        )

        # Try to unmount the filesystem
        _ = Native.unmount(mount_info.mount_session)
        new_state = remove_mount(state, mount_id)
        {:noreply, new_state}

      :error ->
        # Unknown handler crashed, ignore
        {:noreply, state}
    end
  end

  ## Private Helpers

  defp validate_mount_point(mount_point) do
    expanded = Path.expand(mount_point)

    cond do
      not File.exists?(expanded) ->
        {:error, :mount_point_not_found}

      not File.dir?(expanded) ->
        {:error, :mount_point_not_directory}

      true ->
        :ok
    end
  end

  defp check_not_mounted(mount_point, state) do
    normalized = Path.expand(mount_point)

    if Map.has_key?(state.mount_points, normalized) do
      {:error, :already_mounted}
    else
      :ok
    end
  end

  defp get_volume(volume_name) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :volume_not_found}
    end
  end

  defp mount_filesystem(volume_name, volume_id, mount_point, opts, state) do
    normalized_path = Path.expand(mount_point)
    mount_id = generate_mount_id()

    # Start the handler process for this mount under the DynamicSupervisor
    handler_opts = [
      volume: volume_id,
      mount_id: mount_id
    ]

    case MountSupervisor.start_handler(handler_opts) do
      {:ok, handler_pid} ->
        # Monitor the handler process
        Process.monitor(handler_pid)

        # Mount the filesystem via NIF
        mount_opts = build_mount_options(opts)

        try do
          case Native.mount(normalized_path, handler_pid, mount_opts) do
            {:ok, mount_session} ->
              mount_info =
                create_mount_info(
                  mount_id,
                  volume_name,
                  normalized_path,
                  mount_session,
                  handler_pid
                )

              new_state = add_mount(state, mount_info)
              {:reply, {:ok, mount_id}, new_state}

            {:error, reason} ->
              # Mount failed, stop the handler via supervisor
              MountSupervisor.stop_handler(handler_pid)
              {:reply, {:error, {:mount_failed, reason}}, state}
          end
        catch
          :error, :nif_not_loaded ->
            # FUSE NIF not available (not compiled or system libraries missing)
            MountSupervisor.stop_handler(handler_pid)
            {:reply, {:error, {:mount_failed, "FUSE NIF not loaded"}}, state}
        end

      {:error, reason} ->
        {:reply, {:error, {:handler_start_failed, reason}}, state}
    end
  end

  defp unmount_filesystem(mount_info) do
    # Unmount the filesystem first
    result = Native.unmount(mount_info.mount_session)

    # Stop the handler via supervisor (if still alive)
    if Process.alive?(mount_info.handler_pid) do
      MountSupervisor.stop_handler(mount_info.handler_pid)
    end

    result
  end

  defp generate_mount_id do
    "mount_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end

  defp build_mount_options(opts) do
    mount_opts = []

    mount_opts =
      if Keyword.get(opts, :auto_unmount, true) do
        ["auto_unmount" | mount_opts]
      else
        mount_opts
      end

    mount_opts =
      if Keyword.get(opts, :allow_other, false) do
        ["allow_other" | mount_opts]
      else
        mount_opts
      end

    mount_opts =
      if Keyword.get(opts, :allow_root, false) do
        ["allow_root" | mount_opts]
      else
        mount_opts
      end

    mount_opts =
      if Keyword.get(opts, :ro, false) do
        ["ro" | mount_opts]
      else
        mount_opts
      end

    mount_opts
  end

  defp create_mount_info(mount_id, volume_name, mount_point, mount_session, handler_pid) do
    MountInfo.new(
      id: mount_id,
      volume_name: volume_name,
      mount_point: mount_point,
      started_at: DateTime.utc_now(),
      mount_session: mount_session,
      handler_pid: handler_pid
    )
  end

  defp add_mount(state, mount_info) do
    %State{
      mounts: Map.put(state.mounts, mount_info.id, mount_info),
      mount_points: Map.put(state.mount_points, mount_info.mount_point, mount_info.id)
    }
  end

  defp remove_mount(state, mount_id) do
    case Map.fetch(state.mounts, mount_id) do
      {:ok, mount_info} ->
        %State{
          mounts: Map.delete(state.mounts, mount_id),
          mount_points: Map.delete(state.mount_points, mount_info.mount_point)
        }

      :error ->
        state
    end
  end

  defp find_mount_by_handler(state, handler_pid) do
    Enum.find_value(state.mounts, :error, fn {mount_id, mount_info} ->
      if mount_info.handler_pid == handler_pid do
        {:ok, mount_id, mount_info}
      else
        false
      end
    end)
  end
end
