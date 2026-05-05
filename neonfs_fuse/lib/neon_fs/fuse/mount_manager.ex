defmodule NeonFS.FUSE.MountManager do
  @moduledoc """
  Manages FUSE mount lifecycle.

  Coordinates mounting and unmounting volumes, tracking active mounts,
  and managing handler / session processes for each mount.

  ## Mount Lifecycle

  1. Validate mount point (exists, is directory, not already mounted)
  2. Verify volume exists in `VolumeRegistry`
  3. Start `MetadataCache` and `Handler` GenServers under the
     dynamic supervisor.
  4. Open the FUSE fd via `FuseServer.Fusermount.mount/2` (the
     `fusermount3` userspace helper) and start a
     `NeonFS.FUSE.Session` GenServer that owns the fd and dispatches
     incoming frames to the `Handler`.
  5. Track the mount metadata + the three pids (cache, handler,
     session) in state.
  6. Monitor the session process for crashes.

  Sub-issue #661 of #279 cut over from the legacy `fuser` NIF
  (`Native.mount/3`) to this BEAM-native stack.

  ## Crash Handling

  If the Session process crashes, the mount is cleaned up and removed
  from the active mounts list. The Handler is linked to the Session
  internally; both go down together when the Session terminates.
  """

  use GenServer
  require Logger

  alias FuseServer.Fusermount
  alias NeonFS.FUSE.{MetadataCache, MountInfo, MountSupervisor, Session}

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
         {:ok, volume} <- get_volume(volume_name),
         :ok <- check_mount_permission(volume, opts) do
      mount_filesystem(volume_name, volume.id, mount_point, opts, state)
    else
      {:error, reason} = error ->
        Logger.error("Failed to mount volume",
          volume_name: volume_name,
          mount_point: mount_point,
          reason: inspect(reason)
        )

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
    # Session process exited (clean unmount or crash). Clean up the
    # mount entry and ensure `fusermount3 -u` runs so the kernel
    # mountpoint goes away even on crashes.
    case find_mount_by_session(state, pid) do
      {:ok, mount_id, mount_info} ->
        Logger.warning("Session for mount exited",
          mount_id: mount_id,
          volume_name: mount_info.volume_name,
          reason: inspect(reason)
        )

        _ = Fusermount.unmount(mount_info.mount_point, lazy: true)

        if mount_info.cache_pid && Process.alive?(mount_info.cache_pid) do
          MountSupervisor.stop_cache(mount_info.cache_pid)
        end

        new_state = remove_mount(state, mount_id)
        {:noreply, new_state}

      :error ->
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
    case NeonFS.Client.core_call(NeonFS.Core.VolumeRegistry, :get_by_name, [volume_name]) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :volume_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp check_mount_permission(volume, opts) do
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    case NeonFS.Client.core_call(NeonFS.Core.Authorise, :check, [
           uid,
           gids,
           :mount,
           {:volume, volume.id}
         ]) do
      :ok -> :ok
      {:error, :forbidden} -> {:error, :forbidden}
      {:error, reason} -> {:error, reason}
    end
  end

  defp mount_filesystem(volume_name, volume_id, mount_point, opts, state) do
    normalized_path = Path.expand(mount_point)
    mount_id = generate_mount_id()

    cache_opts = [volume_id: volume_id]

    with {:ok, cache_pid} <- MountSupervisor.start_cache(cache_opts),
         cache_table <- MetadataCache.table(cache_pid, timeout: :infinity),
         fusermount_opts <- build_mount_options(opts),
         {:ok, fd} <- mount_via_fusermount(normalized_path, fusermount_opts),
         {:ok, session_pid} <-
           start_session(fd, volume_id, volume_name, cache_table, opts) do
      Process.monitor(session_pid)

      mount_info =
        create_mount_info(
          mount_id,
          volume_name,
          normalized_path,
          fd,
          session_pid,
          cache_pid
        )

      {:reply, {:ok, mount_id}, add_mount(state, mount_info)}
    else
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp mount_via_fusermount(path, fusermount_opts) do
    case Fusermount.mount(path, fusermount_opts) do
      {:ok, fd} ->
        {:ok, fd}

      {:error, :fusermount_no_fd} ->
        {:error, {:mount_failed, diagnose_fusermount_no_fd(path, current_uid())}}

      {:error, reason} ->
        {:error, {:mount_failed, reason}}
    end
  end

  # `:fusermount_no_fd` collapses several distinct causes (mount point
  # missing / not a directory / wrong owner / kernel-rejected option)
  # into one undifferentiated label. Stat the mount point to surface
  # which one fired so the operator gets an actionable error rather
  # than `{:mount_failed, :fusermount_no_fd}`. Issue #756.
  @doc false
  @spec diagnose_fusermount_no_fd(String.t(), non_neg_integer() | nil) ::
          {:fusermount_no_fd, String.t()}
  def diagnose_fusermount_no_fd(path, daemon_uid \\ current_uid()) do
    case File.stat(path) do
      {:error, :enoent} ->
        {:fusermount_no_fd, "Mount point does not exist: #{path}"}

      {:error, reason} ->
        {:fusermount_no_fd, "Cannot stat mount point #{path}: #{inspect(reason)}"}

      {:ok, %File.Stat{type: type}} when type != :directory ->
        {:fusermount_no_fd, "Mount point is not a directory (#{type}): #{path}"}

      {:ok, %File.Stat{uid: stat_uid}} ->
        case daemon_uid do
          uid when is_integer(uid) and uid != 0 and uid != stat_uid ->
            {:fusermount_no_fd,
             "Mount point must be owned by the daemon user " <>
               "(currently uid=#{stat_uid}, daemon uid=#{uid}); " <>
               "run: chown neonfs:neonfs #{path}"}

          _ ->
            {:fusermount_no_fd,
             "fusermount3 rejected the mount of #{path} — " <>
               "check daemon logs (journalctl -u neonfs-fuse) for stderr from the helper"}
        end
    end
  end

  # Linux: `/proc/self` is owned by the calling process's effective uid.
  # Returns `nil` if the proc filesystem isn't available (non-Linux,
  # restricted container) — callers fall back to the generic message.
  defp current_uid do
    case File.stat("/proc/self") do
      {:ok, %File.Stat{uid: uid}} -> uid
      _ -> nil
    end
  end

  defp start_session(fd, volume_id, volume_name, cache_table, opts) do
    session_opts = [
      fd: fd,
      volume: volume_id,
      volume_name: volume_name,
      cache_table: cache_table,
      atime_mode: Keyword.get(opts, :atime_mode, :noatime)
    ]

    case Session.start_link(session_opts) do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> {:error, {:session_start_failed, reason}}
    end
  end

  defp unmount_filesystem(mount_info) do
    # Stop the Session first — it owns the fd, which closes on
    # termination. The linked Handler exits with it. After the fd is
    # closed, ask `fusermount3 -u` to drop the kernel mount.
    if mount_info.session_pid && Process.alive?(mount_info.session_pid) do
      try do
        GenServer.stop(mount_info.session_pid, :shutdown, 5_000)
      catch
        :exit, _ -> :ok
      end
    end

    result =
      case Fusermount.unmount(mount_info.mount_point) do
        :ok -> :ok
        {:error, _} = error -> error
      end

    if mount_info.cache_pid && Process.alive?(mount_info.cache_pid) do
      MountSupervisor.stop_cache(mount_info.cache_pid)
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

  defp create_mount_info(
         mount_id,
         volume_name,
         mount_point,
         fd,
         session_pid,
         cache_pid
       ) do
    MountInfo.new(
      id: mount_id,
      volume_name: volume_name,
      mount_point: mount_point,
      started_at: DateTime.utc_now(),
      mount_session: fd,
      handler_pid: nil,
      session_pid: session_pid,
      cache_pid: cache_pid
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

  defp find_mount_by_session(state, session_pid) do
    Enum.find_value(state.mounts, :error, fn {mount_id, mount_info} ->
      if mount_info.session_pid == session_pid do
        {:ok, mount_id, mount_info}
      else
        false
      end
    end)
  end
end
