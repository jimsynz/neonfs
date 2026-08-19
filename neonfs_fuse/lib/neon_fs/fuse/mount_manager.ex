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
  4. Open the FUSE fd via `Wick.Fusermount.mount/2` (the
     `fusermount3` userspace helper) and start a
     `NeonFS.FUSE.Session` GenServer that owns the fd and dispatches
     incoming frames to the `Handler`.
  5. Track the mount metadata + the three pids (cache, handler,
     session) in state.
  6. Monitor the session process for crashes.

  Cut over from the legacy `fuser` NIF
  (`Native.mount/3`) to this BEAM-native stack.

  ## Crash Handling

  If the Session process crashes, the mount is cleaned up and removed
  from the active mounts list. The Handler is linked to the Session
  internally; both go down together when the Session terminates.

  ## Restart Recovery

  Every successful mount is recorded in `NeonFS.FUSE.MountRegistry`, and
  every explicit unmount removes its record. Nothing else touches the file, so
  a manager that dies — a crash, a `SIGKILL`, a DaemonSet rollout — comes back
  with a record of what it was serving and reconciles against it on boot:
  stale mountpoints are reaped and remounted, paths someone else is serving are
  left alone.

  Reconciliation retries, because a mount needs core and a restarting node
  usually has not discovered one yet. A record that still cannot be remounted
  when the attempts run out is dropped with an error rather than retried
  forever — leaving it would strand an entry no operator can clear, since
  `unmount/1` only knows about live mounts.
  """

  use GenServer
  require Logger

  alias NeonFS.FUSE.{MetadataCache, MountInfo, MountRecovery, MountRegistry, MountSupervisor, Session}
  alias Wick.Fusermount

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
  Tear down every live mount without forgetting it.

  For shutdown. An orderly stop has to drop the kernel mounts — leaving them
  behind strands a mountpoint whose server is about to vanish — but it does not
  mean the host has stopped being responsible for them, so the records stay and
  the next boot puts them back. `unmount/1` is the operator's "stop serving
  this", and that is the only thing that clears a record.
  """
  @spec detach_all() :: :ok
  def detach_all do
    GenServer.call(__MODULE__, :detach_all, 30_000)
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

  @doc """
  Get mount info by volume name.

  Returns the first matching mount when a volume has multiple mounts
  (operators typically have one). `{:error, :not_found}` otherwise.
  """
  @spec get_mount_by_volume_name(String.t()) :: {:ok, MountInfo.t()} | {:error, :not_found}
  def get_mount_by_volume_name(volume_name) do
    GenServer.call(__MODULE__, {:get_mount_by_volume_name, volume_name})
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    {:ok, %State{}, {:continue, :recover}}
  end

  @impl true
  def handle_continue(:recover, state) do
    {:noreply, recover(state, load_records(), 1)}
  end

  @impl true
  def handle_call({:mount, volume_name, mount_point, opts}, _from, state) do
    do_mount(volume_name, mount_point, opts, state)
  end

  @impl true
  def handle_call({:unmount, mount_id}, _from, state) do
    case Map.fetch(state.mounts, mount_id) do
      {:ok, mount_info} ->
        # The state entry is dropped unconditionally — leaving it
        # behind on a `fusermount3 -u` failure strands the operator
        # with `:already_mounted` for every retry and no API to clear
        # it short of restarting the daemon. Bookkeeping
        # follows kernel reality on a best-effort basis; the actual
        # unmount result is still reported to the caller.
        unmount_result = unmount_filesystem(mount_info)

        if match?({:error, _}, unmount_result) do
          Logger.warning("fusermount3 -u failed; dropping mount-manager entry anyway",
            mount_id: mount_id,
            mount_point: mount_info.mount_point,
            reason: inspect(elem(unmount_result, 1))
          )
        end

        new_state = remove_mount(state, mount_id)
        record_mounts(new_state)

        {:reply, unmount_result, new_state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:detach_all, _from, state) do
    Enum.each(Map.values(state.mounts), &unmount_filesystem/1)
    {:reply, :ok, %State{}}
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
  def handle_call({:get_mount_by_volume_name, volume_name}, _from, state) do
    match =
      Enum.find_value(state.mounts, fn {_id, mount_info} ->
        if mount_info.volume_name == volume_name, do: mount_info, else: nil
      end)

    case match do
      nil -> {:reply, {:error, :not_found}, state}
      mount_info -> {:reply, {:ok, mount_info}, state}
    end
  end

  @impl true
  def handle_info({:recover, records, attempt}, state) do
    {:noreply, recover(state, records, attempt)}
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

  # Shared by the `:mount` call and by restart recovery: a remount has to clear
  # exactly the same checks as a fresh one — the volume may have been deleted,
  # converted to a block volume, or had its permissions changed while this node
  # was down.
  defp do_mount(volume_name, mount_point, opts, state) do
    with :ok <- validate_mount_point(mount_point),
         :ok <- check_not_mounted(mount_point, state),
         {:ok, volume} <- get_volume(volume_name),
         :ok <- check_mountable(volume),
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

  # Recovery runs against the records rather than against live state, so an
  # entry survives its own failed attempts and is retried with the rest.
  defp recover(state, [], _attempt), do: state

  defp recover(state, records, attempt) do
    {recovered, outstanding} =
      Enum.reduce(records, {state, []}, fn record, {acc_state, retry} ->
        case recover_one(acc_state, record) do
          {:ok, next_state} -> {next_state, retry}
          :retry -> {acc_state, [record | retry]}
        end
      end)

    record_mounts(recovered)
    schedule_retry(Enum.reverse(outstanding), attempt)
    recovered
  end

  defp recover_one(state, record) do
    case MountRecovery.classify(record.mount_point) do
      :stale ->
        MountRecovery.reap(record.mount_point)
        remount(state, record)

      :vacant ->
        remount(state, record)

      :serving ->
        Logger.warning("Leaving a recorded mount point that something else is serving",
          mount_point: record.mount_point,
          volume_name: record.volume_name
        )

        {:ok, state}

      :missing ->
        Logger.error("Recorded mount point is gone; dropping the record",
          mount_point: record.mount_point,
          volume_name: record.volume_name
        )

        {:ok, state}
    end
  end

  defp remount(state, record) do
    case do_mount(record.volume_name, record.mount_point, record.opts, state) do
      {:reply, {:ok, mount_id}, new_state} ->
        Logger.info("Remounted volume after restart",
          mount_id: mount_id,
          mount_point: record.mount_point,
          volume_name: record.volume_name
        )

        :telemetry.execute(
          [:neonfs, :fuse, :mount_recovery, :remounted],
          %{},
          %{mount_point: record.mount_point, volume_name: record.volume_name}
        )

        {:ok, new_state}

      {:reply, {:error, reason}, _unchanged} ->
        Logger.warning("Could not remount volume after restart",
          mount_point: record.mount_point,
          volume_name: record.volume_name,
          reason: inspect(reason)
        )

        :retry
    end
  end

  # A restarting node usually has not found a core node yet, so the first
  # attempts fail on discovery rather than on anything about the mount.
  defp schedule_retry([], _attempt) do
    :telemetry.execute([:neonfs, :fuse, :mount_recovery, :settled], %{}, %{})
  end

  defp schedule_retry(records, attempt) do
    if attempt < recovery_attempts() do
      Process.send_after(self(), {:recover, records, attempt + 1}, recovery_backoff())
    else
      Enum.each(records, fn record ->
        Logger.error("Giving up remounting volume after restart; dropping the record",
          mount_point: record.mount_point,
          volume_name: record.volume_name,
          attempt: attempt
        )
      end)

      :telemetry.execute(
        [:neonfs, :fuse, :mount_recovery, :abandoned],
        %{count: length(records)},
        %{}
      )
    end
  end

  defp load_records do
    case MountRegistry.load() do
      {:ok, records} ->
        records

      {:error, reason} ->
        Logger.error("Could not read the mount registry; no mounts will be recovered",
          reason: inspect(reason)
        )

        []
    end
  end

  # The registry is written from live state, so it always describes mounts this
  # manager is serving. Records still awaiting a retry are held in the retry
  # message rather than the file: a record that outlives its attempts should
  # not survive a second restart to be attempted forever.
  defp record_mounts(state) do
    entries = state.mounts |> Map.values() |> Enum.map(&MountRegistry.entry/1)

    case MountRegistry.save(entries) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("Could not record mounts; a restart will not recover them",
          reason: inspect(reason)
        )
    end
  end

  defp recovery_attempts, do: Application.get_env(:neonfs_fuse, :mount_recovery_attempts, 10)

  defp recovery_backoff, do: Application.get_env(:neonfs_fuse, :mount_recovery_backoff_ms, 5_000)

  # The path is checked on this FUSE node's own filesystem, so the node name
  # is part of the error: an operator who created the directory on a different
  # host otherwise sees "not found" for a path that is plainly right there.
  defp validate_mount_point(mount_point) do
    expanded = Path.expand(mount_point)

    cond do
      not File.exists?(expanded) ->
        {:error, "mount point #{expanded} not found on FUSE node #{Node.self()}"}

      not File.dir?(expanded) ->
        {:error, "mount point #{expanded} is not a directory on FUSE node #{Node.self()}"}

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

  # A block volume holds one device rather than a namespace of files, so
  # there is nothing for FUSE to present. `neonfs_block` serves it.
  defp check_mountable(%{type: :block}), do: {:error, :not_a_filesystem_volume}
  defp check_mountable(_volume), do: :ok

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
      {:error, %{class: :forbidden}} = err -> err
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
          cache_pid,
          opts
        )

      new_state = add_mount(state, mount_info)
      record_mounts(new_state)

      {:reply, {:ok, mount_id}, new_state}
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
  # than `{:mount_failed, :fusermount_no_fd}`.
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
      {:ok, pid} ->
        # Decouple the session from the manager: `mount_filesystem/5` monitors
        # it, so MountManager learns of its death via `:DOWN`. Leaving them
        # linked meant `GenServer.stop(session, :shutdown)` on unmount (or a
        # session crash) propagated the exit and took the manager down with it —
        # wedging the whole FUSE control interface until a daemon restart.
        Process.unlink(pid)
        {:ok, pid}

      {:error, reason} ->
        {:error, {:session_start_failed, reason}}
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

  @doc false
  @spec build_mount_options(keyword()) :: [String.t()]
  def build_mount_options(opts) do
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
         cache_pid,
         opts
       ) do
    MountInfo.new(
      id: mount_id,
      volume_name: volume_name,
      mount_point: mount_point,
      started_at: DateTime.utc_now(),
      mount_session: fd,
      handler_pid: nil,
      session_pid: session_pid,
      cache_pid: cache_pid,
      opts: opts
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
