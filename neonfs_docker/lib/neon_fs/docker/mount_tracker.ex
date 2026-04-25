defmodule NeonFS.Docker.MountTracker do
  @moduledoc """
  Ref-counted mount registry for the Docker VolumeDriver plugin.

  Docker calls `VolumeDriver.Mount` once per container using a volume
  and `VolumeDriver.Unmount` once per stop. Multiple containers on the
  same host can share a single FUSE mount, so the tracker counts
  references per volume: the first `mount/2` call invokes `mount_fn`,
  subsequent calls just bump the count and return the existing mount
  point; the final `unmount/2` invokes `unmount_fn`.

  On shutdown any still-tracked mounts are unmounted via `unmount_fn`
  so containers don't hit stale `ENODEV`.

  ## Injectable dependencies

    * `:mount_fn` — 1-arity function `fn volume_name ->
      {:ok, {mount_id, mount_point}} | {:error, term()}`. The function
      is responsible for creating the mount point directory and driving
      `NeonFS.FUSE.MountManager`. Default dispatches to the configured
      FUSE node via `GenServer.call({MountManager, fuse_node}, …)`.
    * `:unmount_fn` — 1-arity function `fn mount_id -> :ok | {:error, term()}`.
      Default tears down the FUSE mount via `NeonFS.FUSE.MountManager`.

  Application env keys used by the defaults:

    * `:mount_root` — directory under which per-volume mount points are
      created (default `"/var/lib/neonfs-docker/mounts"`).
    * `:fuse_node` — Erlang node hosting `NeonFS.FUSE.MountManager`
      (default `Node.self/0`, i.e. co-located).
  """

  use GenServer

  @type volume_name :: String.t()
  @type mount_id :: term()
  @type mount_point :: String.t()
  @type mount_fn :: (volume_name() -> {:ok, {mount_id(), mount_point()}} | {:error, term()})
  @type unmount_fn :: (mount_id() -> :ok | {:error, term()})
  @type record :: %{mount_id: mount_id(), mount_point: mount_point(), ref_count: pos_integer()}

  @default_mount_root "/var/lib/neonfs-docker/mounts"

  ## Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Increment the ref count for `volume_name`, invoking `mount_fn` on the
  first reference. Returns the current mount point.
  """
  @spec mount(GenServer.server(), volume_name()) ::
          {:ok, mount_point()} | {:error, term()}
  def mount(server \\ __MODULE__, volume_name) when is_binary(volume_name) do
    GenServer.call(server, {:mount, volume_name})
  end

  @doc """
  Decrement the ref count for `volume_name`, invoking `unmount_fn` when
  the count reaches zero. Returns `:ok` for unknown volumes — Docker
  may replay `Unmount` after a plugin restart.
  """
  @spec unmount(GenServer.server(), volume_name()) :: :ok | {:error, term()}
  def unmount(server \\ __MODULE__, volume_name) when is_binary(volume_name) do
    GenServer.call(server, {:unmount, volume_name})
  end

  @doc """
  Return the current mount point for `volume_name`, or `""` if the
  volume isn't mounted. Matches Docker's `Path` convention where an
  empty string signals "defined but not mounted".
  """
  @spec mountpoint_for(GenServer.server(), volume_name()) :: mount_point()
  def mountpoint_for(server \\ __MODULE__, volume_name) when is_binary(volume_name) do
    GenServer.call(server, {:mountpoint_for, volume_name})
  end

  @doc """
  List every tracked mount as `{volume_name, record}` pairs. Used for
  operator introspection and tests.
  """
  @spec list(GenServer.server()) :: [{volume_name(), record()}]
  def list(server \\ __MODULE__) do
    GenServer.call(server, :list)
  end

  @doc """
  Return the current mount-pool capacity as `{used, max}` where `max`
  is `nil` when no cap is configured. Used by the `:mount_capacity`
  health check to surface "pool full" without poking GenServer state
  directly.
  """
  @spec capacity(GenServer.server()) :: %{
          required(:used) => non_neg_integer(),
          required(:max) => non_neg_integer() | nil
        }
  def capacity(server \\ __MODULE__) do
    GenServer.call(server, :capacity)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    {:ok,
     %{
       mounts: %{},
       mount_fn: Keyword.get(opts, :mount_fn, &default_mount_fn/1),
       unmount_fn: Keyword.get(opts, :unmount_fn, &default_unmount_fn/1),
       max_mounts: Keyword.get_lazy(opts, :max_mounts, &configured_max_mounts/0)
     }}
  end

  @impl true
  def handle_call({:mount, volume_name}, _from, state) do
    case Map.fetch(state.mounts, volume_name) do
      {:ok, record} ->
        # Already mounted — refcount bumps don't count against the
        # cap because they don't allocate a new FUSE mount.
        updated = %{record | ref_count: record.ref_count + 1}

        {:reply, {:ok, record.mount_point},
         %{state | mounts: Map.put(state.mounts, volume_name, updated)}}

      :error ->
        if at_capacity?(state) do
          {:reply, {:error, :mount_pool_full}, state}
        else
          do_first_mount(volume_name, state)
        end
    end
  end

  def handle_call({:unmount, volume_name}, _from, state) do
    case Map.fetch(state.mounts, volume_name) do
      {:ok, %{ref_count: 1} = record} ->
        case state.unmount_fn.(record.mount_id) do
          :ok ->
            {:reply, :ok, %{state | mounts: Map.delete(state.mounts, volume_name)}}

          {:error, _} = err ->
            {:reply, err, state}
        end

      {:ok, record} ->
        updated = %{record | ref_count: record.ref_count - 1}
        {:reply, :ok, %{state | mounts: Map.put(state.mounts, volume_name, updated)}}

      :error ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:mountpoint_for, volume_name}, _from, state) do
    mountpoint =
      case Map.fetch(state.mounts, volume_name) do
        {:ok, %{mount_point: mp}} -> mp
        :error -> ""
      end

    {:reply, mountpoint, state}
  end

  def handle_call(:list, _from, state) do
    {:reply, Map.to_list(state.mounts), state}
  end

  def handle_call(:capacity, _from, state) do
    {:reply, %{used: map_size(state.mounts), max: state.max_mounts}, state}
  end

  defp do_first_mount(volume_name, state) do
    case state.mount_fn.(volume_name) do
      {:ok, {mount_id, mount_point}} ->
        record = %{mount_id: mount_id, mount_point: mount_point, ref_count: 1}

        {:reply, {:ok, mount_point},
         %{state | mounts: Map.put(state.mounts, volume_name, record)}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp at_capacity?(%{max_mounts: nil}), do: false
  defp at_capacity?(%{max_mounts: max, mounts: mounts}), do: map_size(mounts) >= max

  defp configured_max_mounts do
    Application.get_env(:neonfs_docker, :max_mounts, nil)
  end

  @impl true
  def terminate(_reason, state) do
    for {_name, %{mount_id: mid}} <- state.mounts do
      state.unmount_fn.(mid)
    end

    :ok
  end

  ## Default FUSE wiring

  defp default_mount_fn(volume_name) do
    mount_root = Application.get_env(:neonfs_docker, :mount_root, @default_mount_root)
    fuse_node = Application.get_env(:neonfs_docker, :fuse_node, Node.self())
    mount_point = Path.join(mount_root, volume_name)

    with :ok <- File.mkdir_p(mount_point),
         {:ok, mount_id} <-
           GenServer.call(
             {NeonFS.FUSE.MountManager, fuse_node},
             {:mount, volume_name, mount_point, []}
           ) do
      {:ok, {mount_id, mount_point}}
    end
  end

  defp default_unmount_fn(mount_id) do
    fuse_node = Application.get_env(:neonfs_docker, :fuse_node, Node.self())
    GenServer.call({NeonFS.FUSE.MountManager, fuse_node}, {:unmount, mount_id})
  end
end
