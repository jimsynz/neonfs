defmodule NeonFS.NFS.Handler do
  @moduledoc """
  Handles NFS operations from the Rust NIF layer.

  Receives NFS operation messages from the Rust NIF in the format:
  `{:nfs_op, request_id, {operation_name, params}}`

  Dispatches operations to the appropriate NeonFS.Core modules via
  `NeonFS.Client.core_call/3` and replies via the NIF with results or errors.

  ## Volume Routing

  NFS file handles embed a 16-byte volume ID (MD5 hash of the volume name).
  The handler maintains a bidirectional mapping between volume names and their
  hash IDs. A null volume ID (all zeros) represents the synthetic virtual root
  that lists available volumes.

  ## Operation Flow

  1. Rust NFS layer receives NFSv3 operation
  2. Operation is sent as message to this handler
  3. Handler resolves volume from file handle, dispatches to NeonFS.Core
  4. Handler constructs reply map and sends back via NIF
  5. Rust layer converts reply to NFSv3 response
  """

  use GenServer
  import Bitwise
  require Logger

  alias NeonFS.NFS.{InodeTable, MetadataCache, Native}

  @null_volume_id <<0::128>>

  # POSIX file type bits
  @s_ifdir 0o040000
  @s_ifreg 0o100000
  @s_iflnk 0o120000
  @s_ifmt 0o170000

  ## Client API

  @doc """
  Start the NFS handler GenServer.

  Options:
  - `:nfs_server` - Server resource for replying to NFS operations
  - `:name` - Optional name for registration
  - `:test_notify` - PID to notify when operations complete (test only)
  - `:core_call_fn` - Optional `fn module, function, args -> result` override for testing
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @doc """
  Set the NFS server resource on a running handler.

  Called by ExportManager after the NIF server is started, since the handler
  must exist before the NIF (which needs the handler PID), but the handler
  needs the server resource to send replies.
  """
  @spec set_nfs_server(GenServer.server(), reference()) :: :ok
  def set_nfs_server(handler, nfs_server) do
    GenServer.call(handler, {:set_nfs_server, nfs_server})
  end

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    Logger.metadata(component: :nfs)

    cache_table =
      case Keyword.get(opts, :cache_table) do
        nil -> safe_get_cache_table()
        table -> table
      end

    if core_call_fn = Keyword.get(opts, :core_call_fn) do
      Process.put(:core_call_fn, core_call_fn)
    end

    Logger.info("NFS Handler started")

    {:ok,
     %{
       nfs_server: Keyword.get(opts, :nfs_server),
       cache_table: cache_table,
       volume_ids: %{},
       volume_hashes: %{},
       test_notify: Keyword.get(opts, :test_notify)
     }}
  end

  defp safe_get_cache_table do
    MetadataCache.table()
  catch
    :exit, _ -> nil
  end

  @impl true
  def handle_call({:set_nfs_server, nfs_server}, _from, state) do
    Logger.info("NFS server resource set on handler")
    {:reply, :ok, %{state | nfs_server: nfs_server}}
  end

  @impl true
  def handle_info({:nfs_op, request_id, {op_name, params}}, state) do
    Logger.metadata(request_id: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
    Logger.debug("Received NFS operation", operation: op_name)

    {reply, duration, state} =
      try do
        timed_dispatch(op_name, params, state)
      rescue
        e ->
          Logger.error(
            "Dispatch crashed: #{op_name}: #{Exception.message(e)}\n#{Exception.format_stacktrace(__STACKTRACE__)}"
          )

          {{:error, errno(:eio)}, 0, state}
      end

    emit_telemetry(op_name, reply, duration)

    if state.nfs_server do
      case Native.reply_nfs_operation(state.nfs_server, request_id, reply) do
        {:ok, {}} ->
          Logger.debug("Reply sent", request_id: request_id)

        {:error, reason} ->
          Logger.error("Reply failed",
            request_id: request_id,
            operation: op_name,
            reason: inspect(reason),
            reply: inspect(reply, limit: 200)
          )
      end
    else
      Logger.warning("No NFS server resource — cannot reply", request_id: request_id)
    end

    if state.test_notify do
      send(state.test_notify, {:nfs_op_complete, request_id, reply})
    end

    {:noreply, state}
  end

  ## Private: Dispatch and Timing

  defp timed_dispatch(op_name, params, state) do
    start_time = System.monotonic_time()
    {reply, state} = dispatch(op_name, params, state)
    duration = System.monotonic_time() - start_time
    {reply, duration, state}
  end

  defp emit_telemetry(op_name, reply, duration, volume \\ nil) do
    result = if match?({:error, _}, reply), do: :error, else: :ok

    :telemetry.execute(
      [:neonfs, :nfs, :request, :stop],
      %{duration: duration},
      %{operation: op_name, result: result, volume: volume || "unknown"}
    )
  end

  defp emit_cache_telemetry(hit_or_miss, type, volume) do
    :telemetry.execute(
      [:neonfs, :nfs, :metadata_cache, hit_or_miss],
      %{count: 1},
      %{volume: volume, type: type}
    )
  end

  ## Operation Handlers

  # Lookup: resolve filename in parent directory
  defp dispatch("lookup", params, state) do
    parent_inode = params["parent_inode"]
    volume_id_bytes = params["parent_volume_id"]
    name = params["name"]

    if virtual_root?(volume_id_bytes, parent_inode) do
      lookup_volume(name, state)
    else
      result =
        with {:ok, volume} <- resolve_volume(volume_id_bytes, state),
             {:ok, parent_path} <- inode_path(parent_inode) do
          cached_lookup(state.cache_table, volume, parent_path, name)
        end

      {result_to_reply(result), state}
    end
  end

  # Getattr: get file attributes
  defp dispatch("getattr", params, state) do
    inode = params["inode"]
    volume_id_bytes = params["volume_id"]

    result =
      if virtual_root?(volume_id_bytes, inode) do
        {:ok, build_reply("attrs", 1, synthetic_dir_attrs())}
      else
        getattr_resolve(volume_id_bytes, inode, state)
      end

    {result_to_reply(result), state}
  end

  # Read: read file data
  defp dispatch("read", params, state) do
    inode = params["inode"]
    volume_id_bytes = params["volume_id"]
    offset = params["offset"]
    count = params["count"]

    result =
      with {:ok, volume} <- resolve_volume(volume_id_bytes, state),
           {:ok, path} <- inode_path(inode),
           {:ok, data} <- read_file(volume, path, offset: offset, length: count) do
        eof = byte_size(data) < count
        {:ok, {:ok, %{"type" => "read", "data" => data, "eof" => eof}}}
      end

    {result_to_reply(result), state}
  end

  # Readlink: read symlink target
  defp dispatch("readlink", params, state) do
    inode = params["inode"]
    volume_id_bytes = params["volume_id"]

    result =
      with {:ok, volume} <- resolve_volume(volume_id_bytes, state),
           {:ok, path} <- inode_path(inode),
           {:ok, file} <- file_index_get_by_path(volume, path) do
        target = Map.get(file, :symlink_target, "")
        {:ok, {:ok, %{"type" => "readlink", "target" => target}}}
      end

    {result_to_reply(result), state}
  end

  # Readdirplus: list directory with attributes per entry
  defp dispatch("readdirplus", params, state) do
    inode = params["inode"]
    volume_id_bytes = params["volume_id"]
    _cookie = params["cookie"]

    if virtual_root?(volume_id_bytes, inode) do
      list_virtual_root(state)
    else
      result =
        with {:ok, volume} <- resolve_volume(volume_id_bytes, state),
             {:ok, path} <- inode_path(inode) do
          cached_readdir(state.cache_table, volume, path)
        end

      {result_to_reply(result), state}
    end
  end

  # Setattr: modify file attributes
  defp dispatch("setattr", params, state) do
    inode = params["inode"]
    volume_id_bytes = params["volume_id"]

    result =
      with {:ok, volume} <- resolve_volume(volume_id_bytes, state),
           {:ok, path} <- inode_path(inode),
           {:ok, file} <- file_index_get_by_path(volume, path) do
        updates = build_setattr_updates(params)
        new_size = params["size"]

        update_result =
          if new_size != nil and new_size < file.size do
            file_index_truncate(file.id, new_size, Keyword.delete(updates, :size))
          else
            file_index_update(file.id, updates)
          end

        case update_result do
          {:ok, updated} -> {:ok, build_reply("attrs", inode, updated)}
          error -> error
        end
      end

    {result_to_reply(result), state}
  end

  # Write: write file data
  defp dispatch("write", params, state) do
    inode = params["inode"]
    volume_id_bytes = params["volume_id"]
    offset = params["offset"]
    data = params["data"]

    result =
      with {:ok, volume} <- resolve_volume(volume_id_bytes, state),
           {:ok, path} <- inode_path(inode),
           {:ok, existing_file} <- file_index_get_by_path(volume, path),
           merged <- merge_write_data(volume, existing_file, offset, data),
           {:ok, file} <- write_file(volume, path, merged) do
        count = byte_size(data)
        {:ok, build_write_reply(count, inode, file)}
      end

    {result_to_reply(result), state}
  end

  # Create: create a new file
  defp dispatch("create", params, state) do
    parent_inode = params["parent_inode"]
    volume_id_bytes = params["parent_volume_id"]
    name = params["name"]
    mode = params["mode"] || 0o644

    result =
      with {:ok, volume} <- resolve_volume(volume_id_bytes, state),
           {:ok, parent_path} <- inode_path(parent_inode),
           child_path <- Path.join(parent_path, name),
           {:ok, file} <- write_file(volume, child_path, <<>>, mode: @s_ifreg ||| mode),
           {:ok, inode} <- InodeTable.allocate_inode(volume, child_path) do
        {:ok, build_reply("create", inode, file)}
      end

    {result_to_reply(result), state}
  end

  # Create exclusive: atomic create-if-absent
  defp dispatch("create_exclusive", params, state) do
    parent_inode = params["parent_inode"]
    volume_id_bytes = params["parent_volume_id"]
    name = params["name"]

    result =
      with {:ok, volume} <- resolve_volume(volume_id_bytes, state),
           {:ok, parent_path} <- inode_path(parent_inode),
           child_path <- Path.join(parent_path, name),
           :ok <- check_not_exists(volume, child_path),
           {:ok, file} <- write_file(volume, child_path, <<>>),
           {:ok, inode} <- InodeTable.allocate_inode(volume, child_path) do
        {:ok, build_reply("create", inode, file)}
      end

    {result_to_reply(result), state}
  end

  # Mkdir: create a new directory
  defp dispatch("mkdir", params, state) do
    parent_inode = params["parent_inode"]
    volume_id_bytes = params["parent_volume_id"]
    name = params["name"]

    result =
      with {:ok, volume} <- resolve_volume(volume_id_bytes, state),
           {:ok, parent_path} <- inode_path(parent_inode),
           child_path <- Path.join(parent_path, name),
           {:ok, file} <- write_file(volume, child_path, <<>>, mode: @s_ifdir ||| 0o755),
           {:ok, inode} <- InodeTable.allocate_inode(volume, child_path) do
        {:ok, build_reply("create", inode, file)}
      end

    {result_to_reply(result), state}
  end

  # Remove: delete file or directory
  defp dispatch("remove", params, state) do
    parent_inode = params["parent_inode"]
    volume_id_bytes = params["parent_volume_id"]
    name = params["name"]

    result =
      with {:ok, volume} <- resolve_volume(volume_id_bytes, state),
           {:ok, parent_path} <- inode_path(parent_inode),
           child_path <- Path.join(parent_path, name),
           {:ok, file} <- file_index_get_by_path(volume, child_path),
           :ok <- file_index_delete(file.id) do
        case InodeTable.get_inode(volume, child_path) do
          {:ok, old_inode} -> InodeTable.release_inode(old_inode)
          _ -> :ok
        end

        {:ok, {:ok, %{"type" => "empty"}}}
      end

    {result_to_reply(result), state}
  end

  # Rename: move a file or directory
  defp dispatch("rename", params, state) do
    from_parent_inode = params["from_parent_inode"]
    from_vol_bytes = params["from_parent_volume_id"]
    from_name = params["from_name"]
    to_parent_inode = params["to_parent_inode"]
    _to_vol_bytes = params["to_parent_volume_id"]
    to_name = params["to_name"]

    result =
      with {:ok, volume} <- resolve_volume(from_vol_bytes, state),
           {:ok, from_parent} <- inode_path(from_parent_inode),
           {:ok, to_parent} <- inode_path(to_parent_inode),
           from_path <- Path.join(from_parent, from_name),
           to_path <- Path.join(to_parent, to_name),
           {:ok, file} <- file_index_get_by_path(volume, from_path),
           {:ok, _} <- file_index_update(file.id, path: to_path) do
        case InodeTable.get_inode(volume, from_path) do
          {:ok, old_inode} ->
            InodeTable.release_inode(old_inode)
            InodeTable.allocate_inode(volume, to_path)

          _ ->
            :ok
        end

        {:ok, {:ok, %{"type" => "empty"}}}
      end

    {result_to_reply(result), state}
  end

  # Symlink: create a symbolic link
  defp dispatch("symlink", params, state) do
    parent_inode = params["parent_inode"]
    volume_id_bytes = params["parent_volume_id"]
    name = params["name"]
    target = params["target"]

    result =
      with {:ok, volume} <- resolve_volume(volume_id_bytes, state),
           {:ok, parent_path} <- inode_path(parent_inode),
           child_path <- Path.join(parent_path, name),
           {:ok, file} <-
             core_call(NeonFS.Core.WriteOperation, :create_symlink, [
               volume,
               child_path,
               target
             ]),
           {:ok, inode} <- InodeTable.allocate_inode(volume, child_path) do
        {:ok, build_reply("create", inode, file)}
      end

    {result_to_reply(result), state}
  end

  # Unknown operation
  defp dispatch(op_name, _params, state) do
    Logger.warning("Unknown NFS operation", operation: op_name)
    {{:error, errno(:enosys)}, state}
  end

  ## Cache-Aware Operations

  defp getattr_resolve(volume_id_bytes, inode, state) do
    with {:ok, _volume} <- resolve_volume(volume_id_bytes, state),
         {:ok, {volume, path}} <- InodeTable.get_path(inode) do
      if path == "/" do
        {:ok, build_reply("attrs", inode, synthetic_dir_attrs())}
      else
        cached_getattr(state.cache_table, volume, path, inode)
      end
    end
  end

  defp cached_lookup(cache_table, volume, parent_path, name) do
    case cache_table && MetadataCache.get_lookup(cache_table, volume, parent_path, name) do
      {:ok, result} ->
        emit_cache_telemetry(:hit, :lookup, volume)
        {:ok, result}

      _ ->
        if cache_table, do: emit_cache_telemetry(:miss, :lookup, volume)
        uncached_lookup(cache_table, volume, parent_path, name)
    end
  end

  defp uncached_lookup(cache_table, volume, parent_path, name) do
    child_path = Path.join(parent_path, name)

    with {:ok, file} <- file_index_get_by_path(volume, child_path),
         {:ok, inode} <- InodeTable.allocate_inode(volume, child_path) do
      reply = build_reply("lookup", inode, file)
      maybe_cache_lookup(cache_table, volume, parent_path, name, child_path, file, reply)
      {:ok, reply}
    end
  end

  defp maybe_cache_lookup(nil, _vol, _parent, _name, _child, _file, _reply), do: :ok

  defp maybe_cache_lookup(cache_table, volume, parent_path, name, child_path, file, reply) do
    MetadataCache.put_lookup(cache_table, volume, parent_path, name, reply)
    MetadataCache.put_attrs(cache_table, volume, child_path, file)
  end

  defp cached_getattr(cache_table, volume, path, inode) do
    case cache_table && MetadataCache.get_attrs(cache_table, volume, path) do
      {:ok, attrs} ->
        emit_cache_telemetry(:hit, :attrs, volume)
        {:ok, build_reply("attrs", inode, attrs)}

      _ ->
        if cache_table, do: emit_cache_telemetry(:miss, :attrs, volume)
        uncached_getattr(cache_table, volume, path, inode)
    end
  end

  defp uncached_getattr(cache_table, volume, path, inode) do
    with {:ok, file} <- file_index_get_by_path(volume, path) do
      if cache_table, do: MetadataCache.put_attrs(cache_table, volume, path, file)
      {:ok, build_reply("attrs", inode, file)}
    end
  end

  defp cached_readdir(cache_table, volume, path) do
    case cache_table && MetadataCache.get_dir_listing(cache_table, volume, path) do
      {:ok, entries} ->
        emit_cache_telemetry(:hit, :dir, volume)
        {:ok, {:ok, %{"type" => "dir_entries", "entries" => entries}}}

      _ ->
        if cache_table, do: emit_cache_telemetry(:miss, :dir, volume)
        uncached_readdir(cache_table, volume, path)
    end
  end

  defp uncached_readdir(cache_table, volume, path) do
    with {:ok, entries} <- list_directory(volume, path) do
      dir_entries =
        Enum.map(entries, fn {name, child_path, file_attrs} ->
          {:ok, child_inode} = InodeTable.allocate_inode(volume, child_path)
          build_dir_entry(child_inode, name, file_attrs)
        end)

      if cache_table, do: MetadataCache.put_dir_listing(cache_table, volume, path, dir_entries)
      {:ok, {:ok, %{"type" => "dir_entries", "entries" => dir_entries}}}
    end
  end

  ## Volume Management

  defp virtual_root?(volume_id_bytes, inode) do
    volume_id_bytes == @null_volume_id and inode == 1
  end

  defp volume_id_hash(volume_name) do
    :crypto.hash(:md5, volume_name)
  end

  defp register_volume(volume_name, state) do
    hash = volume_id_hash(volume_name)

    %{
      state
      | volume_ids: Map.put(state.volume_ids, hash, volume_name),
        volume_hashes: Map.put(state.volume_hashes, volume_name, hash)
    }
  end

  defp resolve_volume(volume_id_bytes, state) do
    case Map.get(state.volume_ids, volume_id_bytes) do
      nil -> {:error, :stale}
      name -> {:ok, name}
    end
  end

  defp lookup_volume(name, state) do
    case core_call(NeonFS.Core.VolumeRegistry, :get_by_name, [name]) do
      {:ok, _volume} ->
        new_state = register_volume(name, state)
        {:ok, _} = InodeTable.allocate_inode(name, "/")
        {:ok, root_inode} = InodeTable.get_inode(name, "/")

        reply =
          build_reply("lookup", root_inode, synthetic_dir_attrs())
          |> put_volume_id(name)

        {reply, new_state}

      {:error, :not_found} ->
        {{:error, errno(:enoent)}, state}

      {:error, reason} ->
        Logger.warning("Volume lookup failed", name: name, reason: inspect(reason))
        {{:error, errno(:eio)}, state}
    end
  end

  defp list_virtual_root(state) do
    case core_call(NeonFS.Core.VolumeRegistry, :list, []) do
      volumes when is_list(volumes) ->
        new_state =
          Enum.reduce(volumes, state, fn vol, acc ->
            name = volume_name(vol)
            register_volume(name, acc)
          end)

        entries =
          Enum.map(volumes, fn vol ->
            name = volume_name(vol)
            {:ok, _} = InodeTable.allocate_inode(name, "/")
            {:ok, inode} = InodeTable.get_inode(name, "/")

            build_dir_entry(inode, name, synthetic_dir_attrs())
            |> Map.put("volume_id", volume_id_hash(name))
          end)

        reply = {:ok, %{"type" => "dir_entries", "entries" => entries}}
        {reply, new_state}

      {:error, reason} ->
        Logger.warning("List volumes failed", reason: inspect(reason))
        {{:error, errno(:eio)}, state}
    end
  end

  defp volume_name(vol) when is_binary(vol), do: vol
  defp volume_name(%{name: name}) when is_binary(name), do: name

  ## Reply Building

  defp build_reply(type, inode, file_or_attrs) do
    {:ok, file_to_nfs_attrs(type, inode, file_or_attrs)}
  end

  defp put_volume_id({:ok, map}, volume_name) do
    {:ok, Map.put(map, "volume_id", volume_id_hash(volume_name))}
  end

  defp build_write_reply(count, inode, file) do
    attrs = file_to_nfs_attrs("write", inode, file)
    {:ok, Map.put(attrs, "count", count)}
  end

  defp build_dir_entry(inode, name, file_or_attrs) do
    mode = Map.get(file_or_attrs, :mode, 0o644)
    {atime_secs, atime_nsecs} = datetime_to_nfs_time(Map.get(file_or_attrs, :accessed_at))
    {mtime_secs, mtime_nsecs} = datetime_to_nfs_time(Map.get(file_or_attrs, :modified_at))
    {ctime_secs, ctime_nsecs} = datetime_to_nfs_time(Map.get(file_or_attrs, :changed_at))

    %{
      "file_id" => inode,
      "name" => name,
      "size" => Map.get(file_or_attrs, :size, 0),
      "kind" => file_kind(mode),
      "mode" => mode &&& 0o7777,
      "uid" => Map.get(file_or_attrs, :uid, 0),
      "gid" => Map.get(file_or_attrs, :gid, 0),
      "nlink" => if(directory?(mode), do: 2, else: 1),
      "atime_secs" => atime_secs,
      "atime_nsecs" => atime_nsecs,
      "mtime_secs" => mtime_secs,
      "mtime_nsecs" => mtime_nsecs,
      "ctime_secs" => ctime_secs,
      "ctime_nsecs" => ctime_nsecs
    }
  end

  defp file_to_nfs_attrs(type, inode, file_or_attrs) when is_map(file_or_attrs) do
    mode = Map.get(file_or_attrs, :mode, 0o644)
    {atime_secs, atime_nsecs} = datetime_to_nfs_time(Map.get(file_or_attrs, :accessed_at))
    {mtime_secs, mtime_nsecs} = datetime_to_nfs_time(Map.get(file_or_attrs, :modified_at))
    {ctime_secs, ctime_nsecs} = datetime_to_nfs_time(Map.get(file_or_attrs, :changed_at))

    %{
      "type" => type,
      "file_id" => inode,
      "size" => Map.get(file_or_attrs, :size, 0),
      "kind" => file_kind(mode),
      "mode" => mode &&& 0o7777,
      "uid" => Map.get(file_or_attrs, :uid, 0),
      "gid" => Map.get(file_or_attrs, :gid, 0),
      "nlink" => if(directory?(mode), do: 2, else: 1),
      "atime_secs" => atime_secs,
      "atime_nsecs" => atime_nsecs,
      "mtime_secs" => mtime_secs,
      "mtime_nsecs" => mtime_nsecs,
      "ctime_secs" => ctime_secs,
      "ctime_nsecs" => ctime_nsecs
    }
  end

  defp synthetic_dir_attrs do
    now = DateTime.utc_now()

    %{
      size: 0,
      mode: @s_ifdir ||| 0o755,
      uid: 0,
      gid: 0,
      accessed_at: now,
      modified_at: now,
      changed_at: now
    }
  end

  ## Result to Reply Conversion

  defp result_to_reply({:ok, nfs_reply}), do: nfs_reply
  defp result_to_reply({:error, :not_found}), do: {:error, errno(:enoent)}
  defp result_to_reply({:error, %{class: :not_found}}), do: {:error, errno(:enoent)}
  defp result_to_reply({:error, :forbidden}), do: {:error, errno(:eacces)}
  defp result_to_reply({:error, %{class: :forbidden}}), do: {:error, errno(:eacces)}
  defp result_to_reply({:error, :stale}), do: {:error, errno(:estale)}
  defp result_to_reply({:error, :exists}), do: {:error, errno(:eexist)}

  defp result_to_reply({:error, reason}) do
    Logger.warning("Operation failed", reason: inspect(reason))
    {:error, errno(:eio)}
  end

  ## Helpers

  defp inode_path(inode) do
    case InodeTable.get_path(inode) do
      {:ok, {_volume, path}} -> {:ok, path}
      error -> error
    end
  end

  defp directory?(mode), do: (mode &&& @s_ifmt) == @s_ifdir

  defp file_kind(mode) do
    case mode &&& @s_ifmt do
      @s_ifdir -> "directory"
      @s_iflnk -> "symlink"
      _ -> "file"
    end
  end

  defp datetime_to_nfs_time(%DateTime{} = dt) do
    total_ns = DateTime.to_unix(dt, :nanosecond)
    secs = div(total_ns, 1_000_000_000)
    nsecs = rem(total_ns, 1_000_000_000)
    {secs, nsecs}
  end

  defp datetime_to_nfs_time(_), do: {0, 0}

  defp merge_write_data(volume, file, offset, new_data) do
    case read_file(volume, file.path) do
      {:ok, existing} ->
        padded =
          if offset > byte_size(existing) do
            existing <> :binary.copy(<<0>>, offset - byte_size(existing))
          else
            existing
          end

        <<before::binary-size(offset), _rest::binary>> = padded
        before <> new_data

      {:error, _} ->
        if offset > 0 do
          :binary.copy(<<0>>, offset) <> new_data
        else
          new_data
        end
    end
  end

  defp check_not_exists(volume, path) do
    case file_index_get_by_path(volume, path) do
      {:ok, _} -> {:error, :exists}
      {:error, :not_found} -> :ok
      {:error, %{class: :not_found}} -> :ok
      error -> error
    end
  end

  defp build_setattr_updates(params) do
    []
    |> maybe_add_update(:mode, params["mode"])
    |> maybe_add_update(:uid, params["uid"])
    |> maybe_add_update(:gid, params["gid"])
    |> maybe_add_update(:size, params["size"])
    |> maybe_add_setattr_time(:accessed_at, params["atime"])
    |> maybe_add_setattr_time(:modified_at, params["mtime"])
  end

  defp maybe_add_update(updates, _key, nil), do: updates
  defp maybe_add_update(updates, key, value), do: [{key, value} | updates]

  defp maybe_add_setattr_time(updates, _key, nil), do: updates

  defp maybe_add_setattr_time(updates, key, {sec, nsec}) do
    dt = DateTime.from_unix!(sec * 1_000_000_000 + nsec, :nanosecond)
    [{key, dt} | updates]
  end

  ## Directory Listing

  defp list_directory(volume, "/") do
    files = file_index_list_volume(volume)

    entries =
      files
      |> Enum.filter(&top_level_file?/1)
      |> Enum.map(fn file ->
        name = Path.basename(file.path)
        {name, file.path, file}
      end)

    {:ok, entries}
  end

  defp list_directory(volume, path) do
    dir_path = String.trim_trailing(path, "/")

    case file_index_list_dir(volume, dir_path) do
      children when is_map(children) ->
        {:ok, Enum.map(children, &child_info_to_entry(dir_path, &1))}

      files when is_list(files) ->
        {:ok, Enum.map(files, fn file -> {Path.basename(file.path), file.path, file} end)}
    end
  end

  defp child_info_to_entry(dir_path, {name, child_info}) do
    child_path = Path.join(dir_path, name)
    mode = child_info_mode(child_info[:type])
    {name, child_path, %{size: 0, mode: mode, uid: 0, gid: 0}}
  end

  defp child_info_mode(:dir), do: @s_ifdir ||| 0o755
  defp child_info_mode(_), do: @s_ifreg ||| 0o644

  defp top_level_file?(file) do
    case String.split(file.path, "/", trim: true) do
      [_single] -> true
      _ -> false
    end
  end

  ## RPC Wrappers

  defp core_call(module, function, args) do
    case Process.get(:core_call_fn) do
      fun when is_function(fun, 3) ->
        fun.(module, function, args)

      _ ->
        NeonFS.Client.core_call(module, function, args)
    end
  catch
    :exit, reason ->
      Logger.warning("Core call failed",
        module: module,
        function: function,
        reason: inspect(reason)
      )

      {:error, :all_nodes_unreachable}
  end

  defp file_index_delete(file_id) do
    core_call(NeonFS.Core.FileIndex, :delete, [file_id])
  end

  defp file_index_get_by_path(volume, path) do
    core_call(NeonFS.Core.FileIndex, :get_by_path, [volume, path])
  end

  defp file_index_list_dir(volume, path) do
    case core_call(NeonFS.Core.FileIndex, :list_dir, [volume, path]) do
      {:ok, children} when is_map(children) -> children
      files when is_list(files) -> files
      {:error, _} -> %{}
    end
  end

  defp file_index_list_volume(volume) do
    case core_call(NeonFS.Core.FileIndex, :list_volume, [volume]) do
      files when is_list(files) -> files
      {:error, _} -> []
    end
  end

  defp file_index_truncate(file_id, new_size, additional_updates) do
    core_call(NeonFS.Core.FileIndex, :truncate, [file_id, new_size, additional_updates])
  end

  defp file_index_update(file_id, updates) do
    core_call(NeonFS.Core.FileIndex, :update, [file_id, updates])
  end

  defp read_file(volume, path, opts \\ []) do
    core_call(NeonFS.Core.ReadOperation, :read_file, [volume, path, opts])
  end

  defp write_file(volume, path, data, opts \\ []) do
    core_call(NeonFS.Core.WriteOperation, :write_file, [volume, path, data, opts])
  end

  ## Errno Mapping

  @dialyzer {:nowarn_function, errno: 1}
  defp errno(:eacces), do: 13
  defp errno(:eexist), do: 17
  defp errno(:eio), do: 5
  defp errno(:enoent), do: 2
  defp errno(:enosys), do: 38
  defp errno(:estale), do: 70
  defp errno(_), do: 5
end
