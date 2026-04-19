defmodule NeonFS.FUSE.Handler do
  @moduledoc """
  Handles FUSE operations from the Rust NIF layer.

  Receives FUSE operation messages from the Rust NIF in the format:
  `{:fuse_op, request_id, {operation_name, params}}`

  Dispatches operations to the appropriate NeonFS.Core modules and replies
  via the NIF with results or errors.

  ## Operation Flow

  1. Rust FUSE layer receives filesystem operation
  2. Operation is sent as message to this handler
  3. Handler dispatches to NeonFS.Core (ReadOperation, WriteOperation, FileIndex)
  4. Handler constructs reply and sends back via NIF
  5. Rust layer converts reply to FUSE response

  ## Inode Management

  Uses InodeTable to map FUSE inodes to filesystem paths.
  - Inode 1 is always the root directory
  - Inodes are allocated on lookup/create/mkdir
  - Inodes are released on unlink/rmdir
  """

  use GenServer
  import Bitwise
  require Logger

  alias NeonFS.Client.ChunkReader
  alias NeonFS.FUSE.{InodeTable, MetadataCache, Native}

  @default_volume "default"

  # POSIX file type bits
  @s_ifdir 0o040000
  @s_ifreg 0o100000
  @s_ifmt 0o170000

  # File modes with type bits included
  @default_mode @s_ifreg ||| 0o644
  @dir_mode @s_ifdir ||| 0o755

  ## Client API

  @doc """
  Start the FUSE handler GenServer.

  Options:
  - `:volume` - Volume ID to use for this mount (default: "default")
  - `:volume_name` - Volume name used for data-plane reads via
    `NeonFS.Client.ChunkReader` (defaults to the `:volume` value)
  - `:mount_id` - Mount ID for logging purposes
  - `:name` - Optional name for registration (default: no registration)
  - `:cache_table` - ETS table reference for MetadataCache (default: nil)
  - `:test_notify` - Pid to notify when operations complete (test only)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @doc """
  Checks whether `accessed_at` is stale under `relatime` rules.

  Returns `true` if:
  - `accessed_at` is older than `modified_at`, OR
  - `accessed_at` is more than 24 hours old

  The optional `now` parameter is for testing.
  """
  @spec relatime_stale?(DateTime.t(), DateTime.t(), DateTime.t()) :: boolean()
  def relatime_stale?(accessed_at, modified_at, now \\ DateTime.utc_now()) do
    DateTime.compare(accessed_at, modified_at) == :lt or
      DateTime.diff(now, accessed_at, :second) > 86_400
  end

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    volume = Keyword.get(opts, :volume, @default_volume)
    volume_name = Keyword.get(opts, :volume_name, volume)
    Logger.metadata(component: :fuse, volume_id: volume)
    fuse_server = Keyword.get(opts, :fuse_server)
    uid = Keyword.get(opts, :uid, 0)
    gid = Keyword.get(opts, :gid, 0)
    gids = Keyword.get(opts, :gids, [])
    cache_table = Keyword.get(opts, :cache_table)
    atime_mode = Keyword.get(opts, :atime_mode, :noatime)

    Logger.info("FUSE Handler started", volume: volume, atime_mode: atime_mode)

    {:ok,
     %{
       fuse_server: fuse_server,
       volume: volume,
       volume_name: volume_name,
       uid: uid,
       gid: gid,
       gids: gids,
       cache_table: cache_table,
       atime_mode: atime_mode,
       test_notify: Keyword.get(opts, :test_notify)
     }}
  end

  @impl true
  def handle_info({:fuse_op, request_id, operation}, state) do
    Logger.metadata(request_id: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))

    Logger.debug("Received FUSE operation", operation: inspect(operation))

    {reply, duration} = timed_handle_operation(operation, state)

    emit_fuse_telemetry(operation, reply, duration, state.volume)

    if state.fuse_server do
      Native.reply_fuse_operation(state.fuse_server, request_id, reply)
    end

    if state.test_notify do
      send(state.test_notify, {:fuse_op_complete, request_id, reply})
    end

    {:noreply, state}
  end

  ## Private Helpers

  defp timed_handle_operation(operation, state) do
    start_time = System.monotonic_time()
    reply = handle_operation(operation, state)
    duration = System.monotonic_time() - start_time
    {reply, duration}
  end

  defp emit_fuse_telemetry({op_name, _params}, reply, duration, volume) do
    result = if match?({"error", _}, reply), do: :error, else: :ok

    :telemetry.execute(
      [:neonfs, :fuse, :request, :stop],
      %{duration: duration},
      %{operation: op_name, volume: volume, result: result}
    )
  end

  defp emit_cache_telemetry(hit_or_miss, type, volume) do
    :telemetry.execute(
      [:neonfs, :fuse, :metadata_cache, hit_or_miss],
      %{count: 1},
      %{volume: volume, type: type}
    )
  end

  # Handle lookup operation: resolve name in parent directory
  defp handle_operation({"lookup", params}, state) do
    parent = params["parent"]
    name = params["name"]

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, file} <- cached_lookup(state.cache_table, volume_id, parent_path, name, child_path) do
      {:ok, inode} = InodeTable.allocate_inode(volume_id, file.path)

      {"lookup_ok", %{"ino" => inode, "size" => file.size, "kind" => file_kind(file.mode)}}
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Lookup failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle getattr operation: get file attributes
  defp handle_operation({"getattr", params}, state) do
    ino = params["ino"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, file} <- cached_getattr(state.cache_table, volume_id, path) do
      {"attr_ok",
       %{
         "ino" => ino,
         "size" => file.size,
         "kind" => file_kind(file.mode),
         "mtime" => datetime_to_unix(file.modified_at),
         "ctime" => datetime_to_unix(file.changed_at),
         "atime" => datetime_to_unix(file.accessed_at)
       }}
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Getattr failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle read operation: read file data
  defp handle_operation({"read", params}, state) do
    ino = params["ino"]
    offset = params["offset"]
    size = params["size"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         :ok <- check_file_permission(volume_id, path, :read, state),
         {:ok, data} <- read_file(state.volume_name, path, offset: offset, length: size) do
      maybe_update_atime(volume_id, path, state.atime_mode)
      {"read_ok", %{"data" => data}}
    else
      {:error, :forbidden} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, %{class: :forbidden}} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Read failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle write operation: write file data
  defp handle_operation({"write", params}, state) do
    ino = params["ino"]
    offset = params["offset"]
    data = params["data"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         :ok <- check_file_permission(volume_id, path, :write, state),
         {:ok, _file} <-
           core_call(NeonFS.Core.WriteOperation, :write_file_at, [volume_id, path, offset, data]) do
      {"write_ok", %{"size" => byte_size(data)}}
    else
      {:error, :forbidden} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, %{class: :forbidden}} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Write failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle readdir operation: list directory contents
  defp handle_operation({"readdir", params}, state) do
    ino = params["ino"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, entries} <- cached_readdir(state.cache_table, volume_id, path) do
      result_entries =
        Enum.map(entries, fn {name, child_path, mode} ->
          {:ok, child_inode} = InodeTable.allocate_inode(volume_id, child_path)
          %{"ino" => child_inode, "name" => name, "kind" => file_kind(mode)}
        end)

      {"readdir_ok", %{"entries" => result_entries}}
    else
      {:error, reason} ->
        Logger.warning("Readdir failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:enoent)}}
    end
  end

  # Handle create operation: create a new file
  defp handle_operation({"create", params}, state) do
    parent = params["parent"]
    name = params["name"]
    file_mode = create_mode(params["mode"], @s_ifreg, @default_mode)

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         :ok <- check_file_permission(volume_id, parent_path, :write, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, _file} <- write_file(volume_id, child_path, <<>>, mode: file_mode),
         {:ok, inode} <- InodeTable.allocate_inode(volume_id, child_path) do
      # Allocate file handle (for now, just use inode as handle)
      fh = inode

      {"entry_ok", %{"ino" => inode, "size" => 0, "kind" => "file", "fh" => fh}}
    else
      {:error, :forbidden} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, %{class: :forbidden}} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, reason} ->
        Logger.warning("Create failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle mkdir operation: create a new directory
  defp handle_operation({"mkdir", params}, state) do
    parent = params["parent"]
    name = params["name"]
    dir_mode = create_mode(params["mode"], @s_ifdir, @dir_mode)

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         :ok <- check_file_permission(volume_id, parent_path, :write, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, _file} <- write_file(volume_id, child_path, <<>>, mode: dir_mode),
         {:ok, inode} <- InodeTable.allocate_inode(volume_id, child_path) do
      {"entry_ok", %{"ino" => inode, "size" => 0, "kind" => "directory", "fh" => 0}}
    else
      {:error, :forbidden} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, %{class: :forbidden}} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, reason} ->
        Logger.warning("Mkdir failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle unlink operation: delete a file
  defp handle_operation({"unlink", params}, state) do
    parent = params["parent"]
    name = params["name"]

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         :ok <- check_file_permission(volume_id, parent_path, :write, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, inode} <- InodeTable.get_inode(volume_id, child_path),
         {:ok, file} <- file_index_get_by_path(volume_id, child_path),
         :ok <- file_index_delete(file.id),
         :ok <- InodeTable.release_inode(inode) do
      {"ok", %{}}
    else
      {:error, :forbidden} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, %{class: :forbidden}} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, :cannot_release_root} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, reason} ->
        Logger.warning("Unlink failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle rmdir operation: delete a directory
  defp handle_operation({"rmdir", params}, state) do
    parent = params["parent"]
    name = params["name"]

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         :ok <- check_file_permission(volume_id, parent_path, :write, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, inode} <- InodeTable.get_inode(volume_id, child_path),
         {:ok, files} <- list_directory(volume_id, child_path),
         true <- Enum.empty?(files) || {:error, :directory_not_empty},
         {:ok, file} <- file_index_get_by_path(volume_id, child_path),
         :ok <- file_index_delete(file.id),
         :ok <- InodeTable.release_inode(inode) do
      {"ok", %{}}
    else
      {:error, :forbidden} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, %{class: :forbidden}} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, :directory_not_empty} ->
        {"error", %{"errno" => errno(:enotempty)}}

      {:error, :cannot_release_root} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, reason} ->
        Logger.warning("Rmdir failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle open operation: open a file for reading/writing
  defp handle_operation({"open", params}, _state) do
    ino = params["ino"]
    # For Phase 1, just return the inode as the file handle
    {"open_ok", %{"fh" => ino}}
  end

  # Handle release operation: close a file
  defp handle_operation({"release", _params}, _state) do
    # For Phase 1, no cleanup needed
    {"ok", %{}}
  end

  # Handle rename operation: rename/move a file or directory
  defp handle_operation({"rename", params}, state) do
    old_parent = params["old_parent"]
    old_name = params["old_name"]
    new_parent = params["new_parent"]
    new_name = params["new_name"]

    with {:ok, {volume_id, old_parent_path}} <- resolve_inode(old_parent, state),
         {:ok, {new_volume_id, new_parent_path}} <- resolve_inode(new_parent, state),
         :ok <- check_same_volume(volume_id, new_volume_id),
         :ok <- check_file_permission(volume_id, old_parent_path, :write, state),
         :ok <- check_file_permission(volume_id, new_parent_path, :write, state),
         old_path <- build_child_path(old_parent_path, old_name),
         new_path <- build_child_path(new_parent_path, new_name),
         {:ok, file} <- file_index_get_by_path(volume_id, old_path),
         {:ok, old_inode} <- InodeTable.get_inode(volume_id, old_path),
         {:ok, _updated_file} <- file_index_update(file.id, path: new_path),
         :ok <- InodeTable.release_inode(old_inode),
         {:ok, _new_inode} <- InodeTable.allocate_inode(volume_id, new_path) do
      {"ok", %{}}
    else
      {:error, :cross_volume} ->
        {"error", %{"errno" => errno(:exdev)}}

      {:error, :forbidden} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, %{class: :forbidden}} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, :cannot_release_root} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, reason} ->
        Logger.warning("Rename failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle setattr operation: modify file attributes
  defp handle_operation({"setattr", params}, state) do
    ino = params["ino"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, file} <- file_index_get_by_path(volume_id, path),
         :ok <- check_setattr_permission(file, params, state) do
      result = apply_setattr(file, params)

      case result do
        {:ok, updated_file} ->
          {"attr_ok",
           %{"ino" => ino, "size" => updated_file.size, "kind" => file_kind(updated_file.mode)}}

        {:error, reason} ->
          Logger.warning("Setattr failed", reason: inspect(reason))
          {"error", %{"errno" => errno(:eio)}}
      end
    else
      {:error, :forbidden} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, %{class: :forbidden}} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Setattr failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle unknown operations
  defp handle_operation({operation, _params}, _state) do
    Logger.warning("Unknown FUSE operation", operation: operation)
    {"error", %{"errno" => errno(:enosys)}}
  end

  # Cache-aware lookup: check cache first, fall through to RPC on miss
  defp cached_lookup(nil, volume_id, _parent_path, _name, child_path) do
    file_index_get_by_path(volume_id, child_path)
  end

  defp cached_lookup(cache_table, volume_id, parent_path, name, child_path) do
    case MetadataCache.get_lookup(cache_table, volume_id, parent_path, name) do
      {:ok, file} ->
        emit_cache_telemetry(:hit, :lookup, volume_id)
        {:ok, file}

      :miss ->
        emit_cache_telemetry(:miss, :lookup, volume_id)

        case file_index_get_by_path(volume_id, child_path) do
          {:ok, file} = result ->
            MetadataCache.put_lookup(cache_table, volume_id, parent_path, name, file)
            MetadataCache.put_attrs(cache_table, volume_id, file.path, file)
            result

          error ->
            error
        end
    end
  end

  # Cache-aware getattr: check cache first, fall through to RPC on miss
  defp cached_getattr(nil, volume_id, path) do
    fetch_file_or_root(volume_id, path)
  end

  defp cached_getattr(cache_table, volume_id, path) do
    case MetadataCache.get_attrs(cache_table, volume_id, path) do
      {:ok, file} ->
        emit_cache_telemetry(:hit, :attrs, volume_id)
        {:ok, file}

      :miss ->
        emit_cache_telemetry(:miss, :attrs, volume_id)

        case fetch_file_or_root(volume_id, path) do
          {:ok, file} = result ->
            MetadataCache.put_attrs(cache_table, volume_id, path, file)
            result

          error ->
            error
        end
    end
  end

  # Cache-aware readdir: check cache first, fall through to RPC on miss
  defp cached_readdir(nil, volume_id, path) do
    list_directory(volume_id, path)
  end

  defp cached_readdir(cache_table, volume_id, path) do
    case MetadataCache.get_dir_listing(cache_table, volume_id, path) do
      {:ok, entries} ->
        emit_cache_telemetry(:hit, :readdir, volume_id)
        {:ok, entries}

      :miss ->
        emit_cache_telemetry(:miss, :readdir, volume_id)
        {:ok, entries} = list_directory(volume_id, path)
        MetadataCache.put_dir_listing(cache_table, volume_id, path, entries)
        {:ok, entries}
    end
  end

  # Resolve inode to {volume_id, path}
  defp resolve_inode(ino, state) do
    case InodeTable.get_path(ino) do
      {:ok, {nil, "/"}} ->
        # Root inode - use default volume
        {:ok, {state.volume, "/"}}

      {:ok, {volume_id, path}} ->
        {:ok, {volume_id, path}}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  # Fetch file metadata, handling special case for root directory
  defp fetch_file_or_root(_volume_id, "/") do
    # Root directory doesn't exist in FileIndex, return synthetic metadata
    # Use @dir_mode which includes S_IFDIR bit
    now = DateTime.utc_now()

    {:ok,
     %{
       path: "/",
       size: 0,
       mode: @dir_mode,
       uid: 0,
       gid: 0,
       modified_at: now,
       accessed_at: now,
       changed_at: now
     }}
  end

  defp fetch_file_or_root(volume_id, path) do
    file_index_get_by_path(volume_id, path)
  end

  # List directory contents, returning [{name, full_path, mode}, ...]
  defp list_directory(volume_id, "/") do
    files = file_index_list_volume(volume_id)

    entries =
      files
      |> Enum.filter(&top_level_file?/1)
      |> Enum.map(fn file ->
        name = Path.basename(file.path)
        {name, file.path, file.mode}
      end)

    {:ok, entries}
  end

  defp list_directory(volume_id, path) do
    dir_path = String.trim_trailing(path, "/")
    result = file_index_list_dir(volume_id, dir_path)
    {:ok, parse_dir_entries(result, dir_path)}
  end

  defp parse_dir_entries(children, dir_path) when is_map(children) do
    # New format: %{name => %{type: :file | :dir, id: id}}
    Enum.map(children, fn {name, child_info} ->
      child_path = Path.join(dir_path, name)
      mode = if child_info[:type] == :dir, do: 0o40755, else: 0o100644
      {name, child_path, mode}
    end)
  end

  defp parse_dir_entries(files, _dir_path) when is_list(files) do
    # Legacy format: [%FileMeta{}, ...]
    Enum.map(files, fn file ->
      name = Path.basename(file.path)
      {name, file.path, file.mode}
    end)
  end

  # Check if file is at top level (no subdirectories)
  defp top_level_file?(file) do
    case String.split(file.path, "/", trim: true) do
      [_single] -> true
      _ -> false
    end
  end

  # Check if a file is a directory based on mode bits
  defp directory?(mode), do: (mode &&& @s_ifmt) == @s_ifdir

  # Get the file kind ("directory" or "file") based on mode bits
  defp file_kind(mode), do: if(directory?(mode), do: "directory", else: "file")

  # Build a mode value from the kernel-supplied permission bits.
  # OR's in the file-type constant (S_IFREG or S_IFDIR). Falls back to the
  # default mode when the kernel supplies nil.
  defp create_mode(nil, _type_bits, default), do: default
  defp create_mode(perm_bits, type_bits, _default), do: type_bits ||| perm_bits

  # Build child path from parent path and name
  defp build_child_path(parent, name), do: Path.join(parent, name)

  defp check_same_volume(vol, vol), do: :ok
  defp check_same_volume(_old_vol, _new_vol), do: {:error, :cross_volume}

  # Apply setattr, routing to truncate when size is being reduced
  defp apply_setattr(file, params) do
    new_size = params["size"]

    if new_size != nil and new_size < file.size do
      # Size reduction: delegate to FileIndex.truncate which trims chunks/stripes
      other_updates = build_setattr_updates_without_size(params)
      file_index_truncate(file.id, new_size, other_updates)
    else
      updates = build_setattr_updates(params)
      file_index_update(file.id, updates)
    end
  end

  # Build updates list for setattr
  defp build_setattr_updates(params) do
    []
    |> maybe_add_update(:mode, params["mode"])
    |> maybe_add_update(:uid, params["uid"])
    |> maybe_add_update(:gid, params["gid"])
    |> maybe_add_update(:size, params["size"])
    |> maybe_add_setattr_time(:accessed_at, params["atime"])
    |> maybe_add_setattr_time(:modified_at, params["mtime"])
  end

  # Build updates list without size (used when routing through truncate)
  defp build_setattr_updates_without_size(params) do
    []
    |> maybe_add_update(:mode, params["mode"])
    |> maybe_add_update(:uid, params["uid"])
    |> maybe_add_update(:gid, params["gid"])
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

  # Permission checking — routes through core node's Authorise module
  defp check_file_permission(volume_id, path, action, state) do
    # Root (UID 0) bypasses all checks
    if state.uid == 0 do
      :ok
    else
      # For root directory, fall back to volume-level check
      if path == "/" do
        core_call(NeonFS.Core.Authorise, :check, [
          state.uid,
          state.gids,
          action,
          {:volume, volume_id}
        ])
      else
        core_call(NeonFS.Core.Authorise, :check, [
          state.uid,
          state.gids,
          action,
          {:file, volume_id, path}
        ])
      end
    end
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  # Only owner UID or root can chmod/chown
  defp check_setattr_permission(_file, _params, %{uid: 0}), do: :ok

  defp check_setattr_permission(file, params, state) do
    changes_ownership = params["mode"] != nil or params["uid"] != nil or params["gid"] != nil
    file_uid = Map.get(file, :uid, 0)

    if changes_ownership and state.uid != file_uid do
      {:error, :forbidden}
    else
      :ok
    end
  end

  # Asynchronous atime update for relatime mode.
  # Fires off a background process so reads are never blocked by atime writes.
  defp maybe_update_atime(_volume_id, _path, :noatime), do: :ok

  defp maybe_update_atime(volume_id, path, :relatime) do
    spawn(fn ->
      with {:ok, file} <-
             NeonFS.Client.core_call(NeonFS.Core.FileIndex, :get_by_path, [volume_id, path]),
           true <- relatime_stale?(file.accessed_at, file.modified_at) do
        NeonFS.Client.core_call(NeonFS.Core.FileIndex, :touch, [file.id])
      end
    end)

    :ok
  end

  # RPC wrappers — route all core operations through the client
  defp core_call(module, function, args) do
    NeonFS.Client.core_call(module, function, args)
  end

  defp file_index_get_by_path(volume_id, path) do
    core_call(NeonFS.Core.FileIndex, :get_by_path, [volume_id, path])
  end

  defp file_index_list_volume(volume_id) do
    case core_call(NeonFS.Core.FileIndex, :list_volume, [volume_id]) do
      files when is_list(files) -> files
      {:error, _} -> []
    end
  end

  defp file_index_list_dir(volume_id, path) do
    case core_call(NeonFS.Core.FileIndex, :list_dir, [volume_id, path]) do
      {:ok, children} when is_map(children) -> children
      files when is_list(files) -> files
      {:error, _} -> %{}
    end
  end

  defp file_index_delete(file_id) do
    core_call(NeonFS.Core.FileIndex, :delete, [file_id])
  end

  defp file_index_update(file_id, updates) do
    core_call(NeonFS.Core.FileIndex, :update, [file_id, updates])
  end

  defp file_index_truncate(file_id, new_size, additional_updates) do
    core_call(NeonFS.Core.FileIndex, :truncate, [file_id, new_size, additional_updates])
  end

  defp read_file(volume_id, path, opts) do
    ChunkReader.read_file(volume_id, path, opts)
  end

  defp write_file(volume_id, path, data, opts) do
    core_call(NeonFS.Core.WriteOperation, :write_file, [volume_id, path, data, opts])
  end

  # Convert a DateTime to a POSIX timestamp (seconds since epoch)
  defp datetime_to_unix(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp datetime_to_unix(_), do: 0

  # Convert error reason to errno code
  @dialyzer {:nowarn_function, errno: 1}
  defp errno(:enoent), do: 2
  defp errno(:eio), do: 5
  defp errno(:eacces), do: 13
  defp errno(:exdev), do: 18
  defp errno(:enotempty), do: 39
  defp errno(:enosys), do: 38
  defp errno(_), do: 5
end
