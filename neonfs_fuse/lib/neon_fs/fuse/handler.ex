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

  alias NeonFS.FUSE.{InodeTable, Native}

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
  - `:mount_id` - Mount ID for logging purposes
  - `:name` - Optional name for registration (default: no registration)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    fuse_server = Keyword.get(opts, :fuse_server)
    volume = Keyword.get(opts, :volume, @default_volume)

    Logger.info("FUSE Handler started (volume: #{volume})")

    {:ok, %{fuse_server: fuse_server, volume: volume}}
  end

  @impl true
  def handle_info({:fuse_op, request_id, operation}, state) do
    Logger.debug("Received FUSE operation: #{inspect(operation)}")

    reply = handle_operation(operation, state)

    if state.fuse_server do
      Native.reply_fuse_operation(state.fuse_server, request_id, reply)
    end

    {:noreply, state}
  end

  ## Private Helpers

  # Handle lookup operation: resolve name in parent directory
  defp handle_operation({"lookup", params}, state) do
    parent = params["parent"]
    name = params["name"]

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, file} <- file_index_get_by_path(volume_id, child_path) do
      {:ok, inode} = InodeTable.allocate_inode(volume_id, file.path)

      {"lookup_ok", %{"ino" => inode, "size" => file.size, "kind" => file_kind(file.mode)}}
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Lookup failed: #{inspect(reason)}")
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle getattr operation: get file attributes
  defp handle_operation({"getattr", params}, state) do
    ino = params["ino"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, file} <- fetch_file_or_root(volume_id, path) do
      {"attr_ok", %{"ino" => ino, "size" => file.size, "kind" => file_kind(file.mode)}}
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Getattr failed: #{inspect(reason)}")
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle read operation: read file data
  defp handle_operation({"read", params}, state) do
    ino = params["ino"]
    offset = params["offset"]
    size = params["size"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, data} <- read_file(volume_id, path, offset: offset, length: size) do
      {"read_ok", %{"data" => data}}
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Read failed: #{inspect(reason)}")
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle write operation: write file data
  defp handle_operation({"write", params}, state) do
    ino = params["ino"]
    offset = params["offset"]
    data = params["data"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, existing_file} <- file_index_get_by_path(volume_id, path),
         merged_data <- merge_write_data(existing_file, offset, data),
         {:ok, _file} <- write_file(volume_id, path, merged_data) do
      {"write_ok", %{"size" => byte_size(data)}}
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Write failed: #{inspect(reason)}")
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle readdir operation: list directory contents
  defp handle_operation({"readdir", params}, state) do
    ino = params["ino"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, entries} <- list_directory(volume_id, path) do
      result_entries =
        Enum.map(entries, fn {name, child_path, mode} ->
          {:ok, child_inode} = InodeTable.allocate_inode(volume_id, child_path)
          %{"ino" => child_inode, "name" => name, "kind" => file_kind(mode)}
        end)

      {"readdir_ok", %{"entries" => result_entries}}
    else
      {:error, reason} ->
        Logger.warning("Readdir failed: #{inspect(reason)}")
        {"error", %{"errno" => errno(:enoent)}}
    end
  end

  # Handle create operation: create a new file
  defp handle_operation({"create", params}, state) do
    parent = params["parent"]
    name = params["name"]
    _mode = params["mode"]

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, _file} <- write_file(volume_id, child_path, <<>>, mode: @default_mode),
         {:ok, inode} <- InodeTable.allocate_inode(volume_id, child_path) do
      # Allocate file handle (for now, just use inode as handle)
      fh = inode

      {"entry_ok", %{"ino" => inode, "size" => 0, "kind" => "file", "fh" => fh}}
    else
      {:error, reason} ->
        Logger.warning("Create failed: #{inspect(reason)}")
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle mkdir operation: create a new directory
  defp handle_operation({"mkdir", params}, state) do
    parent = params["parent"]
    name = params["name"]
    _mode = params["mode"]

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, _file} <- write_file(volume_id, child_path, <<>>, mode: @dir_mode),
         {:ok, inode} <- InodeTable.allocate_inode(volume_id, child_path) do
      {"entry_ok", %{"ino" => inode, "size" => 0, "kind" => "directory", "fh" => 0}}
    else
      {:error, reason} ->
        Logger.warning("Mkdir failed: #{inspect(reason)}")
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle unlink operation: delete a file
  defp handle_operation({"unlink", params}, state) do
    parent = params["parent"]
    name = params["name"]

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, inode} <- InodeTable.get_inode(volume_id, child_path),
         {:ok, file} <- file_index_get_by_path(volume_id, child_path),
         :ok <- file_index_delete(file.id),
         :ok <- InodeTable.release_inode(inode) do
      {"ok", %{}}
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, :cannot_release_root} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, reason} ->
        Logger.warning("Unlink failed: #{inspect(reason)}")
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle rmdir operation: delete a directory
  defp handle_operation({"rmdir", params}, state) do
    parent = params["parent"]
    name = params["name"]

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, inode} <- InodeTable.get_inode(volume_id, child_path),
         {:ok, files} <- list_directory(volume_id, child_path),
         true <- Enum.empty?(files) || {:error, :directory_not_empty},
         {:ok, file} <- file_index_get_by_path(volume_id, child_path),
         :ok <- file_index_delete(file.id),
         :ok <- InodeTable.release_inode(inode) do
      {"ok", %{}}
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, :directory_not_empty} ->
        {"error", %{"errno" => errno(:enotempty)}}

      {:error, :cannot_release_root} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, reason} ->
        Logger.warning("Rmdir failed: #{inspect(reason)}")
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
         {:ok, {^volume_id, new_parent_path}} <- resolve_inode(new_parent, state),
         old_path <- build_child_path(old_parent_path, old_name),
         new_path <- build_child_path(new_parent_path, new_name),
         {:ok, file} <- file_index_get_by_path(volume_id, old_path),
         {:ok, old_inode} <- InodeTable.get_inode(volume_id, old_path),
         {:ok, _updated_file} <- file_index_update(file.id, path: new_path),
         :ok <- InodeTable.release_inode(old_inode),
         {:ok, _new_inode} <- InodeTable.allocate_inode(volume_id, new_path) do
      {"ok", %{}}
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, :cannot_release_root} ->
        {"error", %{"errno" => errno(:eacces)}}

      {:error, reason} ->
        Logger.warning("Rename failed: #{inspect(reason)}")
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle setattr operation: modify file attributes
  defp handle_operation({"setattr", params}, state) do
    ino = params["ino"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, file} <- file_index_get_by_path(volume_id, path) do
      updates = build_setattr_updates(params)

      case file_index_update(file.id, updates) do
        {:ok, updated_file} ->
          {"attr_ok",
           %{"ino" => ino, "size" => updated_file.size, "kind" => file_kind(updated_file.mode)}}

        {:error, reason} ->
          Logger.warning("Setattr failed: #{inspect(reason)}")
          {"error", %{"errno" => errno(:eio)}}
      end
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Setattr failed: #{inspect(reason)}")
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle unknown operations
  defp handle_operation({operation, _params}, _state) do
    Logger.warning("Unknown FUSE operation: #{operation}")
    {"error", %{"errno" => errno(:enosys)}}
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
    {:ok,
     %{
       path: "/",
       size: 0,
       mode: @dir_mode,
       uid: 0,
       gid: 0
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

  # Build child path from parent path and name
  defp build_child_path(parent, name), do: Path.join(parent, name)

  # Merge write data at offset with existing file data
  defp merge_write_data(file, offset, new_data) do
    case read_file(file.volume_id, file.path) do
      {:ok, existing} ->
        # Pad if offset is beyond current size
        padded =
          if offset > byte_size(existing) do
            existing <> :binary.copy(<<0>>, offset - byte_size(existing))
          else
            existing
          end

        # Split at offset and insert new data
        <<before::binary-size(offset), _rest::binary>> = padded
        before <> new_data

      {:error, _} ->
        # File doesn't exist or read failed, just use new data with padding
        if offset > 0 do
          :binary.copy(<<0>>, offset) <> new_data
        else
          new_data
        end
    end
  end

  # Build updates list for setattr
  defp build_setattr_updates(params) do
    []
    |> maybe_add_update(:mode, params["mode"])
    |> maybe_add_update(:uid, params["uid"])
    |> maybe_add_update(:gid, params["gid"])
    |> maybe_add_setattr_size(params["size"])
    |> maybe_add_setattr_time(:accessed_at, params["atime"])
    |> maybe_add_setattr_time(:modified_at, params["mtime"])
  end

  defp maybe_add_update(updates, _key, nil), do: updates
  defp maybe_add_update(updates, key, value), do: [{key, value} | updates]

  defp maybe_add_setattr_size(updates, nil), do: updates

  defp maybe_add_setattr_size(updates, size) do
    # Size changes require truncation/extension, which is a write operation
    # For now, we just track it in metadata
    [{:size, size} | updates]
  end

  defp maybe_add_setattr_time(updates, _key, nil), do: updates

  defp maybe_add_setattr_time(updates, key, {sec, nsec}) do
    dt = DateTime.from_unix!(sec * 1_000_000_000 + nsec, :nanosecond)
    [{key, dt} | updates]
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

  defp read_file(volume_id, path, opts \\ []) do
    core_call(NeonFS.Core.ReadOperation, :read_file, [volume_id, path, opts])
  end

  defp write_file(volume_id, path, data, opts \\ []) do
    core_call(NeonFS.Core.WriteOperation, :write_file, [volume_id, path, data, opts])
  end

  # Convert error reason to errno code
  @dialyzer {:nowarn_function, errno: 1}
  defp errno(:enoent), do: 2
  defp errno(:eio), do: 5
  defp errno(:eacces), do: 13
  defp errno(:enotempty), do: 39
  defp errno(:enosys), do: 38
  defp errno(_), do: 5
end
