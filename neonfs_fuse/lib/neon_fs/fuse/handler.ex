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
  alias NeonFS.FUSE.{InodeTable, MetadataCache}

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
    uid = Keyword.get(opts, :uid, 0)
    gid = Keyword.get(opts, :gid, 0)
    gids = Keyword.get(opts, :gids, [])
    cache_table = Keyword.get(opts, :cache_table)
    atime_mode = Keyword.get(opts, :atime_mode, :noatime)

    Logger.info("FUSE Handler started", volume: volume, atime_mode: atime_mode)

    {:ok,
     %{
       volume: volume,
       volume_name: volume_name,
       uid: uid,
       gid: gid,
       gids: gids,
       cache_table: cache_table,
       atime_mode: atime_mode,
       # POSIX unlink-while-open: maps an FD-side `fh` to the
       # `file_id` and the `:pinned` namespace claim that keeps
       # the file alive while the handle is open. See sub-issue
       # #651 of #639. On peer crash the namespace coordinator's
       # holder-DOWN bulk release handles cleanup, so no terminate
       # callback is needed here — the safety net lives at the
       # claim layer.
       fh_table: %{},
       next_fh: 1,
       coordinator_module:
         Keyword.get(opts, :coordinator_module, NeonFS.Core.NamespaceCoordinator),
       test_notify: Keyword.get(opts, :test_notify)
     }}
  end

  @impl true
  def handle_info({:fuse_op, request_id, operation}, state) do
    Logger.metadata(request_id: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))

    Logger.debug("Received FUSE operation", operation: inspect(operation))

    {reply, new_state, duration} = timed_handle_operation(operation, state)

    emit_fuse_telemetry(operation, reply, duration, state.volume)

    # Reply path: the Session GenServer subscribes via `:test_notify`
    # and writes the encoded reply back to the FUSE fd. The legacy
    # NIF stack (#279, deleted in #662) used `Native.reply_fuse_operation`
    # via `state.fuse_server`; that field is gone now.
    if state.test_notify do
      send(state.test_notify, {:fuse_op_complete, request_id, reply})
    end

    {:noreply, new_state}
  end

  ## Private Helpers

  defp timed_handle_operation(operation, state) do
    start_time = System.monotonic_time()
    {reply, new_state} = dispatch_operation(operation, state)
    duration = System.monotonic_time() - start_time
    {reply, new_state, duration}
  end

  # Wraps `handle_operation/2` so the few state-mutating ops
  # (`open`, `create`, `release` — the unlink-while-open pin
  # lifecycle, #651) can return `{reply, new_state}` while every
  # other handler keeps its single-return shape. Read-only ops fall
  # through to the unchanged `handle_operation/2` definition.
  defp dispatch_operation({"open", _params} = op, state), do: handle_stateful(op, state)
  defp dispatch_operation({"create", _params} = op, state), do: handle_stateful(op, state)
  defp dispatch_operation({"release", _params} = op, state), do: handle_stateful(op, state)

  defp dispatch_operation(operation, state) do
    {handle_operation(operation, state), state}
  end

  # Open lifecycle (POSIX unlink-while-open, sub-issue #651 of #639).
  #
  # `open`: resolve inode → path → file_id, claim a `:pinned`
  # namespace claim with this GenServer's pid as holder, allocate a
  # monotonic `fh`, and remember `{file_id, claim_id, path}` so
  # later `read` / `write` can route through the file_id (which
  # works for detached files — #638) and so `release` can drop the
  # claim. The Genserver pid is the holder, which means the
  # coordinator's existing holder-DOWN bulk release is the safety
  # net for FUSE-peer crashes.
  defp handle_stateful({"open", params}, state) do
    ino = params["ino"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         :ok <- check_file_permission(volume_id, path, :read, state),
         {:ok, file_meta} <- fetch_file_or_root(volume_id, path) do
      open_dispatch(file_meta, volume_id, path, state)
    else
      {:error, :forbidden} -> {{"error", %{"errno" => errno(:eacces)}}, state}
      {:error, %{class: :forbidden}} -> {{"error", %{"errno" => errno(:eacces)}}, state}
      {:error, :not_found} -> {{"error", %{"errno" => errno(:enoent)}}, state}
      {:error, %{class: :not_found}} -> {{"error", %{"errno" => errno(:enoent)}}, state}
      {:error, _reason} -> {{"error", %{"errno" => errno(:eio)}}, state}
    end
  end

  # FUSE's `create()` is invoked for both `O_CREAT` and `O_CREAT |
  # O_EXCL`. The `O_EXCL` bit means "fail if the target already
  # exists" and must be atomic across nodes — sub-issue #594 of
  # #303 routes this through `WriteOperation`'s `create_only: true`
  # (#592), which in turn uses the namespace coordinator's
  # `claim_create` primitive (#591). After creation we additionally
  # claim a `:pinned` claim against the new path so the open-handle
  # half of the unlink-while-open story (#651) holds.
  defp handle_stateful({"create", params}, state) do
    parent = params["parent"]
    name = params["name"]
    file_mode = create_mode(params["mode"], @s_ifreg, @default_mode)
    flags = Map.get(params, "flags", 0)
    write_opts = create_write_opts(file_mode, flags)

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         :ok <- check_file_permission(volume_id, parent_path, :write, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, file_meta} <- create_empty_file(volume_id, child_path, write_opts),
         {:ok, inode} <- InodeTable.allocate_inode(volume_id, child_path) do
      claim_id =
        case claim_pinned_for_path(volume_id, child_path, state) do
          {:ok, id} -> id
          {:error, _} -> nil
        end

      create_ok(file_meta, claim_id, child_path, inode, state)
    else
      {:error, :forbidden} -> {{"error", %{"errno" => errno(:eacces)}}, state}
      {:error, %{class: :forbidden}} -> {{"error", %{"errno" => errno(:eacces)}}, state}
      {:error, :exists} -> {{"error", %{"errno" => errno(:eexist)}}, state}
      {:error, :conflict, _} -> {{"error", %{"errno" => errno(:eagain)}}, state}
      {:error, reason} -> log_create_failure_and_eio(reason, state)
    end
  end

  # `release` is the FUSE opcode that fires when the last fd for an
  # open file_handle closes. Drop our entry and release the pin so
  # the namespace coordinator's view converges. Idempotent against
  # an unknown `fh` — late-arriving `release` after a crash recovery
  # is plausible and shouldn't be an error.
  defp handle_stateful({"release", params}, state) do
    fh = params["fh"]

    case Map.pop(state.fh_table, fh) do
      {nil, _table} ->
        # Unknown / synthesised fh (directory opens use fh=0
        # without an entry). No-op.
        {{"ok", %{}}, state}

      {%{claim_id: nil}, table} ->
        # Open succeeded without a pin (coordinator was unreachable)
        # — nothing to release.
        {{"ok", %{}}, %{state | fh_table: table}}

      {%{claim_id: claim_id}, table} ->
        release_pin_quietly(state.coordinator_module, claim_id)
        {{"ok", %{}}, %{state | fh_table: table}}
    end
  end

  # Directories don't need a `:pinned` claim — there is no
  # unlink-while-open semantics for them (rmdir on a non-empty dir
  # already errors, and the FUSE layer returns the `ino` directly as
  # `fh` rather than a tracked entry). Regular files go through the
  # claim-and-track path; pin failures are non-fatal so the open
  # still succeeds (the unlink-while-open guarantee is then absent
  # for that single fd, but every other operation works).
  defp open_dispatch(%{mode: mode}, _volume_id, _path, state)
       when (mode &&& @s_ifdir) == @s_ifdir do
    {{"open_ok", %{"fh" => 0}}, state}
  end

  defp open_dispatch(%{id: file_id}, volume_id, path, state) do
    case claim_pinned_for_path(volume_id, path, state) do
      {:ok, claim_id} -> open_ok(file_id, claim_id, path, state)
      {:error, _reason} -> open_ok(file_id, nil, path, state)
    end
  end

  defp open_ok(file_id, claim_id, path, state) do
    fh = state.next_fh

    new_state = %{
      state
      | next_fh: fh + 1,
        fh_table: Map.put(state.fh_table, fh, %{file_id: file_id, claim_id: claim_id, path: path})
    }

    {{"open_ok", %{"fh" => fh}}, new_state}
  end

  defp create_ok(%{id: file_id, size: size}, claim_id, path, inode, state) do
    fh = state.next_fh

    new_state = %{
      state
      | next_fh: fh + 1,
        fh_table: Map.put(state.fh_table, fh, %{file_id: file_id, claim_id: claim_id, path: path})
    }

    {{"entry_ok", %{"ino" => inode, "size" => size, "kind" => "file", "fh" => fh}}, new_state}
  end

  defp log_create_failure_and_eio(reason, state) do
    Logger.warning("Create failed", reason: inspect(reason))
    {{"error", %{"errno" => errno(:eio)}}, state}
  end

  # Calls into the namespace coordinator on a core node. Returns the
  # claim id on success. On any failure (coordinator unreachable,
  # claim conflict, etc.) we surface the error to the caller so they
  # can map it to an errno.
  defp claim_pinned_for_path(volume_id, path, state) do
    key = volume_scoped_path(volume_id, path)

    core_call(state.coordinator_module, :claim_pinned_for, [
      state.coordinator_module,
      key,
      self()
    ])
  catch
    :exit, _ -> {:error, :coordinator_unavailable}
  end

  defp release_pin_quietly(coordinator, claim_id) do
    case core_call(coordinator, :release, [coordinator, claim_id]) do
      :ok ->
        :ok

      other ->
        Logger.warning("Pin release failed", claim_id: claim_id, reason: inspect(other))
    end
  catch
    :exit, _ -> :ok
  end

  defp volume_scoped_path(volume_id, path), do: "vol:" <> volume_id <> ":" <> path

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

  # Handle read operation: read file data.
  #
  # When the FUSE-side `fh` is one we allocated at `open` /
  # `create` (#651), route through `Core.read_file_by_id` so the
  # cached `file_id` keeps working even if another peer has
  # detached the path (#638). Falls back to the path-based
  # `ChunkReader` flow for legacy callers that read without an
  # explicit open (e.g. some readdir-then-read sequences).
  defp handle_operation({"read", params}, state) do
    ino = params["ino"]
    offset = params["offset"]
    size = params["size"]
    fh = params["fh"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         :ok <- check_file_permission(volume_id, path, :read, state),
         {:ok, data} <- read_via_fh_or_path(volume_id, path, fh, offset, size, state) do
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

  # Handle write operation: write file data. Mirrors `read` —
  # registered `fh` → `Core.write_file_at_by_id`, else path-based
  # fallback. See `read` docstring for the unlink-while-open
  # rationale (#651).
  defp handle_operation({"write", params}, state) do
    ino = params["ino"]
    offset = params["offset"]
    data = params["data"]
    fh = params["fh"]

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         :ok <- check_file_permission(volume_id, path, :write, state),
         {:ok, _file} <- write_via_fh_or_path(volume_id, path, fh, offset, data, state) do
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

  # `create` is stateful — it allocates a file handle and pins the
  # path so the unlink-while-open story (#651) works. Lives in
  # `handle_stateful/2` alongside `open` / `release`.

  # Handle mkdir operation: create a new directory
  defp handle_operation({"mkdir", params}, state) do
    parent = params["parent"]
    name = params["name"]
    dir_mode = create_mode(params["mode"], @s_ifdir, @dir_mode)

    with {:ok, {volume_id, parent_path}} <- resolve_inode(parent, state),
         :ok <- check_file_permission(volume_id, parent_path, :write, state),
         child_path <- build_child_path(parent_path, name),
         {:ok, _file} <- create_empty_file(volume_id, child_path, mode: dir_mode),
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

  # `open` / `release` are stateful — see `handle_stateful/2` and
  # `dispatch_operation/2`. They live outside `handle_operation/2`
  # because they mutate `state.fh_table` and `state.next_fh`.

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
         :ok <-
           file_index_rename(
             volume_id,
             old_parent_path,
             old_name,
             new_parent_path,
             new_name
           ),
         {:ok, _updated_file} <- file_index_update(file.id, path: new_path) do
      # FUSE renames must preserve the inode number — `d_move/2` in the
      # kernel keeps the dentry pointing at the same inode, and any
      # subsequent `getattr` arrives with that inode. Re-pointing the
      # existing entry keeps `InodeTable.get_path/1` returning the new
      # path; `allocate_inode/2` would hand the kernel an inode it has
      # never seen.
      case InodeTable.rename_path(volume_id, old_path, new_path) do
        {:ok, _inode} -> :ok
        {:error, :not_found} -> InodeTable.allocate_inode(volume_id, new_path)
      end

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

  # ─── xattr ops (#671) ─────────────────────────────────────────────
  #
  # The `user.*` namespace is the only one we currently permit.
  # `system.*` and `security.*` carry POSIX ACL / capability semantics
  # that aren't modelled in NeonFS yet — refusing them with EPERM is
  # the documented "not supported" reply (see #280's xattr slice).
  # `trusted.*` is also rejected — it'd require CAP_SYS_ADMIN
  # enforcement we don't have a hook for. Everything outside those
  # namespaces (no dot, or unknown prefix) is rejected to keep the
  # surface area minimal until a real consumer needs it.

  defp handle_operation({"setxattr", params}, state) do
    ino = params["ino"]
    name = params["name"]
    value = params["value"]
    flags = params["flags"] || 0

    with :ok <- validate_xattr_namespace(name),
         {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, file} <- file_index_get_by_path(volume_id, path),
         :ok <- check_xattr_flags(file.xattrs, name, flags),
         {:ok, updated} <- file_index_update(file.id, xattrs: Map.put(file.xattrs, name, value)) do
      refresh_attrs_cache(state.cache_table, volume_id, updated)
      {"ok", %{}}
    else
      {:error, errno_atom} when is_atom(errno_atom) ->
        {"error", %{"errno" => errno(errno_atom)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Setxattr failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  defp handle_operation({"getxattr", params}, state) do
    ino = params["ino"]
    name = params["name"]
    size = params["size"] || 0

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, file} <- file_index_get_by_path(volume_id, path),
         {:ok, value} <- fetch_xattr(file.xattrs, name) do
      reply_with_size_probe(value, size, "xattr")
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, :enodata} ->
        {"error", %{"errno" => errno(:enodata)}}

      {:error, reason} ->
        Logger.warning("Getxattr failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  defp handle_operation({"listxattr", params}, state) do
    ino = params["ino"]
    size = params["size"] || 0

    with {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, file} <- file_index_get_by_path(volume_id, path) do
      list_bytes = encode_xattr_names(Map.keys(file.xattrs))
      reply_with_size_probe(list_bytes, size, "xattr_list")
    else
      {:error, :not_found} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Listxattr failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  defp handle_operation({"removexattr", params}, state) do
    ino = params["ino"]
    name = params["name"]

    with :ok <- validate_xattr_namespace(name),
         {:ok, {volume_id, path}} <- resolve_inode(ino, state),
         {:ok, file} <- file_index_get_by_path(volume_id, path),
         true <- Map.has_key?(file.xattrs, name) || {:error, :enodata},
         {:ok, updated} <-
           file_index_update(file.id, xattrs: Map.delete(file.xattrs, name)) do
      refresh_attrs_cache(state.cache_table, volume_id, updated)
      {"ok", %{}}
    else
      {:error, errno_atom} when is_atom(errno_atom) ->
        {"error", %{"errno" => errno(errno_atom)}}

      {:error, %{class: :not_found}} ->
        {"error", %{"errno" => errno(:enoent)}}

      {:error, reason} ->
        Logger.warning("Removexattr failed", reason: inspect(reason))
        {"error", %{"errno" => errno(:eio)}}
    end
  end

  # Handle unknown operations
  defp handle_operation({operation, _params}, _state) do
    Logger.warning("Unknown FUSE operation", operation: operation)
    {"error", %{"errno" => errno(:enosys)}}
  end

  defp validate_xattr_namespace(<<"user.", _::binary>>), do: :ok
  defp validate_xattr_namespace(_), do: {:error, :eperm}

  defp check_xattr_flags(xattrs, name, flags) do
    create? = Bitwise.band(flags, 0x1) != 0
    replace? = Bitwise.band(flags, 0x2) != 0
    present? = Map.has_key?(xattrs, name)

    cond do
      create? and present? -> {:error, :eexist}
      replace? and not present? -> {:error, :enodata}
      true -> :ok
    end
  end

  defp fetch_xattr(xattrs, name) do
    case Map.fetch(xattrs, name) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, :enodata}
    end
  end

  # POSIX listxattr replies with a NUL-separated and NUL-terminated
  # list of names, e.g. `"user.foo\0user.bar\0"`. An empty xattrs map
  # yields the empty binary, which the kernel treats as "no
  # attributes".
  defp encode_xattr_names(names) do
    names
    |> Enum.sort()
    |> Enum.map_join("", fn name -> name <> <<0>> end)
  end

  # Both GETXATTR and LISTXATTR follow the same size-probe convention:
  # `size == 0` is the kernel asking how big a buffer to allocate;
  # `size > 0` is the real fetch. Reply tags differ so Session can
  # encode the right Response struct.
  defp reply_with_size_probe(bytes, size, tag) do
    actual = byte_size(bytes)

    cond do
      size == 0 ->
        {tag <> "_size", %{"size" => actual}}

      size < actual ->
        {"error", %{"errno" => errno(:erange)}}

      true ->
        {tag <> "_data", %{"data" => bytes}}
    end
  end

  defp refresh_attrs_cache(nil, _volume_id, _file), do: :ok

  defp refresh_attrs_cache(cache_table, volume_id, file) do
    MetadataCache.put_attrs(cache_table, volume_id, file.path, file)
    :ok
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

  # Rename/move dispatching the same way `NeonFS.Core.do_rename/3` does:
  # same parent → `FileIndex.rename`; same name across dirs → `FileIndex.move`;
  # different parent and name → move + rename. Both calls update the
  # `DirectoryEntry` quorum-replicated state, which is what path-based
  # lookups consult — `FileIndex.update(:path)` does not.
  defp file_index_rename(volume_id, old_parent, old_name, new_parent, new_name)
       when old_parent == new_parent do
    core_call(NeonFS.Core.FileIndex, :rename, [volume_id, old_parent, old_name, new_name])
  end

  defp file_index_rename(volume_id, old_parent, name, new_parent, name) do
    core_call(NeonFS.Core.FileIndex, :move, [volume_id, old_parent, new_parent, name])
  end

  defp file_index_rename(volume_id, old_parent, old_name, new_parent, new_name) do
    with :ok <-
           core_call(NeonFS.Core.FileIndex, :move, [volume_id, old_parent, new_parent, old_name]) do
      core_call(NeonFS.Core.FileIndex, :rename, [volume_id, new_parent, old_name, new_name])
    end
  end

  defp file_index_truncate(file_id, new_size, additional_updates) do
    core_call(NeonFS.Core.FileIndex, :truncate, [file_id, new_size, additional_updates])
  end

  defp read_file(volume_id, path, opts) do
    ChunkReader.read_file(volume_id, path, opts)
  end

  # Read / write dispatch helpers for the unlink-while-open story
  # (#651): if the FUSE-side `fh` is one we allocated at `open` /
  # `create` and tracked in `state.fh_table`, route through
  # `Core.read_file_by_id` / `write_file_at_by_id` — which work
  # against detached files. Otherwise fall back to the path-based
  # form so legacy callers (no explicit open) keep working.
  defp read_via_fh_or_path(_volume_id, path, fh, offset, size, state) do
    case Map.get(state.fh_table, fh) do
      %{file_id: file_id} ->
        core_call(NeonFS.Core, :read_file_by_id, [
          state.volume_name,
          file_id,
          [offset: offset, length: size]
        ])

      nil ->
        read_file(state.volume_name, path, offset: offset, length: size)
    end
  end

  defp write_via_fh_or_path(volume_id, path, fh, offset, data, state) do
    case Map.get(state.fh_table, fh) do
      %{file_id: file_id} ->
        core_call(NeonFS.Core, :write_file_at_by_id, [
          state.volume_name,
          file_id,
          offset,
          data
        ])

      nil ->
        core_call(NeonFS.Core.WriteOperation, :write_file_at, [volume_id, path, offset, data])
    end
  end

  # Create an empty file or directory entry. FUSE `create`/`mkdir` both
  # land here — they only ever produce an empty file on core, so we go
  # through `write_file_at/5` with offset 0 rather than the streaming
  # API (which doesn't support erasure-coded volumes yet).
  defp create_empty_file(volume_id, path, opts) do
    core_call(NeonFS.Core.WriteOperation, :write_file_at, [volume_id, path, 0, <<>>, opts])
  end

  # Linux `O_EXCL` — same value across glibc / musl / kernel headers.
  @o_excl 0x80

  # Translate the FUSE-level mode + open flags into the keyword opts
  # `WriteOperation.write_file_at/5` expects. `O_EXCL` (always paired
  # with `O_CREAT` on the FUSE create() path) routes through
  # `claim_create` for cross-node atomicity.
  defp create_write_opts(file_mode, flags) when is_integer(flags) do
    base = [mode: file_mode]

    if band(flags, @o_excl) != 0 do
      [{:create_only, true} | base]
    else
      base
    end
  end

  defp create_write_opts(file_mode, _flags), do: [mode: file_mode]

  # Convert a DateTime to a POSIX timestamp (seconds since epoch)
  defp datetime_to_unix(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp datetime_to_unix(_), do: 0

  # Convert error reason to errno code
  @dialyzer {:nowarn_function, errno: 1}
  defp errno(:eperm), do: 1
  defp errno(:enoent), do: 2
  defp errno(:eio), do: 5
  defp errno(:eacces), do: 13
  defp errno(:eexist), do: 17
  defp errno(:exdev), do: 18
  defp errno(:erange), do: 34
  defp errno(:enosys), do: 38
  defp errno(:enotempty), do: 39
  defp errno(:enodata), do: 61
  defp errno(_), do: 5
end
