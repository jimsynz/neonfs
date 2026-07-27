defmodule NeonFS.CIFS.Handler do
  @moduledoc """
  Per-VFS-op handler functions for `vfs_neonfs.so`.

  This module is purely a translation layer: it decodes the ETF
  request shape (a `{op_atom, args_map}` tuple), routes the call
  through `neonfs_client` to the cluster, and re-encodes the result
  in the wire reply shape (`{:ok, payload}` or `{:error, errno}`).

  All core access goes through the `NeonFS.Core` RPC facade with the
  volume **name** bound at `connect` — the facade resolves names to
  volume ids, authorises, and routes to a volume-affine core node.
  Calling id-keyed internals (`NeonFS.Core.FileIndex` /
  `NeonFS.Core.WriteOperation`) with the name instead resolves
  nothing and surfaces as `ENOENT` on every mutating op (#1555).

  Every handler returns `{reply, new_state}`. State threading lets
  ops like `openat` and `fdopendir` mint synthetic 64-bit handles
  the C shim can pass back into subsequent calls (`pread`,
  `readdir`, `close`, `closedir`).

  ## "Must implement" Samba VFS ops

  See [`#116`'s "Must implement" list][issue-116]. The first slice
  covers all 20:

  | Bucket      | Op                                                                   |
  |-------------|----------------------------------------------------------------------|
  | Lifecycle   | `connect`, `disconnect`                                              |
  | Metadata    | `stat`, `lstat`, `fstat`, `fchmod`, `fchown`, `fntimes`              |
  | File I/O    | `openat`, `close`, `pread`, `pwrite`, `ftruncate`                    |
  | Durability  | `fsync`                                                             |
  | Directories | `fdopendir`, `readdir`, `closedir`, `mkdirat`                        |
  | Mutations   | `unlinkat`, `renameat`                                               |
  | Filesystem  | `disk_free`, `fstatvfs`                                              |

  Anything outside this set surfaces as `{:error, :enosys}`. The
  follow-up sub-issue (#280-equivalent for Samba) covers xattrs,
  locks, and async I/O.

  `fsync` (#1503) resolves the open handle to its `{volume, path}` and
  drives the shared `NeonFS.Client.sync_file/2` durability barrier, so
  a CIFS `SMB2_FLUSH` blocks until the file's chunks reach the volume's
  `min_copies` durable replicas — identical semantics to FUSE fsync and
  NFS COMMIT (#1455).

  [issue-116]: https://harton.dev/project-neon/neonfs/issues/116
  """

  require Logger

  alias NeonFS.Client.ChunkReader

  @stat_identity_domain "neonfs-cifs-stat-v1"

  @typedoc "Per-connection state — see `NeonFS.CIFS.ConnectionHandler`."
  @type state :: %{
          required(:volume) => String.t() | nil,
          required(:next_handle) => non_neg_integer(),
          required(:files) => %{non_neg_integer() => {String.t(), String.t(), atom()}},
          required(:dirs) => %{
            non_neg_integer() => [{String.t(), String.t(), non_neg_integer()}]
          }
        }

  @typedoc "Wire-encoded reply."
  @type reply :: {:ok, term()} | {:error, atom()}

  @doc """
  Dispatch a single decoded request. Returns the reply (which the
  caller frames + sends) and the new connection state.
  """
  @spec handle({atom(), map()}, state()) :: {reply(), state()}
  def handle({op, args}, state) when is_atom(op) and is_map(args),
    do: do_handle(op, normalise_paths(args), state)

  def handle(_, state), do: {{:error, :einval}, state}

  # Samba hands the VFS share-relative paths: the share root as "." and entries
  # without a leading slash (`d`, `d/a.txt`). NeonFS core uses absolute paths
  # rooted at "/", so normalise every path argument at ingress — otherwise the
  # share root resolves to `get_by_path(volume, ".")`, which core can't map to
  # the volume root, and every operation fails with OBJECT_PATH_NOT_FOUND
  # (#1550). Dot segments also arrive uncanonicalised when smbd stats the
  # synthesised "." / ".." entries of a directory listing
  # (`smbd_dirptr_get_entry` opens `<dir>/.` verbatim — #1555), so resolve
  # those here too.
  @path_keys ~w(path old_path new_path)
  defp normalise_paths(args) do
    Enum.reduce(@path_keys, args, fn key, acc ->
      case acc do
        %{^key => p} when is_binary(p) -> Map.put(acc, key, to_core_path(p))
        _ -> acc
      end
    end)
  end

  defp to_core_path(p) when p in [".", ""], do: "/"

  defp to_core_path(p) do
    segments =
      p
      |> String.split("/", trim: true)
      |> Enum.reduce([], fn
        ".", acc -> acc
        "..", acc -> Enum.drop(acc, 1)
        segment, acc -> [segment | acc]
      end)

    "/" <> (segments |> Enum.reverse() |> Enum.join("/"))
  end

  ## Lifecycle

  defp do_handle(:connect, %{"volume" => volume}, state) when is_binary(volume) do
    {{:ok, %{}}, %{state | volume: volume}}
  end

  defp do_handle(:disconnect, _args, _state) do
    # Best-effort: C shim is also tearing down, so we just blank
    # the per-connection state and let `handle_close/2` run.
    {{:ok, %{}}, %{volume: nil, next_handle: 1, files: %{}, dirs: %{}}}
  end

  ## Metadata

  defp do_handle(:stat, %{"path" => path}, state),
    do: with_volume(state, &fetch_stat(&1, path, &2))

  defp do_handle(:lstat, %{"path" => path}, state),
    # NFS / NeonFS does not yet model symlinks separately from regular
    # files; lstat falls through to stat. The C shim is free to add
    # symlink semantics in the follow-up xattr/symlink slice.
    do: with_volume(state, &fetch_stat(&1, path, &2))

  defp do_handle(:fstat, %{"handle" => handle}, state) do
    case Map.fetch(state.files, handle) do
      {:ok, {volume, path, _flags}} -> fetch_stat(volume, path, state)
      :error -> {{:error, :ebadf}, state}
    end
  end

  defp do_handle(:fchmod, %{"handle" => handle, "mode" => mode}, state)
       when is_integer(mode) do
    case Map.fetch(state.files, handle) do
      {:ok, {volume, path, _flags}} ->
        case core_call(NeonFS.Core, :update_file_meta, [volume, path, [mode: mode]]) do
          {:ok, _meta} -> {{:ok, %{}}, state}
          {:error, reason} -> {{:error, errno_for(reason)}, state}
        end

      :error ->
        {{:error, :ebadf}, state}
    end
  end

  defp do_handle(:fchown, _args, state) do
    # NeonFS volumes do not yet honour POSIX uid/gid ownership; ACLs
    # ride on the IAM principal model (#135). Returning `:enosys`
    # keeps Samba from mis-applying inherited ACLs based on a
    # spoofed uid/gid until the IAM bridge lands.
    {{:error, :enosys}, state}
  end

  defp do_handle(:fntimes, %{"handle" => handle, "atime" => atime, "mtime" => mtime}, state)
       when is_integer(atime) and is_integer(mtime) do
    case Map.fetch(state.files, handle) do
      {:ok, {volume, path, _flags}} ->
        updates = [
          accessed_at: DateTime.from_unix!(atime),
          modified_at: DateTime.from_unix!(mtime)
        ]

        case core_call(NeonFS.Core, :update_file_meta, [volume, path, updates]) do
          {:ok, _meta} -> {{:ok, %{}}, state}
          {:error, reason} -> {{:error, errno_for(reason)}, state}
        end

      :error ->
        {{:error, :ebadf}, state}
    end
  end

  ## File I/O

  defp do_handle(:openat, %{"path" => path, "flags" => flags} = args, state) do
    create_mode = Map.get(args, "mode", 0o644)

    with_volume(state, fn volume, state ->
      case open_or_create(volume, path, flags, create_mode) do
        {:ok, _file} ->
          {handle, state} = mint_handle(state)
          state = %{state | files: Map.put(state.files, handle, {volume, path, flags})}
          {{:ok, %{handle: handle}}, state}

        {:error, reason} ->
          {{:error, errno_for(reason)}, state}
      end
    end)
  end

  defp do_handle(:close, %{"handle" => handle}, state) do
    case Map.fetch(state.files, handle) do
      {:ok, _} -> {{:ok, %{}}, %{state | files: Map.delete(state.files, handle)}}
      :error -> {{:error, :ebadf}, state}
    end
  end

  defp do_handle(:pread, %{"handle" => handle, "offset" => offset, "size" => size}, state)
       when is_integer(offset) and is_integer(size) and size >= 0 do
    case Map.fetch(state.files, handle) do
      {:ok, {volume, path, _flags}} ->
        case ChunkReader.read_file(volume, path, offset: offset, length: size) do
          {:ok, data} -> {{:ok, %{data: data}}, state}
          {:error, reason} -> {{:error, errno_for(reason)}, state}
        end

      :error ->
        {{:error, :ebadf}, state}
    end
  end

  defp do_handle(:pwrite, %{"handle" => handle, "offset" => offset, "data" => data}, state)
       when is_integer(offset) and is_binary(data) do
    case Map.fetch(state.files, handle) do
      {:ok, {volume, path, _flags}} ->
        case core_call(NeonFS.Core, :write_file_at, [volume, path, offset, data]) do
          {:ok, _file} -> {{:ok, %{written: byte_size(data)}}, state}
          {:error, reason} -> {{:error, errno_for(reason)}, state}
        end

      :error ->
        {{:error, :ebadf}, state}
    end
  end

  defp do_handle(:fsync, %{"handle" => handle}, state) do
    case Map.fetch(state.files, handle) do
      {:ok, {volume, path, _flags}} ->
        case NeonFS.Client.sync_file(volume, path) do
          :ok -> {{:ok, %{}}, state}
          {:error, reason} -> {{:error, errno_for(reason)}, state}
        end

      :error ->
        {{:error, :ebadf}, state}
    end
  end

  defp do_handle(:ftruncate, %{"handle" => handle, "size" => size}, state)
       when is_integer(size) and size >= 0 do
    case Map.fetch(state.files, handle) do
      {:ok, {volume, path, _flags}} ->
        case core_call(NeonFS.Core, :truncate_file, [volume, path, size]) do
          {:ok, _} -> {{:ok, %{}}, state}
          {:error, reason} -> {{:error, errno_for(reason)}, state}
        end

      :error ->
        {{:error, :ebadf}, state}
    end
  end

  ## Directories

  # smbd drives `readdir` one dirent at a time, so the listing is
  # fetched and sorted once here and snapshotted into the dir handle;
  # each `readdir` then just pops the head. (The previous design
  # re-fetched and re-sorted the whole listing per entry — O(n²) RPCs
  # for an n-entry directory.)
  defp do_handle(:fdopendir, %{"path" => path}, state) do
    with_volume(state, fn volume, state ->
      with {:ok, _file} <- core_call(NeonFS.Core, :get_file_meta, [volume, path]),
           {:ok, children} <- core_call(NeonFS.Core, :list_dir, [volume, path]) do
        {handle, state} = mint_handle(state)
        state = %{state | dirs: Map.put(state.dirs, handle, dir_entries(children))}
        {{:ok, %{handle: handle}}, state}
      else
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  defp do_handle(:readdir, %{"handle" => handle}, state) do
    case Map.fetch(state.dirs, handle) do
      {:ok, []} ->
        {{:ok, %{eof: true}}, state}

      {:ok, [entry | rest]} ->
        {{:ok, %{entry: entry_term(entry), eof: false}},
         %{state | dirs: Map.put(state.dirs, handle, rest)}}

      :error ->
        {{:error, :ebadf}, state}
    end
  end

  defp do_handle(:closedir, %{"handle" => handle}, state) do
    case Map.fetch(state.dirs, handle) do
      {:ok, _} -> {{:ok, %{}}, %{state | dirs: Map.delete(state.dirs, handle)}}
      :error -> {{:error, :ebadf}, state}
    end
  end

  # `NeonFS.Core.mkdir/3` is the canonical directory create (the same
  # entry point NFS MKDIR uses): it takes the plain permission bits and
  # stores a `dir:` record under a namespace-coordinator claim, so
  # concurrent mkdirs across interface nodes serialise (#305).
  defp do_handle(:mkdirat, %{"path" => path} = args, state) do
    mode = Map.get(args, "mode", 0o755)

    with_volume(state, fn volume, state ->
      case core_call(NeonFS.Core, :mkdir, [volume, path, [mode: mode]]) do
        {:ok, _} -> {{:ok, %{}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  ## Mutations

  # `delete_file/2` dispatches on path type, so smbd's `unlinkat` and
  # `rmdir` share the one op (the same entry point NFS REMOVE/RMDIR use).
  defp do_handle(:unlinkat, %{"path" => path}, state) do
    with_volume(state, fn volume, state ->
      case core_call(NeonFS.Core, :delete_file, [volume, path]) do
        :ok -> {{:ok, %{}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  # `rename_file/3` handles same-parent renames, cross-parent moves, and
  # combined move-and-rename under a rename claim (#304), so no
  # decomposition happens here.
  #
  # Open handles track paths, so a successful rename must not strand
  # them: smbd's atomic mkdir creates under a tmp name, renames, then
  # fstats the still-open handle (open.c `mkdir_internal`), and an
  # SETINFO rename likewise targets an already-open file. Rewrite any
  # handle whose path is the renamed entry or lives beneath it (#1555).
  defp do_handle(:renameat, %{"old_path" => old, "new_path" => new}, state) do
    with_volume(state, fn volume, state ->
      case core_call(NeonFS.Core, :rename_file, [volume, old, new]) do
        :ok -> {{:ok, %{}}, rewrite_handle_paths(state, old, new)}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  ## Filesystem

  # NeonFS volumes are logically unbounded (capacity is a property of the
  # cluster's drives, not the volume), so report a large synthetic capacity —
  # enough that SMB clients don't refuse writes against a "full" share. The
  # FUSE backend punts on statfs the same way. Accurate per-volume/cluster
  # accounting is a follow-up (#1554-class work), not a correctness blocker.
  @synthetic_capacity 1024 * 1024 * 1024 * 1024 * 1024
  defp do_handle(:disk_free, _args, state) do
    with_volume(state, fn _volume, state ->
      {{:ok,
        %{
          total_bytes: @synthetic_capacity,
          free_bytes: @synthetic_capacity,
          available_bytes: @synthetic_capacity
        }}, state}
    end)
  end

  defp do_handle(:fstatvfs, args, state), do: do_handle(:disk_free, args, state)

  ## Catch-all

  defp do_handle(op, _args, state) do
    Logger.debug("CIFS unknown op", operation: op)
    {{:error, :enosys}, state}
  end

  ## Helpers

  defp with_volume(%{volume: nil} = state, _fun), do: {{:error, :enotconn}, state}
  defp with_volume(state, fun), do: fun.(state.volume, state)

  defp rewrite_handle_paths(state, old, new) do
    files =
      Map.new(state.files, fn {handle, {volume, path, flags}} ->
        {handle, {volume, rewrite_path(path, old, new), flags}}
      end)

    %{state | files: files}
  end

  defp rewrite_path(path, old, new) do
    cond do
      path == old -> new
      String.starts_with?(path, old <> "/") -> new <> String.trim_leading(path, old)
      true -> path
    end
  end

  defp mint_handle(state) do
    handle = state.next_handle
    {handle, %{state | next_handle: handle + 1}}
  end

  # `NeonFS.Core.list_dir/2` returns `[FileMeta]` (directory children
  # synthesised with the S_IFDIR bit set). Flatten to name-sorted
  # `{name, path, mode}` tuples once, at `fdopendir`, so each `readdir`
  # pops the head without touching core.
  defp dir_entries(children) when is_list(children) do
    children
    |> Enum.map(fn meta -> {Path.basename(meta.path), meta.path, meta.mode} end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp fetch_stat(volume, path, state) do
    with {:ok, file} <- core_call(NeonFS.Core, :get_file_meta, [volume, path]),
         {:ok, stat} <- stat_term(file) do
      {{:ok, %{stat: stat}}, state}
    else
      {:error, reason} -> {{:error, errno_for(reason)}, state}
    end
  end

  # `O_CREAT` (0o100) plus `O_EXCL` (0o200) → exclusive create. Plain
  # `O_CREAT` → create if missing. Anything else is open-existing
  # (the Samba shim's `vfs_open` issues these flags from its own
  # POSIX-style open call).
  #
  # Exclusive create routes through `WriteOperation`'s `create_only:
  # true` (sub-issue #595 of #303) so two CIFS interface nodes that
  # both observe `:not_found` can't both win the create — the
  # namespace coordinator's `claim_create` primitive (#591) lets
  # exactly one through and surfaces `{:error, :exists}` to the
  # other, which we map back to `:eexist`.
  defp open_or_create(volume, path, flags, mode) do
    o_creat = Bitwise.band(flags, 0o100) != 0
    o_excl = Bitwise.band(flags, 0o200) != 0

    case core_call(NeonFS.Core, :get_file_meta, [volume, path]) do
      {:ok, _file} when o_excl -> {:error, :eexist}
      {:ok, file} -> {:ok, file}
      {:error, %{class: :not_found}} when o_creat -> create_file(volume, path, mode, o_excl)
      {:error, _} = err -> err
    end
  end

  defp create_file(volume, path, mode, exclusive?) do
    base_opts = [mode: mode]
    opts = if exclusive?, do: [{:create_only, true} | base_opts], else: base_opts

    case core_call(NeonFS.Core, :write_file_at, [volume, path, 0, <<>>, opts]) do
      {:error, %NeonFS.Error.AlreadyExists{}} -> {:error, :eexist}
      other -> other
    end
  end

  defp stat_term(file) do
    with {:ok, device, inode} <- stat_identity(file) do
      {:ok,
       %{
         dev: device,
         ino: inode,
         size: Map.get(file, :size, 0),
         mode: Map.get(file, :mode, 0o644),
         atime: time_to_unix(Map.get(file, :accessed_at)),
         mtime: time_to_unix(Map.get(file, :modified_at)),
         ctime: time_to_unix(Map.get(file, :changed_at)),
         kind: kind_of(Map.get(file, :mode, 0o100644))
       }}
    end
  end

  defp stat_identity(%{volume_id: volume_id, path: "/"}) when is_binary(volume_id) do
    {:ok, stable_stat_id("dev", volume_id, [0]), 1}
  end

  defp stat_identity(%{volume_id: volume_id, id: id})
       when is_binary(volume_id) and is_binary(id) do
    {:ok, stable_stat_id("dev", volume_id, [0]),
     stable_stat_id("ino", volume_id <> <<0>> <> id, [0, 1])}
  end

  defp stat_identity(_file), do: {:error, :eio}

  defp stable_stat_id(kind, material, reserved, nonce \\ 0) do
    <<id::unsigned-big-64, _::binary>> =
      :crypto.hash(
        :sha256,
        [@stat_identity_domain, 0, kind, 0, <<nonce::unsigned-big-32>>, material]
      )

    if id in reserved, do: stable_stat_id(kind, material, reserved, nonce + 1), else: id
  end

  defp entry_term({name, _path, mode}), do: %{name: name, kind: kind_of(mode)}

  defp time_to_unix(nil), do: 0
  defp time_to_unix(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp time_to_unix(n) when is_integer(n), do: n
  defp time_to_unix(_), do: 0

  defp kind_of(mode) when is_integer(mode) do
    cond do
      Bitwise.band(mode, 0o170000) == 0o040000 -> :directory
      Bitwise.band(mode, 0o170000) == 0o100000 -> :file
      true -> :file
    end
  end

  defp kind_of(_), do: :file

  # Map miscellaneous backend errors onto Samba-style POSIX errno
  # atoms. Atoms not in this list pass through unchanged so the C
  # shim can recognise NeonFS-specific reasons without surprise.
  defp errno_for(:not_found), do: :enoent
  defp errno_for(%{class: :not_found}), do: :enoent
  defp errno_for(:forbidden), do: :eacces
  defp errno_for(%{class: :forbidden}), do: :eacces
  defp errno_for(:already_exists), do: :eexist
  defp errno_for(%NeonFS.Error.AlreadyExists{}), do: :eexist
  defp errno_for(%NeonFS.Error.Conflict{}), do: :eagain
  defp errno_for(:directory_not_empty), do: :enotempty
  defp errno_for(:cross_volume), do: :exdev
  defp errno_for(:io_error), do: :eio
  defp errno_for(reason) when is_atom(reason), do: reason
  defp errno_for(_), do: :eio

  defp core_call(module, function, args) do
    NeonFS.Client.core_call(module, function, args)
  end
end
