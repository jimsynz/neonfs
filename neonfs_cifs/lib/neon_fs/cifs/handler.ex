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
  nothing and surfaces as `ENOENT` on every mutating op.

  Every handler returns `{reply, new_state}`. State threading lets
  ops like `openat` and `fdopendir` mint synthetic 64-bit handles
  the C shim can pass back into subsequent calls (`pread`,
  `readdir`, `close`, `closedir`).

  ## "Must implement" Samba VFS ops

  The "must implement" Samba VFS ops, all 20 of which are covered:

  | Bucket      | Op                                                                   |
  |-------------|----------------------------------------------------------------------|
  | Lifecycle   | `connect`, `disconnect`                                              |
  | Metadata    | `stat`, `lstat`, `fstat`, `fchmod`, `fchown`, `fntimes`              |
  | File I/O    | `openat`, `close`, `pread`, `pwrite`, `ftruncate`                    |
  | Durability  | `fsync`                                                             |
  | Directories | `fdopendir`, `readdir`, `closedir`, `mkdirat`                        |
  | Mutations   | `unlinkat`, `renameat`                                               |
  | Filesystem  | `disk_free`, `fstatvfs`                                              |

  Anything outside this set surfaces as `{:error, :enosys}`. Xattrs,
  locks, and async I/O are follow-up work.

  `fsync` resolves the open handle to its `{volume, path}` and
  drives the shared `NeonFS.Client.sync_file/2` durability barrier, so
  a CIFS `SMB2_FLUSH` blocks until the file's chunks reach the volume's
  `min_copies` durable replicas — identical semantics to FUSE fsync and
  NFS COMMIT.

  [issue-116]: https://harton.dev/project-neon/neonfs/issues/116
  """

  require Logger

  alias NeonFS.CIFS.HandleRegistry
  alias NeonFS.Client.ChunkReader

  @stat_identity_domain "neonfs-cifs-stat-v1"

  @typedoc "Per-connection state — see `NeonFS.CIFS.ConnectionHandler`."
  @type state :: %{
          required(:volume) => String.t() | nil,
          required(:next_handle) => non_neg_integer(),
          required(:files) => %{non_neg_integer() => term()},
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
  # Dot segments also arrive uncanonicalised when smbd stats the
  # synthesised "." / ".." entries of a directory listing
  # (`smbd_dirptr_get_entry` opens `<dir>/.` verbatim), so resolve
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

  # Best-effort: the C shim is tearing down too, so blank the
  # per-connection state and let `handle_close/2` run.
  #
  # Blanked from the live state rather than rebuilt as a second literal of
  # the same shape. The second literal was free to drift from
  # `handle_connection/2`'s, and did — it lost `:files`, so `handle_close/2`
  # raised `KeyError` on its first line for every connection that
  # disconnected cleanly, skipping the sweep that releases handles the shim
  # left open and reporting a normal teardown as a crashed one. Updating the
  # map cannot drop a key the initial state put there.
  defp do_handle(:disconnect, _args, state) do
    {{:ok, %{}}, %{state | volume: nil, next_handle: 1, files: %{}, dirs: %{}}}
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
    with_any_handle(handle, state, fn target ->
      with {:ok, file} <- fetch_meta(target),
           {:ok, stat} <- stat_term(file) do
        {{:ok, %{stat: stat}}, state}
      else
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  defp do_handle(:fchmod, %{"handle" => handle, "mode" => mode}, state)
       when is_integer(mode) do
    with_any_handle(handle, state, fn target ->
      case set_attrs(target, mode: mode) do
        {:ok, _meta} -> {{:ok, %{}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  defp do_handle(:fchown, _args, state) do
    # NeonFS volumes do not yet honour POSIX uid/gid ownership; ACLs
    # ride on the IAM principal model. Returning `:enosys`
    # keeps Samba from mis-applying inherited ACLs based on a
    # spoofed uid/gid until the IAM bridge lands.
    {{:error, :enosys}, state}
  end

  defp do_handle(:fntimes, %{"handle" => handle, "atime" => atime, "mtime" => mtime}, state)
       when is_integer(atime) and is_integer(mtime) do
    with_any_handle(handle, state, fn target ->
      updates = [
        accessed_at: DateTime.from_unix!(atime),
        modified_at: DateTime.from_unix!(mtime)
      ]

      case set_attrs(target, updates) do
        {:ok, _meta} -> {{:ok, %{}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  ## File I/O

  defp do_handle(:openat, %{"path" => path, "flags" => flags} = args, state) do
    create_mode = Map.get(args, "mode", 0o644)

    with_volume(state, fn volume, state ->
      with {:ok, file} <- open_or_create(volume, path, flags, create_mode),
           {:ok, claim_id} <- pin_file(volume, path),
           {:ok, handle} <-
             HandleRegistry.open(volume, file.id, path, flags, claim_id, self()) do
        {{:ok, %{handle: handle}}, state}
      else
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  defp do_handle(:close, %{"handle" => handle}, state) do
    case HandleRegistry.close(handle) do
      :ok -> {{:ok, %{}}, state}
      :error -> {{:error, :ebadf}, state}
    end
  end

  defp do_handle(:pread, %{"handle" => handle, "offset" => offset, "size" => size}, state)
       when is_integer(offset) and is_integer(size) and size >= 0 do
    with_handle(handle, state, fn %{volume: volume, file_id: file_id} ->
      case ChunkReader.read_file_by_id(volume, file_id, offset: offset, length: size) do
        {:ok, data} -> {{:ok, %{data: data}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  defp do_handle(:pwrite, %{"handle" => handle, "offset" => offset, "data" => data}, state)
       when is_integer(offset) and is_binary(data) do
    with_handle(handle, state, fn %{volume: volume, file_id: file_id} ->
      case core_call(NeonFS.Core, :write_file_at_by_id, [volume, file_id, offset, data]) do
        {:ok, _file} -> {{:ok, %{written: byte_size(data)}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  defp do_handle(:fsync, %{"handle" => handle}, state) do
    with_handle(handle, state, fn %{volume: volume, file_id: file_id} ->
      case NeonFS.Client.sync_file_by_id(volume, file_id) do
        :ok -> {{:ok, %{}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  defp do_handle(:ftruncate, %{"handle" => handle, "size" => size}, state)
       when is_integer(size) and size >= 0 do
    with_handle(handle, state, fn %{volume: volume, file_id: file_id} ->
      case core_call(NeonFS.Core, :truncate_file_by_id, [volume, file_id, size]) do
        {:ok, _} -> {{:ok, %{}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
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

        entry = %{volume: volume, path: path, entries: dir_entries(children)}
        state = %{state | dirs: Map.put(state.dirs, handle, entry)}
        {{:ok, %{handle: handle}}, state}
      else
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  defp do_handle(:readdir, %{"handle" => handle}, state) do
    case dir_cursor(state, handle) do
      {:ok, []} ->
        {{:ok, %{eof: true}}, state}

      {:ok, [entry | rest]} ->
        {{:ok, %{entry: entry_term(entry), eof: false}}, advance_cursor(state, handle, rest)}

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
  # concurrent mkdirs across interface nodes serialise.
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
  # combined move-and-rename under a rename claim, so no
  # decomposition happens here.
  #
  # Open handles track paths, so a successful rename must not strand
  # them: smbd's atomic mkdir creates under a tmp name, renames, then
  # fstats the still-open handle (open.c `mkdir_internal`), and an
  # No handle rewriting: file handles are keyed by `{volume, file_id}` in the
  # node-wide registry, so a rename does not move what they refer to. The
  # rewriting this used to do only ever fixed the single-connection case
  # anyway.
  defp do_handle(:renameat, %{"old_path" => old, "new_path" => new}, state) do
    with_volume(state, fn volume, state ->
      case core_call(NeonFS.Core, :rename_file, [volume, old, new]) do
        :ok -> {{:ok, %{}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  ## Filesystem

  # NeonFS volumes are logically unbounded (capacity is a property of the
  # cluster's drives, not the volume), so report a large synthetic capacity —
  # enough that SMB clients don't refuse writes against a "full" share. The
  # FUSE backend punts on statfs the same way. Accurate per-volume/cluster
  # accounting is follow-up work, not a correctness blocker.
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

  # The identity pin keeps the file reachable through this handle
  # across a rename and survives an unlink until every handle closes. A
  # coordinator that cannot issue one is not a reason to refuse the open —
  # the handle simply carries no pin, which is how FUSE behaved before
  # it gained pins. Recorded rather than silently dropped so the registry can
  # tell "no pin" from "pin to release".
  defp pin_file(volume, path) do
    case core_call(NeonFS.Core, :pin_file, [volume, path, self()]) do
      {:ok, %{claim_id: claim_id}} -> {:ok, claim_id}
      {:error, _reason} -> {:ok, nil}
    end
  catch
    :exit, _ -> {:ok, nil}
  end

  defp with_volume(%{volume: nil} = state, _fun), do: {{:error, :enotconn}, state}
  defp with_volume(state, fun), do: fun.(state.volume, state)

  # Every fd-bearing op resolves through the node-wide registry, so a handle
  # opened on one connection works on another and none of them re-resolve a
  # path that a concurrent rename or unlink may have moved.
  defp with_handle(handle, state, fun) do
    case HandleRegistry.fetch(handle) do
      {:ok, entry} -> fun.(entry)
      :error -> {{:error, :ebadf}, state}
    end
  end

  # Directory handles only — file handles are minted by the node-wide
  # registry. Directory-handle pinning is not part of that work.
  defp mint_handle(state) do
    handle = state.next_handle
    {handle, %{state | next_handle: handle + 1}}
  end

  # A directory handle records what it points at as well as where readdir
  # has got to. Attribute ops need the former: a directory's mode and times
  # live in a path-keyed record, so `{volume, path}` is what can resolve
  # them — the file-handle registry has neither, which is why `fchmod` on a
  # directory used to fail `:ebadf` before reaching core at all.
  #
  # The cursor stays here rather than moving to the node-wide registry:
  # readdir position is per-connection iteration state, not something other
  # nodes have any use for.
  defp dir_cursor(state, handle) do
    case Map.fetch(state.dirs, handle) do
      {:ok, %{entries: entries}} -> {:ok, entries}
      :error -> :error
    end
  end

  defp advance_cursor(state, handle, rest) do
    %{state | dirs: Map.update!(state.dirs, handle, &%{&1 | entries: rest})}
  end

  # A file handle updates by id; a directory handle by path, because a
  # directory's attributes live in a path-keyed record the by-id API cannot
  # reach. `Core.update_file_meta/4` dispatches on record type, so the path
  # form serves both.
  # The volume root is the one target with no identity to address: its
  # `FileMeta` carries `id: nil`, so a by-id lookup has nothing to look up
  # and answers an error that surfaces to smbd as EIO. It resolves by name
  # instead. This clause has to come first — an entry carries both keys, so
  # the `file_id` clause below would otherwise match with a nil id.
  defp set_attrs(%{volume: volume, file_id: nil, path: path}, updates) do
    core_call(NeonFS.Core, :update_file_meta, [volume, path, updates])
  end

  defp set_attrs(%{volume: volume, file_id: file_id}, updates) do
    core_call(NeonFS.Core, :update_file_meta_by_id, [volume, file_id, updates])
  end

  defp set_attrs(%{volume: volume, path: path}, updates) do
    core_call(NeonFS.Core, :update_file_meta, [volume, path, updates])
  end

  defp fetch_meta(%{volume: volume, file_id: nil, path: path}) do
    core_call(NeonFS.Core, :get_file_meta, [volume, path])
  end

  defp fetch_meta(%{volume: volume, file_id: file_id}) do
    core_call(NeonFS.Core, :get_file_meta_by_id, [volume, file_id])
  end

  defp fetch_meta(%{volume: volume, path: path}) do
    core_call(NeonFS.Core, :get_file_meta, [volume, path])
  end

  # Resolves a handle to whatever can act on it: a file handle carries a
  # `file_id`, a directory handle a `path`. Callers that can serve both
  # dispatch on which key is present.
  defp with_any_handle(handle, state, fun) do
    case HandleRegistry.fetch(handle) do
      {:ok, entry} ->
        fun.(entry)

      :error ->
        case Map.fetch(state.dirs, handle) do
          {:ok, %{volume: volume, path: path}} -> fun.(%{volume: volume, path: path})
          :error -> {{:error, :ebadf}, state}
        end
    end
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
  # true` so two CIFS interface nodes that
  # both observe `:not_found` can't both win the create — the
  # namespace coordinator's `claim_create` primitive lets
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
