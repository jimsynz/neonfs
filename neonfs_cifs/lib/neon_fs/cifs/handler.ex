defmodule NeonFS.CIFS.Handler do
  @moduledoc """
  Per-VFS-op handler functions for `vfs_neonfs.so`.

  This module is purely a translation layer: it decodes the ETF
  request shape (a `{op_atom, args_map}` tuple), routes the call
  through `neonfs_client` to the cluster, and re-encodes the result
  in the wire reply shape (`{:ok, payload}` or `{:error, errno}`).

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
  | Directories | `fdopendir`, `readdir`, `closedir`, `mkdirat`                        |
  | Mutations   | `unlinkat`, `renameat`                                               |
  | Filesystem  | `disk_free`, `fstatvfs`                                              |

  Anything outside this set surfaces as `{:error, :enosys}`. The
  follow-up sub-issue (#280-equivalent for Samba) covers xattrs,
  locks, and async I/O.

  [issue-116]: https://harton.dev/project-neon/neonfs/issues/116
  """

  require Logger

  alias NeonFS.Client.ChunkReader

  @typedoc "Per-connection state — see `NeonFS.CIFS.ConnectionHandler`."
  @type state :: %{
          required(:volume) => String.t() | nil,
          required(:next_handle) => non_neg_integer(),
          required(:files) => %{non_neg_integer() => {String.t(), String.t(), atom()}},
          required(:dirs) => %{
            non_neg_integer() => {String.t(), String.t(), non_neg_integer()}
          }
        }

  @typedoc "Wire-encoded reply."
  @type reply :: {:ok, term()} | {:error, atom()}

  @doc """
  Dispatch a single decoded request. Returns the reply (which the
  caller frames + sends) and the new connection state.
  """
  @spec handle({atom(), map()}, state()) :: {reply(), state()}
  def handle({op, args}, state) when is_atom(op) and is_map(args), do: do_handle(op, args, state)
  def handle(_, state), do: {{:error, :einval}, state}

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
        case core_call(NeonFS.Core.FileIndex, :update_mode, [volume, path, mode]) do
          {:ok, _} -> {{:ok, %{}}, state}
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
        case core_call(NeonFS.Core.FileIndex, :update_times, [volume, path, atime, mtime]) do
          {:ok, _} -> {{:ok, %{}}, state}
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
        case core_call(NeonFS.Core.WriteOperation, :write_file_at, [volume, path, offset, data]) do
          {:ok, _file} -> {{:ok, %{written: byte_size(data)}}, state}
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
        case core_call(NeonFS.Core.FileIndex, :truncate, [volume, path, size]) do
          {:ok, _} -> {{:ok, %{}}, state}
          {:error, reason} -> {{:error, errno_for(reason)}, state}
        end

      :error ->
        {{:error, :ebadf}, state}
    end
  end

  ## Directories

  defp do_handle(:fdopendir, %{"path" => path}, state) do
    with_volume(state, fn volume, state ->
      case core_call(NeonFS.Core.FileIndex, :get_by_path, [volume, path]) do
        {:ok, _file} ->
          {handle, state} = mint_handle(state)
          state = %{state | dirs: Map.put(state.dirs, handle, {volume, path, 0})}
          {{:ok, %{handle: handle}}, state}

        {:error, reason} ->
          {{:error, errno_for(reason)}, state}
      end
    end)
  end

  defp do_handle(:readdir, %{"handle" => handle}, state) do
    case Map.fetch(state.dirs, handle) do
      {:ok, dir_state} -> readdir_step(dir_state, handle, state)
      :error -> {{:error, :ebadf}, state}
    end
  end

  defp do_handle(:closedir, %{"handle" => handle}, state) do
    case Map.fetch(state.dirs, handle) do
      {:ok, _} -> {{:ok, %{}}, %{state | dirs: Map.delete(state.dirs, handle)}}
      :error -> {{:error, :ebadf}, state}
    end
  end

  defp do_handle(:mkdirat, %{"path" => path} = args, state) do
    mode = Map.get(args, "mode", 0o755)

    with_volume(state, fn volume, state ->
      case core_call(NeonFS.Core.WriteOperation, :create_directory, [volume, path, mode]) do
        {:ok, _} -> {{:ok, %{}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  ## Mutations

  defp do_handle(:unlinkat, %{"path" => path}, state) do
    with_volume(state, fn volume, state ->
      case core_call(NeonFS.Core.WriteOperation, :delete_file, [volume, path]) do
        :ok -> {{:ok, %{}}, state}
        {:ok, _} -> {{:ok, %{}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  defp do_handle(:renameat, %{"old_path" => old, "new_path" => new}, state) do
    with_volume(state, fn volume, state ->
      case core_call(NeonFS.Core.WriteOperation, :rename, [volume, old, new]) do
        :ok -> {{:ok, %{}}, state}
        {:ok, _} -> {{:ok, %{}}, state}
        {:error, reason} -> {{:error, errno_for(reason)}, state}
      end
    end)
  end

  ## Filesystem

  defp do_handle(:disk_free, _args, state) do
    with_volume(state, fn volume, state ->
      case core_call(NeonFS.Core.Volume, :stats, [volume]) do
        {:ok, stats} ->
          {{:ok,
            %{
              total_bytes: Map.get(stats, :total_bytes, 0),
              free_bytes: Map.get(stats, :free_bytes, 0),
              available_bytes: Map.get(stats, :available_bytes, 0)
            }}, state}

        {:error, reason} ->
          {{:error, errno_for(reason)}, state}
      end
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

  defp mint_handle(state) do
    handle = state.next_handle
    {handle, %{state | next_handle: handle + 1}}
  end

  defp readdir_step({volume, path, cursor}, handle, state) do
    case core_call(NeonFS.Core.FileIndex, :list_directory, [volume, path]) do
      {:ok, entries} -> readdir_advance(entries, cursor, volume, path, handle, state)
      {:error, reason} -> {{:error, errno_for(reason)}, state}
    end
  end

  defp readdir_advance(entries, cursor, volume, path, handle, state) do
    case Enum.at(entries, cursor) do
      nil ->
        {{:ok, %{eof: true}}, state}

      entry ->
        new_dirs = Map.put(state.dirs, handle, {volume, path, cursor + 1})
        {{:ok, %{entry: entry_term(entry), eof: false}}, %{state | dirs: new_dirs}}
    end
  end

  defp fetch_stat(volume, path, state) do
    case core_call(NeonFS.Core.FileIndex, :get_by_path, [volume, path]) do
      {:ok, file} -> {{:ok, %{stat: stat_term(file)}}, state}
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

    case core_call(NeonFS.Core.FileIndex, :get_by_path, [volume, path]) do
      {:ok, _file} when o_excl -> {:error, :eexist}
      {:ok, file} -> {:ok, file}
      {:error, :not_found} when o_creat -> create_file(volume, path, mode, o_excl)
      {:error, _} = err -> err
    end
  end

  defp create_file(volume, path, mode, exclusive?) do
    base_opts = [mode: mode]
    opts = if exclusive?, do: [{:create_only, true} | base_opts], else: base_opts

    case core_call(NeonFS.Core.WriteOperation, :write_file_at, [volume, path, 0, <<>>, opts]) do
      {:error, :exists} -> {:error, :eexist}
      other -> other
    end
  end

  defp stat_term(file) do
    %{
      size: Map.get(file, :size, 0),
      mode: Map.get(file, :mode, 0o644),
      atime: time_to_unix(Map.get(file, :accessed_at)),
      mtime: time_to_unix(Map.get(file, :modified_at)),
      ctime: time_to_unix(Map.get(file, :changed_at)),
      kind: kind_of(Map.get(file, :mode, 0o100644))
    }
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
  defp errno_for(:directory_not_empty), do: :enotempty
  defp errno_for(:cross_volume), do: :exdev
  defp errno_for(:io_error), do: :eio
  defp errno_for(reason) when is_atom(reason), do: reason
  defp errno_for(_), do: :eio

  defp core_call(module, function, args) do
    NeonFS.Client.core_call(module, function, args)
  end
end
