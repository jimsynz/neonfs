defmodule NFSServer.NFSv3.Handler do
  @moduledoc """
  ONC RPC handler for NFS v3 (program 100003, version 3) — see
  [RFC 1813 §3](https://www.rfc-editor.org/rfc/rfc1813#section-3).

  This handler implements:

  | Proc | Name        |
  |------|-------------|
  | 0    | NULL        |
  | 1    | GETATTR     |
  | 2    | SETATTR     |
  | 3    | LOOKUP      |
  | 4    | ACCESS      |
  | 5    | READLINK    |
  | 6    | READ        |
  | 7    | WRITE       |
  | 8    | CREATE      |
  | 9    | MKDIR       |
  | 12   | REMOVE      |
  | 13   | RMDIR       |
  | 14   | RENAME      |
  | 16   | READDIR     |
  | 17   | READDIRPLUS |
  | 18   | FSSTAT      |
  | 19   | FSINFO      |
  | 20   | PATHCONF    |
  | 21   | COMMIT      |

  The remaining write-path procedures (procs 10, 11, 15) ship in
  separate sub-issues under the #285 tracking issue.

  ## READ streaming

  READ (proc 6) returns an iolist body — `Backend.read/5` hands back
  a lazy `Enumerable.t()` of binary chunks plus an EOF flag. The
  handler accumulates chunks up to the kernel's `count` cap (typically
  ≤ 1 MiB, never the whole file), builds the reply body as nested
  iolist (`status | post_op_attr | count | eof | xdr-opaque chunks`),
  and lets `RPC.RecordMarking.encode/1` propagate the iolist all the
  way to `:gen_tcp.send/2`. Per `CLAUDE.md`, we never materialise an
  unbounded file into a single binary.

  ## Cookie pagination (READDIR / READDIRPLUS)

  Both directory-iteration procs use the same scheme: the client
  passes a `cookie` (initially zero) plus an opaque `cookieverf3`
  back to the server, which returns a slice of entries plus a new
  cookie for the next call. The handler treats cookies as opaque —
  the backend chooses the encoding (offset, name, inode, …). The
  `cookieverf3` is typically the directory's mtime so a writer
  invalidates outstanding paginations on the next mtime bump.

  When the backend's verifier disagrees with the client's, the
  handler maps that to `NFS3ERR_BAD_COOKIE` (10003) so the client
  knows to restart pagination from cookie 0.

  Filesystem decisions are delegated to a `NFSServer.NFSv3.Backend`
  module; the handler stays NeonFS-agnostic. Bind a backend via
  `with_backend/1`:

      programs = %{100_003 => %{3 => NFSServer.NFSv3.Handler.with_backend(MyBackend)}}

  Same shape as `NFSServer.Mount.Handler.with_backend/1`. Tests can
  alternatively pre-stamp `:nfs_v3_backend` onto the dispatcher's
  `ctx` and invoke this module directly.

  ## Procedure layout

  Every procedure follows the same shape: XDR-decode the args via
  helpers in `NFSServer.NFSv3.Types`, invoke the backend callback,
  XDR-encode the reply. RFC 1813 reply unions all share the
  `nfsstat3` discriminant on the wire — we encode the
  status integer first, then the OK or FAIL arm.

  Errors from the backend that don't include a `post_op_attr`
  (e.g. `{:error, :stale}`) get `nil` in the post-op slot, which
  encodes as the FALSE-flag arm of `post_op_attr`.

  ## ACCESS3_* bit flags

  ACCESS uses bitmasks per RFC 1813 §3.3.4. The constants are
  exposed as module attributes so backends and tests can reference
  them by name:

  | Flag                | Mask     |
  |---------------------|----------|
  | `ACCESS3_READ`      | `0x0001` |
  | `ACCESS3_LOOKUP`    | `0x0002` |
  | `ACCESS3_MODIFY`    | `0x0004` |
  | `ACCESS3_EXTEND`    | `0x0008` |
  | `ACCESS3_DELETE`    | `0x0010` |
  | `ACCESS3_EXECUTE`   | `0x0020` |
  """

  @behaviour NFSServer.RPC.Handler

  alias NFSServer.NFSv3.Types
  alias NFSServer.XDR

  @program 100_003
  @version 3

  @proc_null 0
  @proc_getattr 1
  @proc_setattr 2
  @proc_lookup 3
  @proc_access 4
  @proc_readlink 5
  @proc_read 6
  @proc_write 7
  @proc_create 8
  @proc_mkdir 9
  @proc_remove 12
  @proc_rmdir 13
  @proc_rename 14
  @proc_readdir 16
  @proc_readdirplus 17
  @proc_fsstat 18
  @proc_fsinfo 19
  @proc_pathconf 20
  @proc_commit 21

  @doc "ACCESS3_READ — permission to read file data or list a directory."
  @spec access3_read() :: 0x0001
  def access3_read, do: 0x0001

  @doc "ACCESS3_LOOKUP — permission to look up a name within a directory."
  @spec access3_lookup() :: 0x0002
  def access3_lookup, do: 0x0002

  @doc "ACCESS3_MODIFY — permission to rewrite an existing file or directory."
  @spec access3_modify() :: 0x0004
  def access3_modify, do: 0x0004

  @doc "ACCESS3_EXTEND — permission to grow a file or add an entry to a directory."
  @spec access3_extend() :: 0x0008
  def access3_extend, do: 0x0008

  @doc "ACCESS3_DELETE — permission to remove an entry from a directory."
  @spec access3_delete() :: 0x0010
  def access3_delete, do: 0x0010

  @doc "ACCESS3_EXECUTE — permission to execute a file (search a directory does not use this)."
  @spec access3_execute() :: 0x0020
  def access3_execute, do: 0x0020

  @doc "NFS program number (always 100003)."
  @spec program() :: 100_003
  def program, do: @program

  @doc "NFS version this handler implements (always 3)."
  @spec version() :: 3
  def version, do: @version

  @doc """
  Build a thin handler module that dispatches to `backend`. Same
  shape as `NFSServer.Mount.Handler.with_backend/1`.
  """
  @spec with_backend(module()) :: module()
  def with_backend(backend) when is_atom(backend) do
    suffix = backend |> Module.split() |> Enum.join("_")
    name = Module.concat([__MODULE__, "Bound", suffix])

    case Code.ensure_loaded(name) do
      {:module, _} ->
        name

      _ ->
        contents =
          quote do
            @behaviour NFSServer.RPC.Handler
            @backend unquote(backend)

            @impl true
            def handle_call(proc, args, auth, ctx) do
              ctx = Map.put(ctx, :nfs_v3_backend, @backend)
              NFSServer.NFSv3.Handler.handle_call(proc, args, auth, ctx)
            end
          end

        {:module, ^name, _bin, _exports} =
          Module.create(name, contents, Macro.Env.location(__ENV__))

        name
    end
  end

  @impl true
  def handle_call(@proc_null, _args, _auth, _ctx), do: {:ok, <<>>}

  def handle_call(@proc_getattr, args, auth, ctx) do
    with_fhandle(args, &do_getattr(&1, auth, ctx))
  end

  def handle_call(@proc_setattr, args, auth, ctx) do
    case decode_setattr_args(args) do
      {:ok, fh, sattr, guard} -> do_setattr(fh, sattr, guard, auth, ctx)
      :error -> :garbage_args
    end
  end

  def handle_call(@proc_lookup, args, auth, ctx) do
    case Types.decode_diropargs3(args) do
      {:ok, {dir, name}, _} -> do_lookup(dir, name, auth, ctx)
      {:error, _} -> :garbage_args
    end
  end

  def handle_call(@proc_access, args, auth, ctx) do
    case decode_access_args(args) do
      {:ok, fh, mask} -> do_access(fh, mask, auth, ctx)
      :error -> :garbage_args
    end
  end

  def handle_call(@proc_readlink, args, auth, ctx) do
    with_fhandle(args, &do_readlink(&1, auth, ctx))
  end

  def handle_call(@proc_read, args, auth, ctx) do
    case decode_read_args(args) do
      {:ok, fh, offset, count} -> do_read(fh, offset, count, auth, ctx)
      :error -> :garbage_args
    end
  end

  def handle_call(@proc_create, args, auth, ctx) do
    case decode_create_args(args) do
      {:ok, dir, name, mode} -> do_create(dir, name, mode, auth, ctx)
      :error -> :garbage_args
    end
  end

  def handle_call(@proc_mkdir, args, auth, ctx) do
    case decode_mkdir_args(args) do
      {:ok, dir, name, sattr} -> do_mkdir(dir, name, sattr, auth, ctx)
      :error -> :garbage_args
    end
  end

  def handle_call(@proc_remove, args, auth, ctx) do
    case Types.decode_diropargs3(args) do
      {:ok, {dir, name}, _} -> do_remove(dir, name, auth, ctx)
      {:error, _} -> :garbage_args
    end
  end

  def handle_call(@proc_rmdir, args, auth, ctx) do
    case Types.decode_diropargs3(args) do
      {:ok, {dir, name}, _} -> do_rmdir(dir, name, auth, ctx)
      {:error, _} -> :garbage_args
    end
  end

  def handle_call(@proc_rename, args, auth, ctx) do
    case decode_rename_args(args) do
      {:ok, from_dir, from_name, to_dir, to_name} ->
        do_rename(from_dir, from_name, to_dir, to_name, auth, ctx)

      :error ->
        :garbage_args
    end
  end

  def handle_call(@proc_write, args, auth, ctx) do
    case decode_write_args(args) do
      {:ok, fh, offset, data, stable} -> do_write(fh, offset, data, stable, auth, ctx)
      :error -> :garbage_args
    end
  end

  def handle_call(@proc_commit, args, auth, ctx) do
    case decode_commit_args(args) do
      {:ok, fh, offset, count} -> do_commit(fh, offset, count, auth, ctx)
      :error -> :garbage_args
    end
  end

  def handle_call(@proc_readdir, args, auth, ctx) do
    case decode_readdir_args(args) do
      {:ok, fh, cookie, verf, count} -> do_readdir(fh, cookie, verf, count, auth, ctx)
      :error -> :garbage_args
    end
  end

  def handle_call(@proc_readdirplus, args, auth, ctx) do
    case decode_readdirplus_args(args) do
      {:ok, fh, cookie, verf, dircount, maxcount} ->
        do_readdirplus(fh, cookie, verf, dircount, maxcount, auth, ctx)

      :error ->
        :garbage_args
    end
  end

  def handle_call(@proc_fsstat, args, auth, ctx) do
    with_fhandle(args, &do_fsstat(&1, auth, ctx))
  end

  def handle_call(@proc_fsinfo, args, auth, ctx) do
    with_fhandle(args, &do_fsinfo(&1, auth, ctx))
  end

  def handle_call(@proc_pathconf, args, auth, ctx) do
    with_fhandle(args, &do_pathconf(&1, auth, ctx))
  end

  def handle_call(_proc, _args, _auth, _ctx), do: :proc_unavail

  # ——— Decode helpers ———————————————————————————————————————————

  defp with_fhandle(args, fun) do
    case Types.decode_fhandle3(args) do
      {:ok, fh, _} -> fun.(fh)
      {:error, _} -> :garbage_args
    end
  end

  defp decode_access_args(args) do
    with {:ok, fh, rest} <- Types.decode_fhandle3(args),
         {:ok, mask, _} <- XDR.decode_uint(rest) do
      {:ok, fh, mask}
    else
      {:error, _} -> :error
    end
  end

  defp decode_read_args(args) do
    with {:ok, fh, rest} <- Types.decode_fhandle3(args),
         {:ok, offset, rest} <- XDR.decode_uhyper(rest),
         {:ok, count, _} <- XDR.decode_uint(rest) do
      {:ok, fh, offset, count}
    else
      {:error, _} -> :error
    end
  end

  # CREATE args: `diropargs3 + createhow3` (RFC 1813 §3.3.8).
  defp decode_create_args(args) do
    with {:ok, {dir, name}, rest} <- Types.decode_diropargs3(args),
         {:ok, mode, _rest} <- Types.decode_createhow3(rest) do
      {:ok, dir, name, mode}
    else
      {:error, _} -> :error
    end
  end

  # WRITE args: `fhandle3 + offset + count + stable + opaque<>` (RFC
  # 1813 §3.3.7). The `count` field on the wire bounds `data`'s
  # length; trim before passing to the backend so the backend can
  # rely on `byte_size(data) <= count`.
  defp decode_write_args(args) do
    with {:ok, fh, rest} <- Types.decode_fhandle3(args),
         {:ok, offset, rest} <- XDR.decode_uhyper(rest),
         {:ok, count, rest} <- XDR.decode_uint(rest),
         {:ok, stable, rest} <- Types.decode_stable_how(rest),
         {:ok, data, _rest} <- XDR.decode_var_opaque(rest) do
      trimmed = if byte_size(data) > count, do: binary_part(data, 0, count), else: data
      {:ok, fh, offset, trimmed, stable}
    else
      {:error, _} -> :error
    end
  end

  # COMMIT args: `fhandle3 + offset + count` (RFC 1813 §3.3.21).
  defp decode_commit_args(args) do
    with {:ok, fh, rest} <- Types.decode_fhandle3(args),
         {:ok, offset, rest} <- XDR.decode_uhyper(rest),
         {:ok, count, _rest} <- XDR.decode_uint(rest) do
      {:ok, fh, offset, count}
    else
      {:error, _} -> :error
    end
  end

  # RENAME args: `diropargs3 + diropargs3` (RFC 1813 §3.3.14).
  defp decode_rename_args(args) do
    with {:ok, {from_dir, from_name}, rest} <- Types.decode_diropargs3(args),
         {:ok, {to_dir, to_name}, _rest} <- Types.decode_diropargs3(rest) do
      {:ok, from_dir, from_name, to_dir, to_name}
    else
      {:error, _} -> :error
    end
  end

  # MKDIR args: `diropargs3 + sattr3` (RFC 1813 §3.3.9).
  defp decode_mkdir_args(args) do
    with {:ok, {dir, name}, rest} <- Types.decode_diropargs3(args),
         {:ok, sattr, _rest} <- Types.decode_sattr3(rest) do
      {:ok, dir, name, sattr}
    else
      {:error, _} -> :error
    end
  end

  # SETATTR args: `fhandle3 + sattr3 + sattrguard3`. `sattrguard3` is
  # an XDR-optional `nfstime3` — RFC 1813 §3.3.2's "guarded SETATTR"
  # check pre-op `ctime` matches `obj_ctime`.
  defp decode_setattr_args(args) do
    with {:ok, fh, rest} <- Types.decode_fhandle3(args),
         {:ok, sattr, rest} <- Types.decode_sattr3(rest),
         {:ok, guard, _rest} <- XDR.decode_optional(rest, &Types.decode_nfstime3/1) do
      {:ok, fh, sattr, guard}
    else
      {:error, _} -> :error
    end
  end

  defp decode_readdir_args(args) do
    with {:ok, fh, rest} <- Types.decode_fhandle3(args),
         {:ok, cookie, rest} <- XDR.decode_uhyper(rest),
         {:ok, verf, rest} <- Types.decode_cookieverf3(rest),
         {:ok, count, _} <- XDR.decode_uint(rest) do
      {:ok, fh, cookie, verf, count}
    else
      {:error, _} -> :error
    end
  end

  defp decode_readdirplus_args(args) do
    with {:ok, fh, rest} <- Types.decode_fhandle3(args),
         {:ok, cookie, rest} <- XDR.decode_uhyper(rest),
         {:ok, verf, rest} <- Types.decode_cookieverf3(rest),
         {:ok, dircount, rest} <- XDR.decode_uint(rest),
         {:ok, maxcount, _} <- XDR.decode_uint(rest) do
      {:ok, fh, cookie, verf, dircount, maxcount}
    else
      {:error, _} -> :error
    end
  end

  # ——— Internal procedure handlers ———————————————————————————————

  defp do_getattr(fh, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.getattr(fh, auth, ctx) do
      {:ok, %Types.Fattr3{} = a} ->
        {:ok, Types.encode_nfsstat3(:ok) <> Types.encode_fattr3(a)}

      {:error, status} ->
        {:ok, Types.encode_nfsstat3(status)}
    end
  end

  defp do_create(dir, name, mode, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.create(dir, name, mode, auth, ctx) do
      {:ok, fh, attr, %Types.WccData{} = wcc} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_post_op_fh3(fh) <>
           Types.encode_post_op_attr(attr) <>
           Types.encode_wcc_data(wcc)}

      {:error, status, %Types.WccData{} = wcc} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(wcc)}

      {:error, status} ->
        empty = %Types.WccData{before: nil, after: nil}
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(empty)}
    end
  end

  defp do_write(fh, offset, data, stable, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.write(fh, offset, data, stable, auth, ctx) do
      {:ok, %{wcc: wcc, count: count, committed: committed, verf: verf}} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_wcc_data(wcc) <>
           XDR.encode_uint(count) <>
           Types.encode_stable_how(committed) <>
           Types.encode_writeverf3(verf)}

      {:error, status, %Types.WccData{} = wcc} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(wcc)}

      {:error, status} ->
        empty = %Types.WccData{before: nil, after: nil}
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(empty)}
    end
  end

  defp do_commit(fh, offset, count, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.commit(fh, offset, count, auth, ctx) do
      {:ok, %{wcc: wcc, verf: verf}} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_wcc_data(wcc) <>
           Types.encode_writeverf3(verf)}

      {:error, status, %Types.WccData{} = wcc} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(wcc)}

      {:error, status} ->
        empty = %Types.WccData{before: nil, after: nil}
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(empty)}
    end
  end

  defp do_rename(from_dir, from_name, to_dir, to_name, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.rename(from_dir, from_name, to_dir, to_name, auth, ctx) do
      {:ok, %Types.WccData{} = from_wcc, %Types.WccData{} = to_wcc} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_wcc_data(from_wcc) <>
           Types.encode_wcc_data(to_wcc)}

      {:error, status, %Types.WccData{} = from_wcc, %Types.WccData{} = to_wcc} ->
        {:ok,
         Types.encode_nfsstat3(status) <>
           Types.encode_wcc_data(from_wcc) <>
           Types.encode_wcc_data(to_wcc)}

      {:error, status} ->
        empty = %Types.WccData{before: nil, after: nil}

        {:ok,
         Types.encode_nfsstat3(status) <>
           Types.encode_wcc_data(empty) <>
           Types.encode_wcc_data(empty)}
    end
  end

  defp do_mkdir(dir, name, sattr, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.mkdir(dir, name, sattr, auth, ctx) do
      {:ok, fh, attr, %Types.WccData{} = wcc} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_post_op_fh3(fh) <>
           Types.encode_post_op_attr(attr) <>
           Types.encode_wcc_data(wcc)}

      {:error, status, %Types.WccData{} = wcc} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(wcc)}

      {:error, status} ->
        empty = %Types.WccData{before: nil, after: nil}
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(empty)}
    end
  end

  defp do_remove(dir, name, auth, ctx) do
    do_dir_unlink(:remove, dir, name, auth, ctx)
  end

  defp do_rmdir(dir, name, auth, ctx) do
    do_dir_unlink(:rmdir, dir, name, auth, ctx)
  end

  # REMOVE and RMDIR share the wire shape (`diropargs3` in,
  # `wcc_data` out) and the reply codec — only the backend callback
  # differs. Dispatch via the callback name.
  defp do_dir_unlink(callback, dir, name, auth, ctx) do
    backend = fetch_backend!(ctx)

    case apply(backend, callback, [dir, name, auth, ctx]) do
      {:ok, %Types.WccData{} = wcc} ->
        {:ok, Types.encode_nfsstat3(:ok) <> Types.encode_wcc_data(wcc)}

      {:error, status, %Types.WccData{} = wcc} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(wcc)}

      {:error, status} ->
        empty = %Types.WccData{before: nil, after: nil}
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(empty)}
    end
  end

  defp do_setattr(fh, sattr, guard, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.setattr(fh, sattr, guard, auth, ctx) do
      {:ok, %Types.WccData{} = wcc} ->
        {:ok, Types.encode_nfsstat3(:ok) <> Types.encode_wcc_data(wcc)}

      {:error, status, %Types.WccData{} = wcc} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(wcc)}

      {:error, status} ->
        empty = %Types.WccData{before: nil, after: nil}
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_wcc_data(empty)}
    end
  end

  defp do_lookup(dir, name, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.lookup(dir, name, auth, ctx) do
      {:ok, fh, file_attr, dir_attr} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_fhandle3(fh) <>
           Types.encode_post_op_attr(file_attr) <>
           Types.encode_post_op_attr(dir_attr)}

      {:error, status, dir_attr} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(dir_attr)}

      {:error, status} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(nil)}
    end
  end

  defp do_access(fh, mask, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.access(fh, mask, auth, ctx) do
      {:ok, granted, attr} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_post_op_attr(attr) <>
           XDR.encode_uint(granted)}

      {:error, status, attr} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(attr)}

      {:error, status} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(nil)}
    end
  end

  defp do_readlink(fh, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.readlink(fh, auth, ctx) do
      {:ok, path, attr} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_post_op_attr(attr) <>
           Types.encode_nfspath3(path)}

      {:error, status, attr} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(attr)}

      {:error, status} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(nil)}
    end
  end

  defp do_read(fh, offset, count, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.read(fh, offset, count, auth, ctx) do
      {:ok, %{data: data, eof: eof} = reply} ->
        post_op = Map.get(reply, :post_op)
        {chunks, bytes} = take_bytes(data, count)
        pad = rem(4 - rem(bytes, 4), 4)

        body = [
          Types.encode_nfsstat3(:ok),
          Types.encode_post_op_attr(post_op),
          XDR.encode_uint(bytes),
          XDR.encode_bool(eof),
          XDR.encode_uint(bytes),
          chunks,
          <<0::size(pad * 8)>>
        ]

        {:ok, body}

      {:error, status, attr} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(attr)}

      {:error, status} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(nil)}
    end
  end

  defp do_readdir(fh, cookie, verf, count, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.readdir(fh, cookie, verf, count, auth, ctx) do
      {:ok, entries, new_verf, eof, attr} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_post_op_attr(attr) <>
           Types.encode_cookieverf3(new_verf) <>
           encode_readdir_entries(entries) <>
           XDR.encode_bool(eof)}

      {:error, status, attr} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(attr)}

      {:error, status} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(nil)}
    end
  end

  defp do_readdirplus(fh, cookie, verf, dircount, maxcount, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.readdirplus(fh, cookie, verf, dircount, maxcount, auth, ctx) do
      {:ok, entries, new_verf, eof, attr} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_post_op_attr(attr) <>
           Types.encode_cookieverf3(new_verf) <>
           encode_readdirplus_entries(entries) <>
           XDR.encode_bool(eof)}

      {:error, status, attr} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(attr)}

      {:error, status} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(nil)}
    end
  end

  # Pull binary chunks from an `Enumerable.t()` until either it
  # exhausts or we have accumulated `cap` bytes. The reducer trims
  # the trailing chunk to make `cap` an exact upper bound. Returns
  # the chunks (in order, as iodata) and the total byte count.
  #
  # The accumulator is bounded by `cap` (typically ≤ 1 MiB — the
  # kernel-side per-read limit), not by the file size.
  defp take_bytes(stream, cap) when is_integer(cap) and cap >= 0 do
    {acc, total} =
      Enum.reduce_while(stream, {[], 0}, fn chunk, {acc, total} ->
        chunk_size = byte_size(chunk)
        remaining = cap - total

        cond do
          remaining <= 0 ->
            {:halt, {acc, total}}

          chunk_size <= remaining ->
            {:cont, {[chunk | acc], total + chunk_size}}

          true ->
            <<head::binary-size(remaining), _::binary>> = chunk
            {:halt, {[head | acc], total + remaining}}
        end
      end)

    {Enum.reverse(acc), total}
  end

  # XDR linked-list encoding for `entry3` (RFC 1813 §3.3.16): each
  # entry is preceded by `bool TRUE` (the "next pointer is non-null"
  # flag); a trailing `bool FALSE` terminates the list. An empty list
  # encodes to a single `bool FALSE`.
  defp encode_readdir_entries([]), do: XDR.encode_bool(false)

  defp encode_readdir_entries(entries) when is_list(entries) do
    body =
      for {fileid, name, cookie} <- entries, into: <<>> do
        XDR.encode_bool(true) <>
          XDR.encode_uhyper(fileid) <>
          Types.encode_filename3(name) <>
          XDR.encode_uhyper(cookie)
      end

    body <> XDR.encode_bool(false)
  end

  # Same linked-list pattern as `entry3`, but `entryplus3` adds
  # `name_attributes` (post_op_attr) and `name_handle` (post_op_fh3)
  # right before the next-pointer flag. RFC 1813 §3.3.17.
  defp encode_readdirplus_entries([]), do: XDR.encode_bool(false)

  defp encode_readdirplus_entries(entries) when is_list(entries) do
    body =
      for {fileid, name, cookie, attr, fh} <- entries, into: <<>> do
        XDR.encode_bool(true) <>
          XDR.encode_uhyper(fileid) <>
          Types.encode_filename3(name) <>
          XDR.encode_uhyper(cookie) <>
          Types.encode_post_op_attr(attr) <>
          Types.encode_post_op_fh3(fh)
      end

    body <> XDR.encode_bool(false)
  end

  defp do_fsstat(fh, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.fsstat(fh, auth, ctx) do
      {:ok, reply, attr} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_post_op_attr(attr) <>
           XDR.encode_uhyper(reply.tbytes) <>
           XDR.encode_uhyper(reply.fbytes) <>
           XDR.encode_uhyper(reply.abytes) <>
           XDR.encode_uhyper(reply.tfiles) <>
           XDR.encode_uhyper(reply.ffiles) <>
           XDR.encode_uhyper(reply.afiles) <>
           XDR.encode_uint(reply.invarsec)}

      {:error, status, attr} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(attr)}

      {:error, status} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(nil)}
    end
  end

  defp do_fsinfo(fh, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.fsinfo(fh, auth, ctx) do
      {:ok, reply, attr} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_post_op_attr(attr) <>
           XDR.encode_uint(reply.rtmax) <>
           XDR.encode_uint(reply.rtpref) <>
           XDR.encode_uint(reply.rtmult) <>
           XDR.encode_uint(reply.wtmax) <>
           XDR.encode_uint(reply.wtpref) <>
           XDR.encode_uint(reply.wtmult) <>
           XDR.encode_uint(reply.dtpref) <>
           XDR.encode_uhyper(reply.maxfilesize) <>
           Types.encode_nfstime3(reply.time_delta) <>
           XDR.encode_uint(reply.properties)}

      {:error, status, attr} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(attr)}

      {:error, status} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(nil)}
    end
  end

  defp do_pathconf(fh, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.pathconf(fh, auth, ctx) do
      {:ok, reply, attr} ->
        {:ok,
         Types.encode_nfsstat3(:ok) <>
           Types.encode_post_op_attr(attr) <>
           XDR.encode_uint(reply.linkmax) <>
           XDR.encode_uint(reply.name_max) <>
           XDR.encode_bool(reply.no_trunc) <>
           XDR.encode_bool(reply.chown_restricted) <>
           XDR.encode_bool(reply.case_insensitive) <>
           XDR.encode_bool(reply.case_preserving)}

      {:error, status, attr} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(attr)}

      {:error, status} ->
        {:ok, Types.encode_nfsstat3(status) <> Types.encode_post_op_attr(nil)}
    end
  end

  defp fetch_backend!(ctx) do
    case Map.fetch(ctx, :nfs_v3_backend) do
      {:ok, backend} when is_atom(backend) ->
        backend

      _ ->
        raise ArgumentError,
              "NFSServer.NFSv3.Handler invoked without a backend in ctx; use `with_backend/1` to register"
    end
  end
end
