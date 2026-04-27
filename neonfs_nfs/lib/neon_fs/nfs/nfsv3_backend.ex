defmodule NeonFS.NFS.NFSv3Backend do
  @moduledoc """
  `NFSServer.NFSv3.Backend` implementation that delegates to NeonFS
  via `NeonFS.Client.Router` for metadata and `NeonFS.Client.ChunkReader`
  for streaming reads.

  Lives in the `neonfs_nfs` interface package, so its only NeonFS
  dependency is `neonfs_client` — the new BEAM stack stays compatible
  with the orchestration layering rule (interface packages do not
  depend on `neonfs_core`). Sub-issue #532; cf. #284 (NFSv3 epic) and
  #113 (native-BEAM NFS epic).

  ## Filehandle scheme

  See `NeonFS.NFS.Filehandle` — every callback that receives a
  `fhandle3` decodes it into `{volume_id, fileid, generation}`,
  resolves the volume id back to the volume name via the cluster
  metadata path, and resolves the fileid back to a path via
  `NeonFS.NFS.InodeTable`. Wrong-volume or malformed handles return
  `{:error, :stale}`, which the handler maps to `NFS3ERR_STALE`.

  ## Streaming reads

  `c:read/5` returns a `read_reply` whose `:data` field is a lazy
  `NeonFS.Client.ChunkReader` stream — chunk bytes traverse the TLS
  data plane and never materialise as a single binary. The handler's
  `take_bytes/2` truncates to the kernel's `count` cap. Per
  `CLAUDE.md` no-whole-file-buffering rule.

  ## Test injection

  Every callback funnels its NeonFS RPCs through `core_call/3` and
  its inode lookups through `inode_table_get_path/1`. Both can be
  overridden via app env so unit tests don't need a live core node:

      Application.put_env(:neonfs_nfs, :core_call_fn, fn module, fun, args -> ... end)
      Application.put_env(:neonfs_nfs, :inode_table_get_path_fn, fn inode -> ... end)

  The default implementations defer to `NeonFS.Client.Router.call/3`
  and `NeonFS.NFS.InodeTable.get_path/1` respectively.
  """

  @behaviour NFSServer.NFSv3.Backend

  alias NeonFS.Client.{ChunkReader, Router}
  alias NeonFS.Core.FileMeta
  alias NeonFS.NFS.{Filehandle, InodeTable}
  alias NFSServer.NFSv3.Types.{Fattr3, Nfstime3, Sattr3, Specdata3, WccAttr, WccData}

  import Bitwise, only: [<<<: 2, band: 2]

  require Logger

  # POSIX file-type bits used to derive `Fattr3.type` from a FileMeta mode.
  @s_ifdir 0o040000
  @s_ifreg 0o100000
  @s_iflnk 0o120000
  @s_ifmt 0o170000

  ## Behaviour callbacks

  @impl true
  def getattr(fh, _auth, _ctx) do
    with {:ok, _vol, _path, meta} <- resolve_meta(fh) do
      {:ok, fattr_from_meta(meta)}
    end
  end

  @impl true
  def access(fh, requested_mask, _auth, _ctx) do
    case resolve_meta(fh) do
      {:ok, _vol, _path, meta} ->
        {:ok, requested_mask, fattr_from_meta(meta)}

      {:error, status} ->
        {:error, status, nil}
    end
  end

  @impl true
  def lookup(dir_fh, name, _auth, _ctx) do
    with {:ok, vol_name, dir_path} <- resolve_dir(dir_fh),
         child_path <- Path.join(dir_path, name),
         {:ok, child_meta} <- core_call(NeonFS.Core, :get_file_meta, [vol_name, child_path]),
         {:ok, vol_id_bin} <- volume_uuid_to_binary(child_meta.volume_id),
         {:ok, fileid} <- allocate_inode(vol_name, child_path) do
      child_fh = Filehandle.encode(vol_id_bin, fileid)
      child_attr = fattr_from_meta(child_meta)

      dir_attr =
        case core_call(NeonFS.Core, :get_file_meta, [vol_name, dir_path]) do
          {:ok, dm} -> fattr_from_meta(dm)
          _ -> nil
        end

      {:ok, child_fh, child_attr, dir_attr}
    else
      {:error, :not_found} -> lookup_not_found(dir_fh)
      {:error, :invalid} -> {:error, :stale, nil}
      {:error, status} when is_atom(status) -> {:error, status, nil}
      err -> err
    end
  end

  @impl true
  def readlink(fh, _auth, _ctx) do
    case resolve_meta(fh) do
      {:ok, vol_name, path, meta} -> do_readlink(vol_name, path, meta)
      {:error, status} -> {:error, status, nil}
    end
  end

  defp do_readlink(vol_name, path, meta) do
    attr = fattr_from_meta(meta)

    if attr.type == :lnk do
      readlink_target(vol_name, path, attr)
    else
      {:error, :inval, attr}
    end
  end

  defp readlink_target(vol_name, path, attr) do
    case core_call(NeonFS.Core, :read_file, [vol_name, path]) do
      {:ok, target} -> {:ok, target, attr}
      {:error, status} -> {:error, status, attr}
    end
  end

  @impl true
  def read(fh, offset, count, _auth, _ctx) do
    case resolve_meta(fh) do
      {:ok, vol_name, path, meta} ->
        stream = read_file_stream(vol_name, path, offset, count)
        eof = offset + count >= meta.size

        {:ok, %{data: stream, eof: eof, post_op: fattr_from_meta(meta)}}

      {:error, status} ->
        {:error, status, nil}
    end
  end

  @impl true
  def readdir(fh, cookie, _verf, count, _auth, _ctx) do
    with {:ok, vol_name, path, dir_meta} <- resolve_meta(fh),
         {:ok, entries} <-
           core_call(NeonFS.Core, :list_dir, [vol_name, path]) do
      readdir_entries =
        entries
        |> Enum.with_index(1)
        |> Enum.drop(cookie)
        |> Enum.take_while(fn {_e, i} -> i <= cookie + max(div(count, 64), 1) end)
        |> Enum.map(fn {entry, idx} ->
          child_path = Path.join(path, entry.path |> Path.basename())
          {:ok, fileid} = allocate_inode(vol_name, child_path)
          {fileid, Path.basename(entry.path), idx}
        end)

      eof = length(readdir_entries) + cookie >= length(entries)
      {:ok, readdir_entries, <<0::64>>, eof, fattr_from_meta(dir_meta)}
    else
      {:error, status} -> {:error, status, nil}
    end
  end

  @impl true
  def readdirplus(fh, cookie, verf, _dircount, maxcount, auth, ctx) do
    case readdir(fh, cookie, verf, maxcount, auth, ctx) do
      {:ok, plain_entries, new_verf, eof, dir_attr} ->
        {:ok, vol_name, _path, _meta} = resolve_meta(fh)
        plus_entries = Enum.map(plain_entries, &expand_readdirplus_entry(&1, vol_name))

        {:ok, plus_entries, new_verf, eof, dir_attr}

      err ->
        err
    end
  end

  defp expand_readdirplus_entry({fileid, name, idx}, vol_name) do
    child_path = "/" <> name
    inflated_path = derive_path_from_inode(fileid, vol_name, child_path)

    case core_call(NeonFS.Core, :get_file_meta, [vol_name, inflated_path]) do
      {:ok, child_meta} -> readdirplus_entry_with_meta(fileid, name, idx, child_meta)
      _ -> {fileid, name, idx, nil, nil}
    end
  end

  defp readdirplus_entry_with_meta(fileid, name, idx, child_meta) do
    {:ok, vol_id_bin} = volume_uuid_to_binary(child_meta.volume_id)
    child_fh = Filehandle.encode(vol_id_bin, fileid)
    {fileid, name, idx, fattr_from_meta(child_meta), child_fh}
  end

  @impl true
  def fsstat(fh, _auth, _ctx) do
    case resolve_meta(fh) do
      {:ok, _vol_name, _path, meta} ->
        # Capacity reporting is per-cluster, not per-volume; return
        # generous fixed values until #321 lands a Prometheus-backed
        # forecaster the NFS layer can query.
        reply = %{
          tbytes: 1 <<< 50,
          fbytes: 1 <<< 49,
          abytes: 1 <<< 49,
          tfiles: 1 <<< 32,
          ffiles: 1 <<< 31,
          afiles: 1 <<< 31,
          invarsec: 0
        }

        {:ok, reply, fattr_from_meta(meta)}

      {:error, status} ->
        {:error, status, nil}
    end
  end

  @impl true
  def fsinfo(fh, _auth, _ctx) do
    case resolve_meta(fh) do
      {:ok, _vol_name, _path, meta} ->
        {:ok, fsinfo_reply(), fattr_from_meta(meta)}

      {:error, status} ->
        {:error, status, nil}
    end
  end

  @impl true
  def pathconf(fh, _auth, _ctx) do
    case resolve_meta(fh) do
      {:ok, _vol_name, _path, meta} ->
        {:ok, pathconf_reply(), fattr_from_meta(meta)}

      {:error, status} ->
        {:error, status, nil}
    end
  end

  # The createverf3 (8 bytes) lives under this key in the file's
  # `metadata` map, so a retried EXCLUSIVE CREATE with the same verf
  # observes the existing file rather than failing with EEXIST.
  @createverf_meta_key "nfs3_createverf"

  @impl true
  def create(dir_fh, name, mode, _auth, _ctx) do
    case resolve_dir(dir_fh) do
      {:ok, vol_name, dir_path} ->
        do_create(vol_name, dir_path, name, mode)

      {:error, status} ->
        {:error, status, %WccData{before: nil, after: nil}}
    end
  end

  defp do_create(vol_name, dir_path, name, mode) do
    child_path = Path.join(dir_path, name)
    pre_dir_wcc = pre_dir_wcc(vol_name, dir_path)

    case mode do
      {:unchecked, %Sattr3{} = sattr} ->
        write_new_file(vol_name, dir_path, child_path, sattr, pre_dir_wcc, opts: [])

      {:guarded, %Sattr3{} = sattr} ->
        case core_call(NeonFS.Core, :get_file_meta, [vol_name, child_path]) do
          {:ok, _existing} ->
            {:error, :exist,
             %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

          {:error, _} ->
            write_new_file(vol_name, dir_path, child_path, sattr, pre_dir_wcc, opts: [])
        end

      {:exclusive, verf} when is_binary(verf) and byte_size(verf) == 8 ->
        exclusive_create(vol_name, dir_path, child_path, verf, pre_dir_wcc)
    end
  end

  defp exclusive_create(vol_name, dir_path, child_path, verf, pre_dir_wcc) do
    case core_call(NeonFS.Core, :get_file_meta, [vol_name, child_path]) do
      {:ok, %FileMeta{metadata: %{@createverf_meta_key => ^verf}} = existing} ->
        # Idempotent retry — same verf, return existing file.
        ok_create_reply(existing, vol_name, dir_path, pre_dir_wcc)

      {:ok, _other} ->
        # File exists with a different (or no) verf → EEXIST.
        {:error, :exist, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      {:error, _} ->
        write_new_file(vol_name, dir_path, child_path, %Sattr3{}, pre_dir_wcc,
          opts: [
            create_only: true,
            metadata: %{@createverf_meta_key => verf}
          ]
        )
    end
  end

  # Common path: hand off to `WriteOperation.write_file_at/5` to
  # create the empty file with the requested mode/uid/gid + any
  # extra opts (e.g. `create_only: true` for EXCLUSIVE). Re-fetches
  # the metadata for the post-op view; failures map to a parent-only
  # wcc reply.
  defp write_new_file(vol_name, dir_path, child_path, %Sattr3{} = sattr, pre_dir_wcc, opts) do
    write_opts =
      opts
      |> Keyword.get(:opts, [])
      |> Keyword.merge(opts)
      |> Keyword.delete(:opts)
      |> sattr_to_write_opts(sattr)

    case core_call(NeonFS.Core, :write_file_at, [vol_name, child_path, 0, <<>>, write_opts]) do
      {:ok, %FileMeta{} = meta} ->
        ok_create_reply(meta, vol_name, dir_path, pre_dir_wcc)

      {:error, :exists} ->
        {:error, :exist, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      {:error, status} when is_atom(status) ->
        {:error, map_create_error(status),
         %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      {:error, _} ->
        {:error, :io, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}
    end
  end

  defp ok_create_reply(%FileMeta{} = meta, vol_name, dir_path, pre_dir_wcc) do
    case volume_uuid_to_binary(meta.volume_id) do
      {:ok, vol_id_bin} ->
        {:ok, fileid} = allocate_inode(vol_name, meta.path)
        child_fh = Filehandle.encode(vol_id_bin, fileid)

        wcc = %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}
        {:ok, child_fh, fattr_from_meta(meta), wcc}

      _ ->
        {:error, :io, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}
    end
  end

  defp sattr_to_write_opts(opts, %Sattr3{} = sattr) do
    opts
    |> maybe_put_opt(:mode, sattr.mode)
    |> maybe_put_opt(:uid, sattr.uid)
    |> maybe_put_opt(:gid, sattr.gid)
  end

  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp pre_dir_wcc(vol_name, dir_path) do
    case core_call(NeonFS.Core, :get_file_meta, [vol_name, dir_path]) do
      {:ok, %FileMeta{} = meta} -> wcc_attr_from_meta(meta)
      _ -> nil
    end
  end

  defp post_dir_attr(vol_name, dir_path) do
    case core_call(NeonFS.Core, :get_file_meta, [vol_name, dir_path]) do
      {:ok, %FileMeta{} = meta} -> fattr_from_meta(meta)
      _ -> nil
    end
  end

  defp map_create_error(:not_found), do: :noent
  defp map_create_error(:noent), do: :noent
  defp map_create_error(:perm), do: :perm
  defp map_create_error(:acces), do: :acces
  defp map_create_error(:invalid_argument), do: :inval
  defp map_create_error(:inval), do: :inval
  defp map_create_error(:nospc), do: :nospc
  defp map_create_error(:dquot), do: :dquot
  defp map_create_error(:fbig), do: :fbig
  defp map_create_error(_), do: :io

  @impl true
  def mkdir(dir_fh, name, %Sattr3{} = sattr, _auth, _ctx) do
    case resolve_dir(dir_fh) do
      {:ok, vol_name, dir_path} ->
        do_mkdir(vol_name, dir_path, name, sattr)

      {:error, status} ->
        {:error, status, %WccData{before: nil, after: nil}}
    end
  end

  defp do_mkdir(vol_name, dir_path, name, %Sattr3{} = _sattr) do
    child_path = Path.join(dir_path, name)
    pre_dir_wcc = pre_dir_wcc(vol_name, dir_path)

    case core_call(NeonFS.Core, :mkdir, [vol_name, child_path]) do
      {:ok, _dir_entry} ->
        # `mkdir` creates the directory entry but doesn't return a
        # `FileMeta`; synthesise the directory's attrs by re-reading
        # via `get_file_meta` (root-style synthesis lives in
        # `resolve_meta`'s `"/"` branch — for non-root dirs FileIndex
        # synthesises directory FileMetas through `get_by_path`).
        case core_call(NeonFS.Core, :get_file_meta, [vol_name, child_path]) do
          {:ok, %FileMeta{} = child_meta} ->
            ok_create_reply(child_meta, vol_name, dir_path, pre_dir_wcc)

          _ ->
            {:ok, nil, nil,
             %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}
        end

      {:error, :eexist} ->
        {:error, :exist, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      {:error, status} when is_atom(status) ->
        {:error, map_create_error(status),
         %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      {:error, _} ->
        {:error, :io, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}
    end
  end

  @impl true
  def remove(dir_fh, name, _auth, _ctx) do
    case resolve_dir(dir_fh) do
      {:ok, vol_name, dir_path} ->
        do_remove(vol_name, dir_path, name)

      {:error, status} ->
        {:error, status, %WccData{before: nil, after: nil}}
    end
  end

  defp do_remove(vol_name, dir_path, name) do
    child_path = Path.join(dir_path, name)
    pre_dir_wcc = pre_dir_wcc(vol_name, dir_path)

    case core_call(NeonFS.Core, :delete_file, [vol_name, child_path]) do
      :ok ->
        {:ok, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      {:error, :not_found} ->
        {:error, :noent, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      {:error, status} when is_atom(status) ->
        {:error, map_create_error(status),
         %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      {:error, _} ->
        {:error, :io, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}
    end
  end

  @impl true
  def rmdir(dir_fh, name, _auth, _ctx) do
    case resolve_dir(dir_fh) do
      {:ok, vol_name, dir_path} ->
        do_rmdir(vol_name, dir_path, name)

      {:error, status} ->
        {:error, status, %WccData{before: nil, after: nil}}
    end
  end

  defp do_rmdir(vol_name, dir_path, name) do
    child_path = Path.join(dir_path, name)
    pre_dir_wcc = pre_dir_wcc(vol_name, dir_path)

    with :ok <- check_rmdir_target(vol_name, child_path, pre_dir_wcc, dir_path),
         :ok <- check_rmdir_empty(vol_name, child_path, pre_dir_wcc, dir_path) do
      do_rmdir_delete(vol_name, dir_path, child_path, pre_dir_wcc)
    end
  end

  defp check_rmdir_target(vol_name, child_path, pre_dir_wcc, dir_path) do
    case core_call(NeonFS.Core, :get_file_meta, [vol_name, child_path]) do
      {:ok, %FileMeta{mode: mode}} ->
        if Bitwise.band(mode, @s_ifmt) == @s_ifdir do
          :ok
        else
          {:error, :notdir,
           %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}
        end

      {:error, :not_found} ->
        {:error, :noent, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      _ ->
        {:error, :io, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}
    end
  end

  defp check_rmdir_empty(vol_name, child_path, pre_dir_wcc, dir_path) do
    case core_call(NeonFS.Core, :list_dir, [vol_name, child_path]) do
      {:ok, []} ->
        :ok

      {:ok, [_ | _]} ->
        {:error, :notempty,
         %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      _ ->
        # Treat list-dir failure as a non-empty signal — refuse to
        # delete blindly. The caller still gets `wcc_data`.
        {:error, :io, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}
    end
  end

  defp do_rmdir_delete(vol_name, dir_path, child_path, pre_dir_wcc) do
    case core_call(NeonFS.Core, :delete_file, [vol_name, child_path]) do
      :ok ->
        {:ok, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      {:error, status} when is_atom(status) ->
        {:error, map_create_error(status),
         %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}

      {:error, _} ->
        {:error, :io, %WccData{before: pre_dir_wcc, after: post_dir_attr(vol_name, dir_path)}}
    end
  end

  # Per-instance write verifier (RFC 1813 §3.3.7 / §3.3.21). The
  # 8-byte value lives in `:persistent_term` keyed by this module
  # so it stays stable across calls but rotates whenever the BEAM
  # restarts — which is what NFS clients use to detect that any
  # unstable writes need to be resent.
  @writeverf_pt_key {__MODULE__, :writeverf}

  @impl true
  def write(fh, offset, data, stable, _auth, _ctx) when is_binary(data) do
    case resolve_meta(fh) do
      {:ok, vol_name, path, pre_meta} ->
        do_write(vol_name, path, pre_meta, offset, data, stable)

      {:error, status} ->
        {:error, status, %WccData{before: nil, after: nil}}
    end
  end

  defp do_write(vol_name, path, pre_meta, offset, data, requested_stable) do
    pre_wcc = wcc_attr_from_meta(pre_meta)

    case core_call(NeonFS.Core, :write_file_at, [vol_name, path, offset, data, []]) do
      {:ok, %FileMeta{} = post_meta} ->
        # NeonFS' `write_file_at` is FileIndex-quorum-replicated and
        # synchronous — every committed write is durable, so we
        # always report `:file_sync` regardless of the client's
        # `requested_stable` hint. RFC 1813 permits stronger
        # guarantees than asked.
        _ = requested_stable

        {:ok,
         %{
           wcc: %WccData{before: pre_wcc, after: fattr_from_meta(post_meta)},
           count: byte_size(data),
           committed: :file_sync,
           verf: writeverf()
         }}

      {:error, status} when is_atom(status) ->
        {:error, map_create_error(status), %WccData{before: pre_wcc, after: nil}}

      {:error, _} ->
        {:error, :io, %WccData{before: pre_wcc, after: nil}}
    end
  end

  @impl true
  def commit(fh, _offset, _count, _auth, _ctx) do
    case resolve_meta(fh) do
      {:ok, _vol_name, _path, meta} ->
        # FileIndex quorum-writes are already on stable storage
        # before WRITE returns (see `c:write/6` above), so COMMIT is
        # a metadata-only flush. Return the per-instance writeverf3.
        {:ok,
         %{
           wcc: %WccData{before: wcc_attr_from_meta(meta), after: fattr_from_meta(meta)},
           verf: writeverf()
         }}

      {:error, status} ->
        {:error, status, %WccData{before: nil, after: nil}}
    end
  end

  # Lazily-initialised per-VM writeverf3. First reader generates and
  # stashes; later readers fetch. `:persistent_term` is the right
  # tool — read-only after the initial put, no garbage-collection
  # pressure, and it survives every supervisor restart short of a
  # full VM restart (which is exactly when we want a fresh verf).
  defp writeverf do
    case :persistent_term.get(@writeverf_pt_key, :undefined) do
      :undefined ->
        verf = :crypto.strong_rand_bytes(8)
        :persistent_term.put(@writeverf_pt_key, verf)
        verf

      verf when is_binary(verf) ->
        verf
    end
  end

  @impl true
  def setattr(fh, %Sattr3{} = sattr, guard_ctime, _auth, _ctx) do
    case resolve_meta(fh) do
      {:ok, vol_name, path, pre_meta} ->
        do_setattr(vol_name, path, pre_meta, sattr, guard_ctime)

      {:error, status} ->
        {:error, status, %WccData{before: nil, after: nil}}
    end
  end

  defp do_setattr(vol_name, path, pre_meta, %Sattr3{} = sattr, guard_ctime) do
    pre_wcc = wcc_attr_from_meta(pre_meta)

    if guard_failed?(pre_meta, guard_ctime) do
      {:error, :not_sync, %WccData{before: pre_wcc, after: fattr_from_meta(pre_meta)}}
    else
      apply_sattr(vol_name, path, pre_wcc, sattr)
    end
  end

  defp apply_sattr(vol_name, path, pre_wcc, %Sattr3{} = sattr) do
    updates = build_attr_updates(sattr)

    result =
      if is_integer(sattr.size) do
        core_call(NeonFS.Core, :truncate_file, [vol_name, path, sattr.size, updates])
      else
        if updates == [] do
          # No-op SETATTR — RFC 1813 permits this; just refresh post-op.
          core_call(NeonFS.Core, :get_file_meta, [vol_name, path])
        else
          core_call(NeonFS.Core, :update_file_meta, [vol_name, path, updates])
        end
      end

    case result do
      {:ok, %FileMeta{} = post_meta} ->
        {:ok, %WccData{before: pre_wcc, after: fattr_from_meta(post_meta)}}

      {:error, status} when is_atom(status) ->
        {:error, map_setattr_error(status), %WccData{before: pre_wcc, after: nil}}

      {:error, _} ->
        {:error, :io, %WccData{before: pre_wcc, after: nil}}
    end
  end

  defp build_attr_updates(%Sattr3{} = sattr) do
    []
    |> maybe_put(:mode, sattr.mode)
    |> maybe_put(:uid, sattr.uid)
    |> maybe_put(:gid, sattr.gid)
    |> maybe_put(:accessed_at, time_set_to_datetime(sattr.atime))
    |> maybe_put(:modified_at, time_set_to_datetime(sattr.mtime))
  end

  defp maybe_put(kw, _key, nil), do: kw
  defp maybe_put(kw, key, value), do: [{key, value} | kw]

  defp time_set_to_datetime(nil), do: nil
  defp time_set_to_datetime(:set_to_server_time), do: DateTime.utc_now()

  defp time_set_to_datetime({:client, %Nfstime3{seconds: s, nseconds: n}}) do
    DateTime.from_unix!(s * 1_000_000_000 + n, :nanosecond)
  end

  defp guard_failed?(_pre_meta, nil), do: false

  defp guard_failed?(%FileMeta{changed_at: %DateTime{} = ctime}, %Nfstime3{
         seconds: gs,
         nseconds: gn
       }) do
    actual = time_to_nfstime(ctime)
    actual.seconds != gs or actual.nseconds != gn
  end

  defp guard_failed?(_pre_meta, %Nfstime3{}), do: true

  defp wcc_attr_from_meta(%FileMeta{} = meta) do
    %WccAttr{
      size: meta.size,
      mtime: time_to_nfstime(meta.modified_at),
      ctime: time_to_nfstime(meta.changed_at)
    }
  end

  # RFC 1813 §3.3.2 doesn't define a 1:1 mapping for arbitrary core
  # errors; the common ones come up below. Fallthrough is `:io`.
  defp map_setattr_error(:not_found), do: :stale
  defp map_setattr_error(:noent), do: :noent
  defp map_setattr_error(:perm), do: :perm
  defp map_setattr_error(:acces), do: :acces
  defp map_setattr_error(:invalid_argument), do: :inval
  defp map_setattr_error(:inval), do: :inval
  defp map_setattr_error(_), do: :io

  ## Internal — resolution

  # Returns `{:ok, volume_name, path}` or `{:error, :stale}`.
  defp resolve_dir(fh) do
    with {:ok, _decoded, vol_name, path} <- resolve_handle(fh) do
      {:ok, vol_name, path}
    end
  end

  # Returns `{:ok, volume_name, path, FileMeta}` or `{:error, status}`.
  #
  # Root path "/" is synthesised — `FileIndex.get_by_path/2` doesn't
  # carry a row for the volume root (the row sits implicitly inside
  # the volume), so a naive `get_file_meta` lookup on "/" returns
  # `:not_found`. Handler-level callers expect root resolution to
  # succeed with a directory FileMeta so GETATTR / READDIRPLUS on the
  # mount root work without the client having to pre-mkdir the root.
  defp resolve_meta(fh) do
    case resolve_handle(fh) do
      {:ok, decoded, vol_name, "/"} ->
        {:ok, vol_name, "/", root_file_meta(decoded.volume_id, vol_name)}

      {:ok, _decoded, vol_name, path} ->
        case core_call(NeonFS.Core, :get_file_meta, [vol_name, path]) do
          {:ok, meta} -> {:ok, vol_name, path, meta}
          {:error, :not_found} -> {:error, :noent}
          {:error, :stale} -> {:error, :stale}
          {:error, status} when is_atom(status) -> {:error, status}
          _ -> {:error, :stale}
        end

      {:error, status} ->
        {:error, status}
    end
  end

  # Synthetic `FileMeta` for the volume root. POSIX-mode-bit 0o040755
  # marks it as a directory; ownership defaults to `0:0` until the
  # volume's owner field gets surfaced through this layer.
  defp root_file_meta(volume_id_bin, _vol_name) do
    now = DateTime.utc_now()
    volume_id_str = Filehandle.volume_uuid_from_binary(volume_id_bin)

    %FileMeta{
      id: nil,
      volume_id: volume_id_str,
      path: "/",
      chunks: [],
      stripes: nil,
      size: 0,
      content_type: "inode/directory",
      mode: 0o040755,
      uid: 0,
      gid: 0,
      acl_entries: [],
      default_acl: nil,
      created_at: now,
      modified_at: now,
      accessed_at: now,
      changed_at: now,
      version: 1,
      previous_version_id: nil
    }
  end

  defp resolve_handle(fh) do
    with {:ok, decoded} <- Filehandle.decode(fh),
         {:ok, {vol_name_or_nil, path}} <- inode_table_get_path(decoded.fileid) do
      vol_name = vol_name_or_nil || ""
      {:ok, decoded, vol_name, path}
    else
      _ -> {:error, :stale}
    end
  end

  defp lookup_not_found(dir_fh) do
    case resolve_dir(dir_fh) do
      {:ok, vol_name, dir_path} ->
        case core_call(NeonFS.Core, :get_file_meta, [vol_name, dir_path]) do
          {:ok, dm} -> {:error, :noent, fattr_from_meta(dm)}
          _ -> {:error, :noent, nil}
        end

      _ ->
        {:error, :stale, nil}
    end
  end

  ## Internal — Fattr3 mapping

  defp fattr_from_meta(%FileMeta{} = meta) do
    %Fattr3{
      type: ftype_from_mode(meta.mode),
      mode: band(meta.mode, 0o7777),
      nlink: 1,
      uid: meta.uid,
      gid: meta.gid,
      size: meta.size,
      used: meta.size,
      rdev: %Specdata3{specdata1: 0, specdata2: 0},
      fsid: 0,
      fileid: deterministic_fileid(meta),
      atime: time_to_nfstime(meta.accessed_at),
      mtime: time_to_nfstime(meta.modified_at),
      ctime: time_to_nfstime(meta.changed_at)
    }
  end

  defp ftype_from_mode(mode) do
    case band(mode, @s_ifmt) do
      @s_ifdir -> :dir
      @s_iflnk -> :lnk
      @s_ifreg -> :reg
      _ -> :reg
    end
  end

  defp time_to_nfstime(%DateTime{} = dt) do
    seconds = DateTime.to_unix(dt, :second)
    {sub_seconds, _} = dt.microsecond
    %Nfstime3{seconds: seconds, nseconds: sub_seconds * 1000}
  end

  defp time_to_nfstime(_), do: %Nfstime3{seconds: 0, nseconds: 0}

  # FileMeta.id is a UUID string; the NFS layer wants a 64-bit
  # identifier. Take the first 8 bytes of the volume_id+path inode the
  # InodeTable would have allocated. This is stable for the same
  # (volume, path) and unique within a volume.
  defp deterministic_fileid(%FileMeta{volume_id: vol_id, path: path}) do
    :crypto.hash(:sha256, vol_id <> "\0" <> path)
    |> binary_part(0, 8)
    |> :binary.decode_unsigned(:big)
  end

  ## Internal — RPC + ETS injection points

  defp core_call(module, function, args) do
    case Application.get_env(:neonfs_nfs, :core_call_fn) do
      nil ->
        Router.call(module, function, args)

      fun when is_function(fun, 3) ->
        fun.(module, function, args)
    end
  end

  defp inode_table_get_path(inode) do
    case Application.get_env(:neonfs_nfs, :inode_table_get_path_fn) do
      nil -> InodeTable.get_path(inode)
      fun when is_function(fun, 1) -> fun.(inode)
    end
  end

  defp read_file_stream(vol_name, path, offset, count) do
    case Application.get_env(:neonfs_nfs, :read_file_stream_fn) do
      nil ->
        # `ChunkReader.read_file_stream/3` returns
        # `{:ok, %{stream: ..., file_size: ...}}`. The handler's
        # `take_bytes/2` wants the raw `Enumerable.t()`, so unwrap.
        # Errors propagate through unchanged so `read/5`'s caller
        # can map them. (#588.)
        case ChunkReader.read_file_stream(vol_name, path, offset: offset, length: count) do
          {:ok, %{stream: stream}} -> stream
          {:error, _} = err -> err
        end

      fun when is_function(fun, 4) ->
        fun.(vol_name, path, offset, count)
    end
  end

  defp allocate_inode(vol_name, path) do
    case Application.get_env(:neonfs_nfs, :inode_table_allocate_fn) do
      nil -> InodeTable.allocate_inode(vol_name, path)
      fun when is_function(fun, 2) -> fun.(vol_name, path)
    end
  end

  defp volume_uuid_to_binary(uuid_string), do: Filehandle.volume_uuid_to_binary(uuid_string)

  defp derive_path_from_inode(_fileid, _vol_name, fallback_path), do: fallback_path

  ## Internal — fixed FSINFO / PATHCONF replies

  defp fsinfo_reply do
    %{
      rtmax: 1 <<< 20,
      rtpref: 1 <<< 20,
      rtmult: 4096,
      wtmax: 1 <<< 20,
      wtpref: 1 <<< 20,
      wtmult: 4096,
      dtpref: 8192,
      maxfilesize: 0xFFFFFFFFFFFFFFFF,
      time_delta: %Nfstime3{seconds: 0, nseconds: 1},
      properties: 0
    }
  end

  defp pathconf_reply do
    %{
      linkmax: 1024,
      name_max: 255,
      no_trunc: true,
      chown_restricted: true,
      case_insensitive: false,
      case_preserving: true
    }
  end
end
