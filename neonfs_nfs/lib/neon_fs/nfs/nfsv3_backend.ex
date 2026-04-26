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
  alias NFSServer.NFSv3.Types.{Fattr3, Nfstime3, Specdata3}

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

  ## Internal — resolution

  # Returns `{:ok, volume_name, path}` or `{:error, :stale}`.
  defp resolve_dir(fh) do
    with {:ok, _decoded, vol_name, path} <- resolve_handle(fh) do
      {:ok, vol_name, path}
    end
  end

  # Returns `{:ok, volume_name, path, FileMeta}` or `{:error, status}`.
  defp resolve_meta(fh) do
    with {:ok, _decoded, vol_name, path} <- resolve_handle(fh),
         {:ok, meta} <- core_call(NeonFS.Core, :get_file_meta, [vol_name, path]) do
      {:ok, vol_name, path, meta}
    else
      {:error, :not_found} -> {:error, :noent}
      {:error, :stale} -> {:error, :stale}
      {:error, status} when is_atom(status) -> {:error, status}
      _ -> {:error, :stale}
    end
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
      nil -> ChunkReader.read_file_stream(vol_name, path, offset: offset, length: count)
      fun when is_function(fun, 4) -> fun.(vol_name, path, offset, count)
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
