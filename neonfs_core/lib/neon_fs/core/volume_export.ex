defmodule NeonFS.Core.VolumeExport do
  @moduledoc """
  Export a volume's tree as a portable TAR archive (#965, part of
  the snapshots epic #959).

  Walks the volume's live `FileIndex`, streams each file's content
  via `ReadOperation.read_file_stream/3`, and writes minimal ustar
  entries straight to the output IO device. The working set is
  bounded by chunk size, never by file size — per CLAUDE.md's "no
  whole-file buffering" rule.

  V1 scope: live root only, local output path only, file content +
  manifest. Snapshot export, ACL/xattr capture, and S3 URL outputs
  are tracked as follow-ups so this PR stays focused.
  """

  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.FileMeta
  alias NeonFS.Core.ReadOperation
  alias NeonFS.Core.Snapshot
  alias NeonFS.Core.Volume.MetadataReader
  alias NeonFS.Core.Volume.MetadataValue
  alias NeonFS.Core.VolumeRegistry

  require Logger

  @manifest_version 1
  @manifest_name "manifest.json"
  @files_prefix "files"

  @type export_summary :: %{
          path: String.t(),
          file_count: non_neg_integer(),
          byte_count: non_neg_integer()
        }

  @doc """
  Export `volume_name`'s live root as a TAR archive at
  `output_path`.

  Returns `{:ok, %{path, file_count, byte_count}}` on success. The
  TAR contains one `manifest.json` entry plus one `files/<path>`
  entry per regular file in the volume.

  ## Options

  - `:snapshot_id` — export a specific snapshot's tree rather than
    the live root. Walks the snapshot's `:file_index` via
    `MetadataReader.range/5` with `:at_root` set to the snapshot's
    `root_chunk_hash`, then streams each file's content through
    `ReadOperation.read_file_stream_from_meta/3`. Chunks remain
    content-addressed so the read path is unchanged — only the
    `FileMeta` enumeration differs.

  - `:include_acls` (boolean) — when true, the manifest's per-file
    entry includes `acl_entries` (and `default_acl` when set).
    Restored on import via `FileIndex.update/2`.

  - `:include_system_xattrs` (boolean) — when true, the manifest's
    per-file entry includes `xattrs` (keys + values base64-encoded
    for JSON binary safety). Restored on import.
  """
  @spec export(binary(), Path.t(), keyword()) ::
          {:ok, export_summary()} | {:error, term()}
  def export(volume_name, output_path, opts \\ [])
      when is_binary(volume_name) and is_binary(output_path) and is_list(opts) do
    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, files} <- list_files_for_export(volume, opts),
         {:ok, byte_count} <- write_archive(output_path, volume, files, opts) do
      {:ok,
       %{
         path: output_path,
         file_count: length(files),
         byte_count: byte_count
       }}
    end
  end

  ## Volume / file enumeration

  defp resolve_volume(name) do
    case VolumeRegistry.get_by_name(name) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :volume_not_found}
    end
  end

  defp list_files_for_export(volume, opts) do
    case Keyword.get(opts, :snapshot_id) do
      nil -> {:ok, list_live_files(volume.id)}
      snapshot_id when is_binary(snapshot_id) -> list_snapshot_files(volume, snapshot_id)
    end
  end

  defp list_live_files(volume_id) do
    volume_id
    |> FileIndex.list_volume()
    |> Enum.sort_by(& &1.path)
  end

  defp list_snapshot_files(volume, snapshot_id) do
    with {:ok, snapshot} <- Snapshot.get(volume.id, snapshot_id),
         {:ok, raw_entries} <-
           MetadataReader.range(volume.id, :file_index, <<>>, <<>>,
             at_root: snapshot.root_chunk_hash
           ) do
      files =
        raw_entries
        |> Enum.flat_map(&decode_file_entry/1)
        |> Enum.sort_by(& &1.path)

      {:ok, files}
    end
  end

  defp decode_file_entry({"file:" <> _, bytes}) when is_binary(bytes) do
    case MetadataValue.decode(bytes) do
      {:ok, map} when is_map(map) -> [storable_to_file_meta(map)]
      _ -> []
    end
  end

  defp decode_file_entry(_), do: []

  defp storable_to_file_meta(map) do
    %FileMeta{
      id: Map.get(map, :id),
      volume_id: Map.get(map, :volume_id),
      path: Map.get(map, :path),
      chunks: Map.get(map, :chunks, []) || [],
      stripes: decode_stripes_field(Map.get(map, :stripes)),
      size: Map.get(map, :size, 0),
      content_type: Map.get(map, :content_type) || "application/octet-stream",
      mode: Map.get(map, :mode, 0o644),
      uid: Map.get(map, :uid, 0),
      gid: Map.get(map, :gid, 0),
      created_at: Map.get(map, :created_at, DateTime.utc_now()),
      modified_at: Map.get(map, :modified_at, DateTime.utc_now()),
      accessed_at: Map.get(map, :accessed_at, DateTime.utc_now()),
      changed_at: Map.get(map, :changed_at, DateTime.utc_now()),
      version: Map.get(map, :version, 1),
      previous_version_id: Map.get(map, :previous_version_id),
      hlc_timestamp: Map.get(map, :hlc_timestamp)
    }
  end

  defp decode_stripes_field(nil), do: nil
  defp decode_stripes_field(list) when is_list(list), do: list
  defp decode_stripes_field(_), do: nil

  defp entry_name_for(path) do
    @files_prefix <> ensure_leading_slash(path)
  end

  defp ensure_leading_slash("/" <> _ = path), do: path
  defp ensure_leading_slash(path), do: "/" <> path

  ## Archive writing

  defp write_archive(output_path, volume, files, opts) do
    File.mkdir_p!(Path.dirname(output_path))

    File.open(output_path, [:write, :raw, :binary], fn io ->
      manifest_bytes = build_manifest(volume, files, opts)
      :ok = write_entry(io, @manifest_name, manifest_bytes, now_unix())

      byte_count =
        Enum.reduce(files, 0, fn file, acc ->
          write_file_entry(io, volume, file, opts)
          acc + file.size
        end)

      write_end_of_archive(io)
      byte_count
    end)
    |> case do
      {:ok, bytes} -> {:ok, bytes}
      {:error, _} = err -> err
    end
  end

  defp build_manifest(volume, files, opts) do
    base = %{
      version: @manifest_version,
      schema: "neonfs.volume-export.v1",
      volume: %{
        id: volume.id,
        name: volume.name
      },
      exported_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      file_count: length(files),
      total_bytes: Enum.reduce(files, 0, fn f, acc -> acc + f.size end),
      files: Enum.map(files, &manifest_file_entry(&1, opts))
    }

    case Keyword.get(opts, :snapshot_id) do
      nil -> base
      id -> Map.put(base, :snapshot_id, id)
    end
    |> Jason.encode!()
  end

  defp manifest_file_entry(f, opts) do
    base = %{
      path: f.path,
      size: f.size,
      mode: f.mode,
      uid: f.uid,
      gid: f.gid,
      modified_at: DateTime.to_iso8601(f.modified_at)
    }

    base
    |> maybe_attach_acls(f, Keyword.get(opts, :include_acls, false))
    |> maybe_attach_xattrs(f, Keyword.get(opts, :include_system_xattrs, false))
  end

  defp maybe_attach_acls(entry, _file, false), do: entry

  defp maybe_attach_acls(entry, file, true) do
    acl_entries = encode_acl_entries(file.acl_entries || [])
    default_acl = encode_acl_entries(file.default_acl)

    entry
    |> Map.put(:acl_entries, acl_entries)
    |> maybe_put(:default_acl, default_acl)
  end

  defp maybe_attach_xattrs(entry, _file, false), do: entry

  defp maybe_attach_xattrs(entry, file, true) do
    Map.put(entry, :xattrs, encode_xattrs(file.xattrs || %{}))
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp encode_acl_entries(nil), do: nil

  defp encode_acl_entries(entries) when is_list(entries) do
    Enum.map(entries, fn %{type: type, id: id, permissions: perms} ->
      %{
        type: Atom.to_string(type),
        id: id,
        permissions: perms |> MapSet.to_list() |> Enum.sort() |> Enum.map(&Atom.to_string/1)
      }
    end)
  end

  # `xattrs` is `%{binary => binary}`. Keys and values can carry
  # arbitrary bytes (kernel xattrs are NUL-safe), so base64 both for
  # JSON-safe transport.
  defp encode_xattrs(xattrs) do
    Map.new(xattrs, fn {k, v} -> {Base.encode64(k), Base.encode64(v)} end)
  end

  defp write_file_entry(io, volume, file_meta, opts) do
    name = entry_name_for(file_meta.path)
    :ok = write_header_with_optional_longlink(io, name, file_meta)

    :ok = stream_file_content(io, volume, file_meta, opts)
    :ok = write_padding(io, file_meta.size)
    :ok
  end

  # Paths up to 256 bytes fit in ustar's `prefix` (155) + `name`
  # (100) split. Anything longer rides through a GNU `LongLink`
  # (typeflag 'L') pseudo-entry: a fake header whose body is the
  # real name, followed by the real entry header carrying a
  # truncated 100-byte name field. Tools that understand LongLink
  # use the cached name; others see the truncated name (the file
  # body still lands correctly).
  defp write_header_with_optional_longlink(io, name, file_meta) do
    case ustar_split(name) do
      {:ok, prefix, short_name} ->
        header = build_ustar_header(prefix, short_name, file_meta)
        IO.binwrite(io, header)

      :too_long ->
        :ok = write_long_link(io, name)
        truncated = binary_part(name, 0, min(100, byte_size(name)))
        header = build_ustar_header("", truncated, file_meta)
        IO.binwrite(io, header)
    end
  end

  defp write_long_link(io, name) do
    body = name <> <<0>>
    body_size = byte_size(body)

    header =
      build_ustar_header_raw(
        "././@LongLink",
        prefix: "",
        body_size: body_size,
        mode: 0,
        uid: 0,
        gid: 0,
        mtime: 0,
        typeflag: ?L
      )

    :ok = IO.binwrite(io, header)
    :ok = IO.binwrite(io, body)
    write_padding(io, body_size)
  end

  defp stream_file_content(_io, _volume, %{size: 0}, _opts), do: :ok

  defp stream_file_content(io, volume, file_meta, opts) do
    case file_stream(volume, file_meta, opts) do
      {:ok, %{stream: stream}} ->
        Enum.each(stream, fn chunk -> :ok = IO.binwrite(io, chunk) end)
        :ok

      {:error, reason} ->
        # Mid-stream errors abort the export rather than leaving a
        # truncated entry behind.
        raise "failed to stream #{file_meta.path}: #{inspect(reason)}"
    end
  end

  # Live-root export: resolve by path through the live FileIndex.
  # Snapshot export: the FileMeta was decoded from the snapshot's
  # `:file_index` tree, so go straight to the chunk-streaming path
  # without another lookup (which would hit the live root and could
  # see a different FileMeta or none at all).
  defp file_stream(volume, file_meta, opts) do
    case Keyword.get(opts, :snapshot_id) do
      nil -> ReadOperation.read_file_stream(file_meta.volume_id, file_meta.path)
      _ -> ReadOperation.read_file_stream_from_meta(volume, file_meta)
    end
  end

  # In-memory entry (used for the manifest). The whole binary fits
  # in RAM by construction — manifests carry only file metadata, not
  # content.
  defp write_entry(io, name, body, mtime) when is_binary(body) do
    {:ok, prefix, short_name} = ustar_split(name)

    header =
      build_ustar_header_raw(short_name,
        prefix: prefix,
        body_size: byte_size(body),
        mode: 0o644,
        uid: 0,
        gid: 0,
        mtime: mtime,
        typeflag: ?0
      )

    :ok = IO.binwrite(io, header)
    :ok = IO.binwrite(io, body)
    write_padding(io, byte_size(body))
  end

  defp build_ustar_header(
         prefix,
         short_name,
         %{
           size: size,
           mode: mode,
           uid: uid,
           gid: gid
         } = file_meta
       ) do
    mtime = DateTime.to_unix(file_meta.modified_at)

    build_ustar_header_raw(short_name,
      prefix: prefix,
      body_size: size,
      mode: mode,
      uid: uid,
      gid: gid,
      mtime: mtime,
      typeflag: ?0
    )
  end

  # Builds a 512-byte ustar header. Caller has already done any
  # name/prefix splitting (or set up a LongLink pre-entry for paths
  # too long to fit ustar's 256-byte cap).
  defp build_ustar_header_raw(short_name, opts) do
    prefix = Keyword.get(opts, :prefix, "")

    fields =
      [
        pad(short_name, 100),
        oct(opts[:mode], 8),
        oct(opts[:uid], 8),
        oct(opts[:gid], 8),
        oct(opts[:body_size], 12),
        oct(opts[:mtime], 12),
        # Checksum placeholder — 8 spaces while we compute it.
        :binary.copy(" ", 8),
        <<opts[:typeflag]>>,
        # linkname (100 bytes)
        :binary.copy(<<0>>, 100),
        # magic "ustar\0" + version "00"
        "ustar\0",
        "00",
        # uname (32 bytes)
        :binary.copy(<<0>>, 32),
        # gname (32 bytes)
        :binary.copy(<<0>>, 32),
        # devmajor + devminor (8 + 8)
        :binary.copy(<<0>>, 16),
        pad(prefix, 155),
        # trailing padding to 512
        :binary.copy(<<0>>, 12)
      ]

    header = IO.iodata_to_binary(fields)
    checksum = ustar_checksum(header)

    # Splice the real checksum into bytes 148..155. The chksum field
    # is 8 bytes: 6 octal digits + NUL + SPACE. `oct/2` already adds
    # the NUL terminator, so we append a single SPACE to land on 8.
    {head, rest} = :erlang.split_binary(header, 148)
    {_placeholder, tail} = :erlang.split_binary(rest, 8)
    head <> oct(checksum, 7) <> <<32>> <> tail
  end

  # Returns `{:ok, prefix, short_name}` when the name fits ustar's
  # prefix (155) + name (100) split, or `:too_long` when it
  # doesn't — caller emits a GNU LongLink pseudo-entry instead.
  defp ustar_split(name) when byte_size(name) <= 100, do: {:ok, "", name}

  defp ustar_split(name) do
    case find_split_point(name, min(100, byte_size(name) - 1)) do
      {:ok, idx} ->
        <<prefix::binary-size(idx), "/", rest::binary>> = name
        {:ok, prefix, rest}

      :error ->
        :too_long
    end
  end

  defp find_split_point(_name, idx) when idx <= 0, do: :error

  defp find_split_point(name, idx) do
    case :binary.at(name, idx) do
      ?/ ->
        # Tail (rest after the `/`) must be ≤ 100 bytes; prefix ≤ 155.
        tail_size = byte_size(name) - idx - 1
        head_size = idx

        if tail_size <= 100 and head_size <= 155 do
          {:ok, idx}
        else
          find_split_point(name, idx - 1)
        end

      _ ->
        find_split_point(name, idx - 1)
    end
  end

  defp ustar_checksum(<<header::binary>>) do
    header
    |> :binary.bin_to_list()
    |> Enum.sum()
  end

  defp pad(bin, size) when is_binary(bin) and byte_size(bin) <= size do
    bin <> :binary.copy(<<0>>, size - byte_size(bin))
  end

  defp oct(int, size) when is_integer(int) and int >= 0 do
    # Ustar uses NUL-terminated octal; the last byte is `\0`, so we
    # have `size - 1` digit slots zero-padded on the left.
    digits = size - 1
    str = Integer.to_string(int, 8)

    if byte_size(str) > digits do
      raise ArgumentError, "value #{int} won't fit in #{digits} octal digits"
    else
      String.pad_leading(str, digits, "0") <> <<0>>
    end
  end

  defp write_padding(io, size) do
    rem = rem(size, 512)

    case rem do
      0 -> :ok
      _ -> IO.binwrite(io, :binary.copy(<<0>>, 512 - rem))
    end
  end

  defp write_end_of_archive(io) do
    # Two consecutive all-zero 512-byte records mark end of archive.
    IO.binwrite(io, :binary.copy(<<0>>, 1024))
  end

  defp now_unix, do: DateTime.utc_now() |> DateTime.to_unix()
end
