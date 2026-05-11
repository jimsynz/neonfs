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
  alias NeonFS.Core.ReadOperation
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

  None currently (placeholder for `:include_acls`,
  `:include_system_xattrs`, `:snapshot_id` — to land in follow-ups).
  """
  @spec export(binary(), Path.t(), keyword()) ::
          {:ok, export_summary()} | {:error, term()}
  def export(volume_name, output_path, opts \\ [])
      when is_binary(volume_name) and is_binary(output_path) and is_list(opts) do
    with {:ok, volume} <- resolve_volume(volume_name),
         files <- list_files(volume.id),
         :ok <- ensure_paths_fit_ustar(files),
         {:ok, byte_count} <- write_archive(output_path, volume, files) do
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

  defp list_files(volume_id) do
    volume_id
    |> FileIndex.list_volume()
    |> Enum.sort_by(& &1.path)
  end

  # Ustar's prefix (155) + name (100) split caps total path length
  # at ~255 chars. Anything longer needs GNU LongLink — deferred to
  # a follow-up issue.
  defp ensure_paths_fit_ustar(files) do
    files
    |> Enum.find(&too_long?/1)
    |> case do
      nil -> :ok
      file -> {:error, {:path_too_long_for_ustar, file.path}}
    end
  end

  defp too_long?(%{path: path}) do
    entry_name = entry_name_for(path)
    byte_size(entry_name) > 255
  end

  defp entry_name_for(path) do
    @files_prefix <> ensure_leading_slash(path)
  end

  defp ensure_leading_slash("/" <> _ = path), do: path
  defp ensure_leading_slash(path), do: "/" <> path

  ## Archive writing

  defp write_archive(output_path, volume, files) do
    File.mkdir_p!(Path.dirname(output_path))

    File.open(output_path, [:write, :raw, :binary], fn io ->
      manifest_bytes = build_manifest(volume, files)
      :ok = write_entry(io, @manifest_name, manifest_bytes, now_unix())

      byte_count =
        Enum.reduce(files, 0, fn file, acc ->
          write_file_entry(io, volume, file)
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

  defp build_manifest(volume, files) do
    Jason.encode!(%{
      version: @manifest_version,
      schema: "neonfs.volume-export.v1",
      volume: %{
        id: volume.id,
        name: volume.name
      },
      exported_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      file_count: length(files),
      total_bytes: Enum.reduce(files, 0, fn f, acc -> acc + f.size end),
      files:
        Enum.map(files, fn f ->
          %{
            path: f.path,
            size: f.size,
            mode: f.mode,
            uid: f.uid,
            gid: f.gid,
            modified_at: DateTime.to_iso8601(f.modified_at)
          }
        end)
    })
  end

  defp write_file_entry(io, volume, file_meta) do
    header = build_ustar_header(entry_name_for(file_meta.path), file_meta)
    :ok = IO.binwrite(io, header)

    :ok = stream_file_content(io, volume, file_meta)
    :ok = write_padding(io, file_meta.size)
    :ok
  end

  defp stream_file_content(_io, _volume, %{size: 0}), do: :ok

  defp stream_file_content(io, _volume, file_meta) do
    case ReadOperation.read_file_stream(file_meta.volume_id, file_meta.path) do
      {:ok, %{stream: stream}} ->
        Enum.each(stream, fn chunk -> :ok = IO.binwrite(io, chunk) end)
        :ok

      {:error, reason} ->
        # Mid-stream errors abort the export rather than leaving a
        # truncated entry behind.
        raise "failed to stream #{file_meta.path}: #{inspect(reason)}"
    end
  end

  # In-memory entry (used for the manifest). The whole binary fits
  # in RAM by construction — manifests carry only file metadata, not
  # content.
  defp write_entry(io, name, body, mtime) when is_binary(body) do
    header =
      build_ustar_header_raw(
        name,
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

  defp build_ustar_header(name, %{size: size, mode: mode, uid: uid, gid: gid} = file_meta) do
    mtime = DateTime.to_unix(file_meta.modified_at)

    build_ustar_header_raw(name,
      body_size: size,
      mode: mode,
      uid: uid,
      gid: gid,
      mtime: mtime,
      typeflag: ?0
    )
  end

  # Builds a 512-byte ustar header. Long paths are split across the
  # ustar prefix (155 bytes) + name (100 bytes) fields, joined by
  # `/`. `ensure_paths_fit_ustar/1` upstream guarantees the combined
  # length is ≤ 255.
  defp build_ustar_header_raw(name, opts) do
    {prefix, short_name} = split_for_ustar(name)

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

  defp split_for_ustar(name) when byte_size(name) <= 100 do
    {"", name}
  end

  defp split_for_ustar(name) do
    # Walk backwards from the 100-byte cap to the first `/`. The
    # remainder becomes the prefix.
    split_at = find_split_point(name, min(100, byte_size(name) - 1))
    <<prefix::binary-size(split_at), "/", rest::binary>> = name
    {prefix, rest}
  end

  defp find_split_point(_name, idx) when idx <= 0 do
    raise ArgumentError, "no `/` in ustar-eligible position"
  end

  defp find_split_point(name, idx) do
    case :binary.at(name, idx) do
      ?/ ->
        # Tail (rest after the `/`) must be ≤ 100 bytes.
        tail_size = byte_size(name) - idx - 1
        head_size = idx

        if tail_size <= 100 and head_size <= 155 do
          idx
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
