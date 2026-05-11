defmodule NeonFS.Core.VolumeImport do
  @moduledoc """
  Import a `VolumeExport` tarball into a brand-new volume (#966,
  part of the snapshots epic #959). Pairs with
  `NeonFS.Core.VolumeExport` for round-trip volume migration.

  The tar is parsed as a stream: each `files/<path>` entry's body
  is piped directly into `WriteOperation.write_file_streamed/4` via
  a `Stream.unfold/2` that reads from the open IO device in
  chunk-sized blocks. Peak working set is bounded by chunk size,
  never by file size (per CLAUDE.md "no whole-file buffering").

  V1 scope: local input path only, into a fresh volume with
  default storage policy. S3 / `file://` URL inputs, custom
  storage policy via `--storage-policy`, and post-import
  `--verify` are tracked as follow-ups.
  """

  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.Core.WriteOperation

  require Logger

  @block 512
  @read_chunk 64 * 1024
  @manifest_name "manifest.json"
  @files_prefix "files/"

  @type import_summary :: %{
          path: String.t(),
          volume_id: String.t(),
          volume_name: String.t(),
          file_count: non_neg_integer(),
          byte_count: non_neg_integer(),
          source_volume_name: String.t() | nil
        }

  @doc """
  Import the tarball at `input_path` into a new volume named
  `new_volume_name`.

  Returns `{:ok, summary}` where `summary` includes the new
  volume's `id` and `name`, the file/byte counts written, and the
  original volume name from the manifest (when present).

  ## Errors

    * `{:error, :input_missing}` — `input_path` doesn't exist.
    * `{:error, {:manifest_missing}}` — the archive contains no
      `manifest.json` entry, or the first entry isn't the manifest.
    * `{:error, {:manifest_invalid, reason}}` — manifest is present
      but malformed.
    * `{:error, {:manifest_mismatch, field, expected, actual}}` —
      manifest's declared `file_count` / `total_bytes` differs from
      what the archive actually contains.
    * Any `VolumeRegistry.create/2` error (e.g. name collision) or
      `WriteOperation.write_file_streamed/4` error.
  """
  @spec import_archive(Path.t(), binary(), keyword()) ::
          {:ok, import_summary()} | {:error, term()}
  def import_archive(input_path, new_volume_name, _opts \\ [])
      when is_binary(input_path) and is_binary(new_volume_name) do
    with :ok <- ensure_input_exists(input_path),
         {:ok, io} <- File.open(input_path, [:read, :raw, :binary]) do
      try do
        do_import(io, input_path, new_volume_name)
      after
        File.close(io)
      end
    end
  end

  defp ensure_input_exists(path) do
    if File.regular?(path), do: :ok, else: {:error, :input_missing}
  end

  defp do_import(io, input_path, new_volume_name) do
    with {:ok, manifest, io} <- read_manifest(io),
         {:ok, volume} <- VolumeRegistry.create(new_volume_name, []),
         {:ok, counts} <- stream_files_into(io, volume) do
      with :ok <- check_manifest_counts(manifest, counts) do
        {:ok,
         %{
           path: input_path,
           volume_id: volume.id,
           volume_name: volume.name,
           file_count: counts.file_count,
           byte_count: counts.byte_count,
           source_volume_name: get_in(manifest, ["volume", "name"])
         }}
      end
    end
  end

  ## Manifest

  defp read_manifest(io) do
    case read_next_entry(io) do
      {:ok, %{name: @manifest_name, size: size, typeflag: ?0}, io} -> decode_manifest(io, size)
      {:ok, %{name: other}, _io} -> {:error, {:manifest_missing, {:first_entry, other}}}
      {:eof, _io} -> {:error, {:manifest_missing, :empty_archive}}
      {:error, _} = err -> err
    end
  end

  defp decode_manifest(io, size) do
    with {:ok, body} <- binread_exact(io, size),
         {:ok, manifest} <- decode_manifest_body(body),
         {:ok, io} <- skip_padding(io, size) do
      validate_manifest_shape(manifest, io)
    end
  end

  defp decode_manifest_body(body) do
    case Jason.decode(body) do
      {:ok, manifest} -> {:ok, manifest}
      {:error, reason} -> {:error, {:manifest_invalid, reason}}
    end
  end

  defp validate_manifest_shape(%{"schema" => "neonfs.volume-export.v1"} = manifest, io) do
    {:ok, manifest, io}
  end

  defp validate_manifest_shape(%{"schema" => other}, _io) do
    {:error, {:manifest_invalid, {:unsupported_schema, other}}}
  end

  defp validate_manifest_shape(manifest, _io) when is_map(manifest) do
    {:error, {:manifest_invalid, :missing_schema}}
  end

  defp validate_manifest_shape(_other, _io) do
    {:error, {:manifest_invalid, :not_a_map}}
  end

  defp check_manifest_counts(manifest, %{file_count: file_count, byte_count: byte_count}) do
    cond do
      manifest["file_count"] != nil and manifest["file_count"] != file_count ->
        {:error, {:manifest_mismatch, :file_count, manifest["file_count"], file_count}}

      manifest["total_bytes"] != nil and manifest["total_bytes"] != byte_count ->
        {:error, {:manifest_mismatch, :total_bytes, manifest["total_bytes"], byte_count}}

      true ->
        :ok
    end
  end

  ## File entries

  defp stream_files_into(io, volume) do
    stream_files_loop(io, volume, %{file_count: 0, byte_count: 0})
  end

  defp stream_files_loop(io, volume, counts) do
    case read_next_entry(io) do
      {:eof, _io} ->
        {:ok, counts}

      {:ok, %{name: @files_prefix <> path, size: size, typeflag: ?0}, io} ->
        case write_file_entry(io, volume, path, size) do
          {:ok, io} ->
            new_counts = %{
              file_count: counts.file_count + 1,
              byte_count: counts.byte_count + size
            }

            stream_files_loop(io, volume, new_counts)

          {:error, _} = err ->
            err
        end

      {:ok, %{name: name}, _io} ->
        # Out-of-band entries (e.g. future ACL/xattr blobs) are
        # ignored in v1.
        {:error, {:unexpected_entry, name}}

      {:error, _} = err ->
        err
    end
  end

  defp write_file_entry(io, volume, path, size) do
    {body_stream, finish_fn} = body_stream(io, size)

    case WriteOperation.write_file_streamed(volume.id, "/" <> path, body_stream) do
      {:ok, _file_meta} ->
        # `write_file_streamed` consumed the stream, advancing the
        # underlying IO device by `size` bytes. Now skip the ustar
        # padding to land at the next entry's header.
        finish_fn.()

      err ->
        err
    end
  end

  # Returns `{stream, finish_fn}` where `stream` lazily reads up to
  # `size` bytes from `io` in `@read_chunk` chunks, and `finish_fn`
  # is called after the stream is fully drained to advance past
  # the ustar 512-byte padding.
  defp body_stream(io, size) do
    stream =
      Stream.unfold(size, fn
        0 ->
          nil

        remaining ->
          to_read = min(remaining, @read_chunk)

          case IO.binread(io, to_read) do
            :eof -> nil
            {:error, _} -> nil
            data when is_binary(data) -> {data, remaining - byte_size(data)}
          end
      end)

    finish_fn = fn ->
      case skip_padding(io, size) do
        {:ok, io} -> {:ok, io}
        err -> err
      end
    end

    {stream, finish_fn}
  end

  ## Raw ustar reader

  # Returns `{:ok, %{name, size, typeflag}, io}` for a regular entry,
  # `{:eof, io}` when two consecutive zero blocks mark end of archive,
  # or `{:error, reason}` for a malformed header.
  #
  # GNU `LongLink` (`?L`) pseudo-entries are absorbed here: the
  # body of the LongLink is the next entry's real name, which then
  # overrides the ustar `name`/`prefix` fields of the following
  # entry. Callers always see the resolved long-named entry.
  defp read_next_entry(io, pending_long_name \\ nil) do
    case IO.binread(io, @block) do
      :eof ->
        {:eof, io}

      data when is_binary(data) and byte_size(data) == @block ->
        if all_zero?(data) do
          handle_possible_end(io)
        else
          parse_header(data, io, pending_long_name)
        end

      _ ->
        {:error, :truncated_header}
    end
  end

  defp handle_possible_end(io) do
    case IO.binread(io, @block) do
      :eof ->
        {:eof, io}

      data when is_binary(data) and byte_size(data) == @block ->
        if all_zero?(data) do
          {:eof, io}
        else
          # Single zero block in the middle — treat as truncated.
          {:error, :truncated_archive}
        end

      _ ->
        {:eof, io}
    end
  end

  defp parse_header(<<header::binary-size(@block)>>, io, pending_long_name) do
    <<
      name::binary-size(100),
      _mode::binary-size(8),
      _uid::binary-size(8),
      _gid::binary-size(8),
      size_octal::binary-size(12),
      _mtime::binary-size(12),
      _chksum::binary-size(8),
      typeflag_byte,
      _linkname::binary-size(100),
      _magic::binary-size(6),
      _version::binary-size(2),
      _uname::binary-size(32),
      _gname::binary-size(32),
      _dev::binary-size(16),
      prefix::binary-size(155),
      _trail::binary-size(12)
    >> = header

    with {:ok, size} <- parse_octal(size_octal) do
      case typeflag_byte do
        ?L -> absorb_long_link(io, size)
        _ -> deliver_entry(typeflag_byte, name, prefix, size, pending_long_name, io)
      end
    end
  end

  defp deliver_entry(typeflag_byte, name, prefix, size, pending_long_name, io) do
    full_name =
      case pending_long_name do
        nil -> ustar_join(strip_nuls(prefix), strip_nuls(name))
        long -> long
      end

    {:ok, %{name: full_name, size: size, typeflag: typeflag_byte}, io}
  end

  # GNU LongLink: the body is the next entry's real name (NUL-
  # terminated, padded to 512). Read it, skip the padding, then
  # recurse to grab the real entry with `pending_long_name` set.
  defp absorb_long_link(io, size) do
    with {:ok, body} <- binread_exact(io, size),
         {:ok, io} <- skip_padding(io, size) do
      read_next_entry(io, strip_trailing_nuls(body))
    end
  end

  defp strip_trailing_nuls(bin) do
    case :binary.split(bin, <<0>>) do
      [head | _] -> head
    end
  end

  defp ustar_join("", name), do: name
  defp ustar_join(prefix, name), do: prefix <> "/" <> name

  defp parse_octal(bin) do
    bin
    |> strip_nuls()
    |> String.trim()
    |> case do
      "" ->
        {:ok, 0}

      str ->
        case Integer.parse(str, 8) do
          {n, ""} -> {:ok, n}
          _ -> {:error, {:malformed_octal, bin}}
        end
    end
  end

  defp strip_nuls(bin) do
    case :binary.split(bin, <<0>>) do
      [head | _] -> head
    end
  end

  defp all_zero?(bin), do: bin == :binary.copy(<<0>>, @block)

  defp skip_padding(io, body_size) do
    case rem(body_size, @block) do
      0 ->
        {:ok, io}

      r ->
        to_skip = @block - r

        case IO.binread(io, to_skip) do
          :eof -> {:ok, io}
          {:error, _} = err -> err
          data when is_binary(data) -> {:ok, io}
        end
    end
  end

  defp binread_exact(io, size) do
    case IO.binread(io, size) do
      :eof ->
        {:error, :truncated_body}

      data when is_binary(data) and byte_size(data) == size ->
        {:ok, data}

      data when is_binary(data) ->
        {:error, {:short_read, expected: size, got: byte_size(data)}}

      {:error, _} = err ->
        err
    end
  end
end
