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

  alias NeonFS.Core.BackupCrypto
  alias NeonFS.Core.FileIndex
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
    * `{:error, :passphrase_required}` — the archive is encrypted but
      no `:passphrase` was supplied.
    * `{:error, :bad_passphrase}` — the supplied passphrase doesn't
      unwrap the archive's content key.
    * Any `VolumeRegistry.create/2` error (e.g. name collision) or
      `WriteOperation.write_file_streamed/4` error.

  ## Options

    * `:passphrase` — decrypt an encrypted archive (#1004). Required
      when the manifest carries an `encryption` envelope; checked
      before the volume is created, so a wrong passphrase writes
      nothing.
    * `:into_existing` — when `true`, restore into the existing volume
      named `new_volume_name` instead of creating a fresh one (#1368).
      The volume record must already exist (else `{:error, :not_found}`).
      Files overwrite by path; the target's other files are left in
      place. Used by full-cluster DR restore, where the volume record is
      reseeded from a DR snapshot before its content is restored.
  """
  @spec import_archive(Path.t(), binary(), keyword()) ::
          {:ok, import_summary()} | {:error, term()}
  def import_archive(input_path, new_volume_name, opts \\ [])
      when is_binary(input_path) and is_binary(new_volume_name) do
    with :ok <- ensure_input_exists(input_path),
         {:ok, io} <- File.open(input_path, [:read, :raw, :binary]) do
      try do
        do_import(io, input_path, new_volume_name, opts)
      after
        File.close(io)
      end
    end
  end

  @doc """
  Restore a chain of archives — a full backup followed by zero or more
  incrementals (#1003) — into a new volume named `new_volume_name`.

  `archive_paths` must be ordered oldest-first (the full base, then each
  incremental in the order it was taken). The full is imported into a
  fresh volume; each incremental is then **replayed** into it: its
  included files overwrite by path, and the paths it records as
  `deleted` are removed. The result is byte-for-byte the volume state at
  the last archive.

  A single-element chain is just `import_archive/3`.
  """
  @spec restore_chain([Path.t()], binary(), keyword()) ::
          {:ok, import_summary()} | {:error, term()}
  def restore_chain(archive_paths, new_volume_name, opts \\ [])

  def restore_chain([base], new_volume_name, opts) do
    import_archive(base, new_volume_name, opts)
  end

  def restore_chain([base | incrementals], new_volume_name, opts)
      when is_list(incrementals) do
    with {:ok, summary} <- import_archive(base, new_volume_name, opts),
         {:ok, volume} <- VolumeRegistry.get(summary.volume_id),
         :ok <- replay_incrementals(incrementals, volume, opts) do
      {:ok, Map.put(summary, :chain_length, length(incrementals) + 1)}
    end
  end

  defp replay_incrementals([], _volume, _opts), do: :ok

  defp replay_incrementals([path | rest], volume, opts) do
    with :ok <- apply_archive_into(path, volume, opts) do
      replay_incrementals(rest, volume, opts)
    end
  end

  # Replay one incremental archive into an existing volume: write its
  # included file bodies (overwriting by path) and remove the paths it
  # recorded as deleted since its baseline.
  defp apply_archive_into(path, volume, opts) do
    with :ok <- ensure_input_exists(path),
         {:ok, io} <- File.open(path, [:read, :raw, :binary]) do
      try do
        with {:ok, manifest, io} <- read_manifest(io),
             {:ok, crypto} <- resolve_crypto(manifest, opts),
             {:ok, _counts} <- stream_files_into(io, volume, manifest_index(manifest), crypto) do
          apply_deletions(volume, Map.get(manifest, "deleted", []))
        end
      after
        File.close(io)
      end
    end
  end

  defp apply_deletions(_volume, nil), do: :ok

  defp apply_deletions(volume, paths) when is_list(paths) do
    Enum.each(paths, fn path ->
      case FileIndex.get_by_path(volume.id, path) do
        {:ok, file} -> FileIndex.delete(file.id)
        {:error, :not_found} -> :ok
      end
    end)
  end

  defp ensure_input_exists(path) do
    if File.regular?(path), do: :ok, else: {:error, :input_missing}
  end

  defp do_import(io, input_path, new_volume_name, opts) do
    with {:ok, manifest, io} <- read_manifest(io),
         {:ok, crypto} <- resolve_crypto(manifest, opts),
         {:ok, volume} <- resolve_target_volume(new_volume_name, opts),
         {:ok, counts} <- stream_files_into(io, volume, manifest_index(manifest), crypto) do
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

  # An archive carrying an `encryption` envelope needs the passphrase to
  # recover its content key. Unwrapping here — before the volume is
  # created or any body is read — means a wrong passphrase fails fast
  # with nothing written. Plaintext archives resolve to `nil` crypto.
  defp resolve_crypto(%{"encryption" => envelope}, opts) when is_map(envelope) do
    case Keyword.get(opts, :passphrase) do
      nil ->
        {:error, :passphrase_required}

      passphrase when is_binary(passphrase) ->
        with {:ok, content_key} <- BackupCrypto.open(envelope, passphrase) do
          {:ok, %{content_key: content_key}}
        end
    end
  end

  defp resolve_crypto(_manifest, _opts), do: {:ok, nil}

  # By default a fresh volume is created. With `into_existing: true` the
  # archive is restored into the volume that already carries
  # `new_volume_name` — the case full-cluster DR restore needs, where the
  # `volumes` keyspace has already been reseeded from a DR snapshot so the
  # record exists before its content is restored (#1368). Files overwrite
  # by path; entries already in the target that the archive doesn't carry
  # are left untouched (merge-by-path), which is a clean replace when the
  # target is the freshly-bootstrapped, empty volume.
  defp resolve_target_volume(name, opts) do
    if Keyword.get(opts, :into_existing, false) do
      VolumeRegistry.get_by_name(name)
    else
      VolumeRegistry.create(name, [])
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

  defp validate_manifest_shape(%{"schema" => "neonfs.volume-export.v2"} = manifest, io) do
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

  defp stream_files_into(io, volume, file_index_by_path, crypto) do
    stream_files_loop(io, volume, %{file_count: 0, byte_count: 0}, file_index_by_path, crypto)
  end

  defp stream_files_loop(io, volume, counts, idx, crypto) do
    case read_next_entry(io) do
      {:eof, _io} ->
        {:ok, counts}

      {:ok, %{name: @files_prefix <> path, size: size, typeflag: ?0}, io} ->
        case write_file_entry(io, volume, path, size, idx, crypto) do
          {:ok, io, plaintext_bytes} ->
            new_counts = %{
              file_count: counts.file_count + 1,
              byte_count: counts.byte_count + plaintext_bytes
            }

            stream_files_loop(io, volume, new_counts, idx, crypto)

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

  defp write_file_entry(io, volume, path, size, idx, crypto) do
    {body_stream, finish_fn, plaintext_bytes} = entry_body(io, path, size, idx, crypto)

    case WriteOperation.write_file_streamed(volume.id, "/" <> path, body_stream) do
      {:ok, file_meta} ->
        # `write_file_streamed` consumed the stream, advancing the
        # underlying IO device past the (cipher)text body. Now skip the
        # ustar padding to land at the next entry's header.
        with {:ok, io} <- finish_fn.() do
          :ok = maybe_restore_metadata(file_meta, idx, path)
          {:ok, io, plaintext_bytes}
        end

      err ->
        err
    end
  end

  # Plaintext archive: the tar body is the file content; the stream and
  # byte count are the body itself. Encrypted archive: the tar body is
  # ciphertext frames; decrypt them lazily into the plaintext stream,
  # and report the plaintext size (from the manifest) for the count.
  defp entry_body(io, _path, size, _idx, nil) do
    {stream, finish_fn} = body_stream(io, size)
    {stream, finish_fn, size}
  end

  defp entry_body(io, path, enc_size, idx, %{content_key: content_key}) do
    plaintext_size = manifest_plaintext_size(idx, path)
    {stream, finish_fn} = encrypted_body_stream(io, enc_size, plaintext_size, content_key, path)
    {stream, finish_fn, plaintext_size}
  end

  defp manifest_plaintext_size(idx, path) do
    case Map.get(idx, "/" <> path) do
      %{"size" => size} when is_integer(size) -> size
      _ -> 0
    end
  end

  # If the manifest carried ACLs / xattrs / default_acl for this
  # path, apply them via `FileIndex.update/2` after the file's
  # content is written. The fields are absent when the export ran
  # without `--include-acls` / `--include-system-xattrs`.
  defp maybe_restore_metadata(file_meta, idx, path) do
    case Map.get(idx, "/" <> path) do
      nil ->
        :ok

      entry ->
        updates = manifest_entry_to_updates(entry)
        if updates == [], do: :ok, else: apply_updates(file_meta.id, updates)
    end
  end

  defp manifest_entry_to_updates(entry) do
    []
    |> maybe_add_acl(entry, "acl_entries", :acl_entries)
    |> maybe_add_acl(entry, "default_acl", :default_acl)
    |> maybe_add_xattrs(entry)
  end

  defp maybe_add_acl(updates, entry, key, field) do
    case Map.get(entry, key) do
      nil -> updates
      decoded -> [{field, decode_acl_entries(decoded)} | updates]
    end
  end

  defp maybe_add_xattrs(updates, %{"xattrs" => xattrs}) when is_map(xattrs) do
    [{:xattrs, decode_xattrs(xattrs)} | updates]
  end

  defp maybe_add_xattrs(updates, _entry), do: updates

  defp decode_acl_entries(list) when is_list(list) do
    Enum.map(list, fn entry ->
      %{
        type: String.to_existing_atom(entry["type"]),
        id: entry["id"],
        permissions:
          entry
          |> Map.get("permissions", [])
          |> Enum.map(&String.to_existing_atom/1)
          |> MapSet.new()
      }
    end)
  end

  defp decode_acl_entries(_), do: []

  defp decode_xattrs(map) do
    Map.new(map, fn {k, v} ->
      {decode_base64(k), decode_base64(v)}
    end)
  end

  defp decode_base64(bin) do
    case Base.decode64(bin) do
      {:ok, decoded} -> decoded
      _ -> bin
    end
  end

  defp apply_updates(file_id, updates) do
    case FileIndex.update(file_id, updates) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        # Metadata restore failure isn't fatal — the content is
        # already written. Log + continue.
        Logger.warning(
          "VolumeImport: failed to restore metadata for #{file_id}: #{inspect(reason)}"
        )

        :ok
    end
  end

  # Index the manifest's per-file entries by `path` so the streamer
  # can pluck the right entry as it processes each tar entry.
  defp manifest_index(%{"files" => files}) when is_list(files) do
    Map.new(files, fn entry -> {entry["path"], entry} end)
  end

  defp manifest_index(_), do: %{}

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

  # Decrypting body stream: reads one `nonce ‖ ciphertext ‖ tag` frame
  # at a time and yields its plaintext. The frame boundaries are derived
  # from the manifest's plaintext `size` (every frame but the last is
  # `frame_size`), so no length prefixes are needed on the wire. The
  # working set is one frame, never the whole file. The AAD path matches
  # the exporter's `file_meta.path` (leading slash).
  defp encrypted_body_stream(io, enc_size, plaintext_size, content_key, path) do
    aad_path = "/" <> path
    n_frames = BackupCrypto.frame_count(plaintext_size)

    stream =
      Stream.unfold(0, fn
        index when index >= n_frames ->
          nil

        index ->
          pt_len = BackupCrypto.frame_plaintext_len(plaintext_size, index, n_frames)
          frame = read_exact_frame!(io, pt_len + BackupCrypto.frame_overhead(), aad_path, index)

          case BackupCrypto.decrypt_frame(frame, content_key, aad_path, index) do
            {:ok, plaintext} ->
              {plaintext, index + 1}

            {:error, reason} ->
              raise "decrypt failed for #{aad_path} frame #{index}: #{inspect(reason)}"
          end
      end)

    finish_fn = fn -> skip_padding(io, enc_size) end
    {stream, finish_fn}
  end

  defp read_exact_frame!(io, len, path, index) do
    case binread_exact(io, len) do
      {:ok, frame} ->
        frame

      {:error, reason} ->
        raise "truncated encrypted frame for #{path} frame #{index}: #{inspect(reason)}"
    end
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
