defmodule NeonFS.S3.Backend do
  @moduledoc """
  Firkin.Backend implementation that maps S3 operations to NeonFS core calls.

  Buckets map 1:1 to NeonFS volumes. S3 object keys map to file paths within
  the volume. Control-plane operations go through `NeonFS.Client.Router`.
  Object GET streams chunks via `NeonFS.Client.ChunkReader.read_file_stream/3`
  so bulk bytes are fetched over the TLS data plane (or range-limited
  per-chunk RPCs for compressed/encrypted volumes) regardless of whether a
  core node is co-located on the S3 node's VM.
  """

  @behaviour Firkin.Backend

  alias NeonFS.Client.ChunkReader
  alias NeonFS.Client.ChunkWriter
  alias NeonFS.Client.Discovery
  alias NeonFS.Client.Router
  alias NeonFS.S3.MultipartStore

  require Logger

  # Credential lookup

  @impl true
  def lookup_credential(access_key_id) do
    case call_core(:lookup_s3_credential, [access_key_id]) do
      {:ok, %{secret_access_key: secret, identity: identity}} ->
        {:ok,
         %Firkin.Credential{
           access_key_id: access_key_id,
           secret_access_key: secret,
           identity: identity
         }}

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, _reason} ->
        {:error, :not_found}
    end
  end

  # Bucket operations

  @impl true
  def list_buckets(_ctx) do
    case call_core(:list_volumes, []) do
      {:ok, volumes} ->
        buckets =
          volumes
          |> Enum.map(fn vol ->
            %Firkin.Bucket{name: vol.name, creation_date: vol.created_at}
          end)
          |> Enum.sort_by(& &1.name)

        {:ok, buckets}

      {:error, reason} ->
        {:error, internal_error(reason)}
    end
  end

  @impl true
  def create_bucket(_ctx, bucket) do
    case call_core(:create_volume, [bucket]) do
      {:ok, _volume} -> :ok
      {:error, :already_exists} -> {:error, %Firkin.Error{code: :bucket_already_exists}}
      {:error, reason} -> {:error, internal_error(reason)}
    end
  end

  @impl true
  def delete_bucket(_ctx, bucket) do
    with :ok <- ensure_bucket_exists(bucket),
         :ok <- ensure_bucket_empty(bucket) do
      case call_core(:delete_volume, [bucket]) do
        :ok -> :ok
        {:error, reason} -> {:error, internal_error(reason)}
      end
    end
  end

  @impl true
  def head_bucket(_ctx, bucket) do
    ensure_bucket_exists(bucket)
  end

  @impl true
  def get_bucket_location(_ctx, bucket) do
    case ensure_bucket_exists(bucket) do
      :ok -> {:ok, Application.get_env(:neonfs_s3, :region, "neonfs")}
      error -> error
    end
  end

  # Object operations

  @impl true
  def get_object(_ctx, bucket, key, opts) do
    with :ok <- ensure_bucket_exists(bucket),
         {:ok, meta} <- fetch_object_meta(bucket, key) do
      read_opts = range_to_read_opts(opts.range, meta.size)
      build_object(bucket, key, meta, read_opts)
    end
  end

  defp build_object(bucket, key, meta, read_opts) do
    case try_stream_read(bucket, key, read_opts) do
      {:ok, %{stream: stream}} ->
        {:ok, file_meta_to_stream_object(meta, stream, read_opts)}

      {:error, :not_found} ->
        {:error, %Firkin.Error{code: :no_such_key}}

      {:error, reason} ->
        {:error, internal_error(reason)}
    end
  end

  @impl true
  def put_object(_ctx, bucket, key, body, opts) do
    with :ok <- ensure_bucket_exists(bucket) do
      case write_via_chunk_writer(bucket, key, body, put_object_write_opts(opts)) do
        {:ok, meta} -> {:ok, compute_etag_from_meta(meta)}
        {:error, reason} -> {:error, internal_error(reason)}
      end
    end
  end

  @impl true
  def put_object_stream(_ctx, bucket, key, body, opts) do
    with :ok <- ensure_bucket_exists(bucket) do
      write_opts = put_object_write_opts(opts)
      do_put_object_stream(bucket, key, body, write_opts)
    end
  end

  defp do_put_object_stream(bucket, key, body, write_opts) do
    case stream_write(bucket, key, body, write_opts) do
      {:ok, meta} -> {:ok, compute_etag_from_meta(meta)}
      {:error, reason} -> {:error, internal_error(reason)}
    end
  end

  # Writes a byte stream to `bucket/key`. Prefers the co-located
  # `write_file_streamed/4` fast path when it's reachable (via
  # `call_core_stream/2`); falls back to chunking locally and pushing
  # over the TLS data plane when the core node is remote. Peak memory
  # is bounded by the chunker's max chunk size in either path.
  defp stream_write(bucket, key, body, write_opts) do
    case call_core_stream(:write_file_streamed, [bucket, key, body, write_opts]) do
      {:error, :not_available} -> write_via_chunk_writer(bucket, key, body, write_opts)
      other -> other
    end
  end

  defp put_object_write_opts(opts) do
    []
    |> maybe_put_content_type(opts.content_type)
    |> maybe_put_metadata(opts.metadata)
  end

  @impl true
  def delete_object(_ctx, bucket, key) do
    case call_core(:delete_file, [bucket, key]) do
      :ok -> :ok
      {:error, :not_found} -> :ok
      {:error, reason} -> {:error, internal_error(reason)}
    end
  end

  @impl true
  def delete_objects(_ctx, bucket, keys) do
    results =
      Enum.map(keys, fn key ->
        case call_core(:delete_file, [bucket, key]) do
          :ok ->
            {:deleted, %{key: key}}

          {:error, :not_found} ->
            {:deleted, %{key: key}}

          {:error, reason} ->
            {:error, %{key: key, code: "InternalError", message: inspect(reason)}}
        end
      end)

    deleted = for {:deleted, entry} <- results, do: entry
    errors = for {:error, entry} <- results, do: entry

    {:ok, %Firkin.DeleteResult{deleted: deleted, errors: errors}}
  end

  @impl true
  def head_object(_ctx, bucket, key) do
    with :ok <- ensure_bucket_exists(bucket) do
      case call_core(:get_file_meta, [bucket, key]) do
        {:ok, meta} ->
          {:ok, file_meta_to_object_meta(meta, key)}

        {:error, :not_found} ->
          {:error, %Firkin.Error{code: :no_such_key}}

        {:error, reason} ->
          {:error, internal_error(reason)}
      end
    end
  end

  @impl true
  def list_objects_v2(_ctx, bucket, opts) do
    with :ok <- ensure_bucket_exists(bucket) do
      list_path = prefix_to_path(opts.prefix)

      case call_core(:list_files_recursive, [bucket, list_path]) do
        {:ok, entries} ->
          {:ok, build_list_result(bucket, entries, opts)}

        {:error, :not_found} ->
          {:ok, empty_list_result(bucket, opts)}

        {:error, reason} ->
          {:error, internal_error(reason)}
      end
    end
  end

  @impl true
  def copy_object(_ctx, dest_bucket, _dest_key, source_bucket, _source_key)
      when dest_bucket != source_bucket do
    {:error,
     %Firkin.Error{
       code: :not_implemented,
       message: "Cross-bucket CopyObject is not supported"
     }}
  end

  def copy_object(_ctx, bucket, dest_key, bucket, source_key) do
    with :ok <- ensure_bucket_exists(bucket),
         {:ok, source_meta} <- fetch_object_meta(bucket, source_key),
         {:ok, %{stream: source_stream}} <- try_stream_read(bucket, source_key, []),
         write_opts = content_type_write_opts(source_meta),
         {:ok, dest_meta} <- stream_write(bucket, dest_key, source_stream, write_opts) do
      {:ok,
       %Firkin.CopyResult{
         etag: compute_etag_from_meta(dest_meta),
         last_modified: DateTime.utc_now()
       }}
    else
      {:error, :not_found} -> {:error, %Firkin.Error{code: :no_such_key}}
      {:error, %Firkin.Error{}} = err -> err
      {:error, reason} -> {:error, internal_error(reason)}
    end
  end

  # Multipart upload operations

  @impl true
  def create_multipart_upload(_ctx, bucket, key, opts) do
    with :ok <- ensure_bucket_exists(bucket) do
      content_type = Map.get(opts, :content_type, "application/octet-stream")
      upload_id = MultipartStore.create(bucket, key, content_type)
      {:ok, upload_id}
    end
  end

  @impl true
  def upload_part(_ctx, _bucket, _key, upload_id, part_number, body) do
    body_binary = IO.iodata_to_binary(body)
    etag = compute_etag(body_binary)

    case MultipartStore.get(upload_id) do
      {:ok, upload} ->
        log_label = multipart_log_label(upload_id, part_number)

        case ship_chunks(upload.bucket, log_label, body_binary) do
          {:ok, refs} ->
            part = %{etag: etag, size: byte_size(body_binary), chunk_refs: refs}
            MultipartStore.put_part(upload_id, part_number, part)
            {:ok, etag}

          {:error, reason} ->
            {:error, internal_error(reason)}
        end

      {:error, :not_found} ->
        {:error, %Firkin.Error{code: :no_such_upload}}
    end
  end

  @impl true
  def upload_part_stream(_ctx, _bucket, _key, upload_id, part_number, body) do
    case MultipartStore.get(upload_id) do
      {:ok, upload} ->
        do_upload_part_stream(upload, upload_id, part_number, body)

      {:error, :not_found} ->
        {:error, %Firkin.Error{code: :no_such_upload}}
    end
  end

  defp do_upload_part_stream(upload, upload_id, part_number, body) do
    {tracked, finish} = track_md5_and_size(body)
    log_label = multipart_log_label(upload_id, part_number)

    case ship_chunks(upload.bucket, log_label, tracked) do
      {:ok, refs} ->
        %{md5: md5, size: size} = finish.()
        record_part(upload_id, part_number, refs, md5, size)

      {:error, reason} ->
        _ = finish.()
        {:error, internal_error(reason)}
    end
  end

  defp record_part(upload_id, part_number, refs, md5, size) do
    etag = Base.encode16(md5, case: :lower)

    MultipartStore.put_part(upload_id, part_number, %{
      etag: etag,
      size: size,
      chunk_refs: refs
    })

    {:ok, etag}
  end

  # Returns `{stream, finish_fn}` where `stream` yields each chunk of `body`
  # unchanged while accumulating md5 state and total size in the process
  # dictionary. After the stream is consumed (by `call_core_stream` or an
  # explicit `Enum.to_list` drain), call `finish_fn.()` to retrieve the
  # final md5 digest and size.
  defp track_md5_and_size(body) do
    key = {__MODULE__, :md5_tracker, make_ref()}
    Process.put(key, %{md5: :crypto.hash_init(:md5), size: 0})

    stream =
      Stream.map(body, fn chunk ->
        state = Process.get(key)

        Process.put(key, %{
          md5: :crypto.hash_update(state.md5, chunk),
          size: state.size + IO.iodata_length(chunk)
        })

        chunk
      end)

    finish = fn ->
      state = Process.get(key)
      Process.delete(key)
      %{md5: :crypto.hash_final(state.md5), size: state.size}
    end

    {stream, finish}
  end

  defp call_core_stream(function, args) do
    case Application.get_env(:neonfs_s3, :core_call_fn) do
      nil ->
        if local_core?() do
          apply(NeonFS.Core, function, args)
        else
          {:error, :not_available}
        end

      fun when is_function(fun, 2) ->
        fun.(function, args)
    end
  end

  defp local_core? do
    node() in Discovery.get_core_nodes()
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  @impl true
  def complete_multipart_upload(_ctx, bucket, key, upload_id, _parts) do
    case MultipartStore.get(upload_id) do
      {:ok, upload} ->
        sorted_parts = Enum.sort_by(upload.parts, &elem(&1, 0))
        flattened_refs = Enum.flat_map(sorted_parts, fn {_num, part} -> part.chunk_refs end)

        write_opts =
          []
          |> maybe_put_content_type(upload.content_type)

        case commit_refs(bucket, key, flattened_refs, write_opts) do
          {:ok, meta} ->
            MultipartStore.delete(upload_id)

            {:ok,
             %Firkin.CompleteResult{
               location: "/#{bucket}/#{key}",
               bucket: bucket,
               key: key,
               etag: compute_etag_from_meta(meta)
             }}

          {:error, reason} ->
            {:error, internal_error(reason)}
        end

      {:error, :not_found} ->
        {:error, %Firkin.Error{code: :no_such_upload}}
    end
  end

  @impl true
  def abort_multipart_upload(_ctx, _bucket, _key, upload_id) do
    # No per-part `FileIndex` entries are ever created on the new
    # path, so there's nothing to delete — the shipped chunks are
    # orphaned and will be reaped by the core-side GC the first time
    # it runs. Best-effort `ChunkWriter` abort callbacks are not
    # invoked here because the refs have already escaped to the
    # `MultipartStore`; we'd need the writer's in-flight state to
    # do that safely. The interface-side `PendingWriteLog` deferred
    # in #450 is the long-term home for tracked aborts.
    case MultipartStore.get(upload_id) do
      {:ok, _upload} ->
        MultipartStore.delete(upload_id)
        :ok

      {:error, :not_found} ->
        {:error, %Firkin.Error{code: :no_such_upload}}
    end
  end

  @impl true
  def list_multipart_uploads(_ctx, bucket, _opts) do
    uploads = MultipartStore.list_for_bucket(bucket)
    {:ok, %Firkin.MultipartList{bucket: bucket, uploads: uploads}}
  end

  @impl true
  def list_parts(_ctx, _bucket, _key, upload_id, _opts) do
    case MultipartStore.get(upload_id) do
      {:ok, upload} ->
        parts =
          upload.parts
          |> Enum.map(fn {num, part} ->
            %{
              part_number: num,
              etag: part.etag,
              size: part.size,
              last_modified: DateTime.utc_now()
            }
          end)
          |> Enum.sort_by(& &1.part_number)

        {:ok,
         %Firkin.PartList{
           bucket: upload.bucket,
           key: upload.key,
           upload_id: upload_id,
           parts: parts
         }}

      {:error, :not_found} ->
        {:error, %Firkin.Error{code: :no_such_upload}}
    end
  end

  # Private helpers

  defp try_stream_read(bucket, key, read_opts) do
    case Application.get_env(:neonfs_s3, :core_stream_fn) do
      fun when is_function(fun, 3) ->
        fun.(bucket, key, read_opts)

      nil ->
        ChunkReader.read_file_stream(bucket, key, read_opts)
    end
  end

  defp file_meta_to_stream_object(meta, stream, read_opts) do
    etag = compute_etag_from_meta(meta)
    content_length = stream_content_length(meta.size, read_opts)

    %Firkin.Object{
      body: stream,
      content_type: meta_content_type(meta),
      content_length: content_length,
      total_size: meta.size,
      etag: etag,
      last_modified: meta.modified_at || meta.created_at || DateTime.utc_now(),
      metadata: %{}
    }
  end

  defp stream_content_length(file_size, []), do: file_size

  defp stream_content_length(file_size, opts) do
    offset = Keyword.get(opts, :offset, 0)
    length = Keyword.get(opts, :length, file_size - offset)
    min(length, file_size - offset)
  end

  defp call_core(function, args) do
    case Application.get_env(:neonfs_s3, :core_call_fn) do
      nil -> Router.call(NeonFS.Core, function, args)
      fun when is_function(fun, 2) -> fun.(function, args)
    end
  end

  # Streaming write via `NeonFS.Client.ChunkWriter` + `commit_chunks/4`.
  # Accepts a binary, iodata, or an Enumerable of binary segments. The
  # input is chunked on this (S3) node and pushed over the TLS data
  # plane, then finalised with a metadata-only `commit_chunks/4` RPC.
  # Peak memory is bounded by the chunker's max chunk size, not the
  # upload size.
  #
  # Returns the FileMeta on success, matching the shape of the
  # `call_core(:write_file_at, …)` it replaced (#411).
  #
  # Unit tests inject `:write_via_chunk_writer_fn` to route around the
  # real ChunkWriter / Router machinery — same shape as
  # `:core_call_fn` for the rest of the backend.
  defp write_via_chunk_writer(bucket, key, body, write_opts) do
    case Application.get_env(:neonfs_s3, :write_via_chunk_writer_fn) do
      nil -> do_write_via_chunk_writer(bucket, key, body, write_opts)
      fun when is_function(fun, 4) -> fun.(bucket, key, body, write_opts)
    end
  end

  defp do_write_via_chunk_writer(bucket, key, body, write_opts) do
    with {:ok, refs} <- ship_chunks(bucket, key, body) do
      commit_refs(bucket, key, refs, write_opts)
    end
  end

  # Chunks the body locally and ships each chunk to a core node over
  # the TLS data plane, returning the per-chunk ref list. No
  # `commit_chunks/4` RPC — callers that need a finalised file call
  # `commit_refs/4` separately. Multipart upload parts use this
  # directly so the chunk list can be stitched together in
  # `complete_multipart_upload/5` without per-part `FileIndex` entries.
  #
  # Unit tests inject `:ship_chunks_fn` to stash synthetic refs
  # without a running cluster — shape: `(bucket, key, body) ->
  # {:ok, [chunk_ref]} | {:error, reason}`.
  defp ship_chunks(bucket, key, body) do
    case Application.get_env(:neonfs_s3, :ship_chunks_fn) do
      nil ->
        stream = body_to_stream(body)
        ChunkWriter.write_file_stream(bucket, key, stream)

      fun when is_function(fun, 3) ->
        fun.(bucket, key, body)
    end
  end

  # Commits an ordered list of chunk refs as a file at `bucket/key`
  # via the metadata-only `commit_chunks/4` RPC. Refs are typically
  # the output of `ship_chunks/3`, either for a single object or
  # flattened across multipart parts.
  #
  # Unit tests inject `:commit_refs_fn` — shape:
  # `(bucket, key, refs, write_opts) -> {:ok, FileMeta} | {:error, reason}`.
  defp commit_refs(bucket, key, refs, write_opts) do
    case Application.get_env(:neonfs_s3, :commit_refs_fn) do
      nil ->
        commit_opts = build_commit_opts(refs, write_opts)

        Router.call(NeonFS.Core, :commit_chunks, [
          bucket,
          key,
          commit_opts.hashes,
          commit_opts.extra
        ])

      fun when is_function(fun, 4) ->
        fun.(bucket, key, refs, write_opts)
    end
  end

  defp body_to_stream(body) when is_binary(body), do: [body]
  defp body_to_stream(body) when is_list(body), do: [IO.iodata_to_binary(body)]
  defp body_to_stream(stream), do: stream

  defp build_commit_opts(refs, write_opts) do
    %{hashes: hashes, locations: locations, chunk_codecs: chunk_codecs, total_size: total_size} =
      ChunkWriter.chunk_refs_to_commit_opts(refs)

    extra =
      [
        total_size: total_size,
        locations: locations,
        chunk_codecs: chunk_codecs
      ]
      |> Keyword.merge(Keyword.take(write_opts, [:content_type, :metadata, :mode, :uid, :gids]))

    %{hashes: hashes, extra: extra}
  end

  defp ensure_bucket_exists(bucket) do
    case call_core(:get_volume, [bucket]) do
      {:ok, _volume} -> :ok
      {:error, :not_found} -> {:error, %Firkin.Error{code: :no_such_bucket}}
      {:error, reason} -> {:error, internal_error(reason)}
    end
  end

  defp ensure_bucket_empty(bucket) do
    case call_core(:list_files_recursive, [bucket, "/"]) do
      {:ok, []} -> :ok
      {:ok, _entries} -> {:error, %Firkin.Error{code: :bucket_not_empty}}
      {:error, :not_found} -> :ok
      {:error, reason} -> {:error, internal_error(reason)}
    end
  end

  defp range_to_read_opts(nil, _file_size), do: []

  defp range_to_read_opts({start_byte, end_byte}, file_size) do
    clamped_end = min(end_byte, file_size - 1)
    [offset: start_byte, length: clamped_end - start_byte + 1]
  end

  defp fetch_object_meta(bucket, key) do
    case call_core(:get_file_meta, [bucket, key]) do
      {:ok, meta} -> {:ok, meta}
      {:error, :not_found} -> {:error, %Firkin.Error{code: :no_such_key}}
      {:error, reason} -> {:error, internal_error(reason)}
    end
  end

  defp compute_etag(data) when is_binary(data) do
    :crypto.hash(:md5, data) |> Base.encode16(case: :lower)
  end

  defp file_meta_to_object_meta(meta, key) do
    %Firkin.ObjectMeta{
      key: strip_leading_slash(key),
      etag: compute_etag_from_meta(meta),
      size: meta.size,
      last_modified: meta.modified_at || meta.created_at || DateTime.utc_now(),
      content_type: meta_content_type(meta)
    }
  end

  defp meta_content_type(%{content_type: ct}) when is_binary(ct), do: ct
  defp meta_content_type(_meta), do: "application/octet-stream"

  defp compute_etag_from_meta(%{chunks: chunks}) when is_list(chunks) and chunks != [] do
    chunks
    |> IO.iodata_to_binary()
    |> then(&:crypto.hash(:md5, &1))
    |> Base.encode16(case: :lower)
  end

  defp compute_etag_from_meta(%{size: size}) do
    :crypto.hash(:md5, <<size::64>>) |> Base.encode16(case: :lower)
  end

  defp prefix_to_path(nil), do: "/"
  defp prefix_to_path(""), do: "/"

  defp prefix_to_path(prefix) do
    parts = String.split(prefix, "/", trim: true)

    case parts do
      [] -> "/"
      _ -> "/" <> Enum.join(parts, "/")
    end
  end

  defp build_list_result(bucket, entries, opts) do
    prefix = opts.prefix
    delimiter = opts.delimiter

    object_metas =
      entries
      |> Enum.map(fn meta ->
        key = strip_leading_slash(meta.path)

        %Firkin.ObjectMeta{
          key: key,
          etag: compute_etag_from_meta(meta),
          size: meta.size,
          last_modified: meta.modified_at || meta.created_at || DateTime.utc_now(),
          content_type: meta_content_type(meta)
        }
      end)
      |> Enum.filter(fn meta ->
        if prefix, do: String.starts_with?(meta.key, prefix), else: true
      end)
      |> Enum.sort_by(& &1.key)

    {contents, common_prefixes} = split_by_delimiter(object_metas, prefix, delimiter)

    max_keys = opts.max_keys
    truncated = length(contents) > max_keys
    contents = Enum.take(contents, max_keys)

    %Firkin.ListResult{
      name: bucket,
      prefix: prefix,
      delimiter: delimiter,
      contents: contents,
      common_prefixes: common_prefixes,
      key_count: length(contents),
      max_keys: max_keys,
      is_truncated: truncated
    }
  end

  defp empty_list_result(bucket, opts) do
    %Firkin.ListResult{
      name: bucket,
      prefix: opts.prefix,
      delimiter: opts.delimiter,
      contents: [],
      common_prefixes: [],
      key_count: 0,
      max_keys: opts.max_keys,
      is_truncated: false
    }
  end

  defp split_by_delimiter(entries, _prefix, nil), do: {entries, []}

  defp split_by_delimiter(entries, prefix, delimiter) do
    prefix_len = String.length(prefix || "")

    {regular, prefixed} =
      Enum.split_with(entries, fn meta ->
        rest = String.slice(meta.key, prefix_len..-1//1)
        not String.contains?(rest, delimiter)
      end)

    prefix_set =
      prefixed
      |> Enum.map(fn meta ->
        rest = String.slice(meta.key, prefix_len..-1//1)
        idx = :binary.match(rest, delimiter) |> elem(0)
        (prefix || "") <> String.slice(rest, 0, idx + 1)
      end)
      |> Enum.uniq()
      |> Enum.sort()

    {regular, prefix_set}
  end

  defp strip_leading_slash("/" <> rest), do: rest
  defp strip_leading_slash(path), do: path

  # Label used for log metadata on `ship_chunks/3` for multipart
  # parts — never a real file path since nothing writes a part file
  # anymore.
  defp multipart_log_label(upload_id, part_number) do
    "/.neonfs-multipart/#{upload_id}/part-#{part_number}"
  end

  defp internal_error(reason) do
    Logger.error("S3 backend error", reason: inspect(reason))
    %Firkin.Error{code: :internal_error}
  end

  defp content_type_write_opts(%{content_type: ct}) when is_binary(ct), do: [content_type: ct]
  defp content_type_write_opts(_meta), do: []

  defp maybe_put_content_type(opts, "application/octet-stream"), do: opts
  defp maybe_put_content_type(opts, content_type), do: [{:content_type, content_type} | opts]

  defp maybe_put_metadata(opts, metadata) when map_size(metadata) == 0, do: opts
  defp maybe_put_metadata(opts, metadata), do: [{:metadata, metadata} | opts]
end
