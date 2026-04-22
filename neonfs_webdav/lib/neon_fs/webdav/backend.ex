defmodule NeonFS.WebDAV.Backend do
  @moduledoc """
  Davy.Backend implementation that maps WebDAV operations to NeonFS
  core calls.

  The root collection lists NeonFS volumes. The first path segment identifies
  the volume, and remaining segments map to file paths within the volume.
  Control-plane operations go through `NeonFS.Client.Router`; GET content
  falls back to `NeonFS.Client.ChunkReader` so chunk bytes are fetched over
  the TLS data plane rather than shipped across Erlang distribution when
  streaming isn't available.
  """

  @behaviour Davy.Backend

  alias NeonFS.Client.ChunkReader
  alias NeonFS.Client.Discovery
  alias NeonFS.Client.Router
  alias NeonFS.WebDAV.LockStore

  require Logger
  import Bitwise

  @s_ifmt 0o170000
  @s_ifdir 0o040000

  # --- Authentication ---

  @impl true
  def authenticate(_conn) do
    {:ok, %{user: "anonymous"}}
  end

  # --- Resource resolution ---

  @impl true
  def resolve(_auth, []) do
    {:ok, root_resource()}
  end

  def resolve(_auth, [volume_name]) do
    case call_core(:get_volume, [volume_name]) do
      {:ok, volume} ->
        {:ok, volume_resource(volume)}

      {:error, :not_found} ->
        {:error, %Davy.Error{code: :not_found}}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  def resolve(_auth, [volume_name | rest] = path) do
    file_path = "/" <> Enum.join(rest, "/")

    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, meta} <- call_core(:get_file_meta, [volume_name, file_path]) do
      {:ok, file_resource(volume_name, volume.id, rest, meta)}
    else
      {:error, :not_found} ->
        if LockStore.lock_null?(path) do
          {:ok, lock_null_resource(volume_name, rest)}
        else
          {:error, %Davy.Error{code: :not_found}}
        end

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  # --- Properties ---

  @impl true
  def get_properties(_auth, resource, properties) do
    Enum.map(properties, fn prop -> {prop, get_property(resource, prop)} end)
  end

  @impl true
  def set_properties(_auth, %{backend_data: %{type: type}}, _operations)
      when type in [:root, :volume] do
    {:error, %Davy.Error{code: :forbidden, message: "Cannot set properties on #{type}"}}
  end

  def set_properties(_auth, resource, operations) do
    %{volume_name: volume_name, file_path: file_path, meta: meta} = resource.backend_data
    current_metadata = meta.metadata || %{}

    new_metadata =
      Enum.reduce(operations, current_metadata, fn
        {:set, {namespace, name}, value}, acc ->
          Map.put(acc, "#{namespace}#{name}", value)

        {:remove, {namespace, name}}, acc ->
          Map.delete(acc, "#{namespace}#{name}")
      end)

    case call_core(:update_file_meta, [volume_name, file_path, [metadata: new_metadata]]) do
      {:ok, _updated} -> :ok
      {:error, _reason} -> {:error, internal_error()}
    end
  end

  # --- File operations ---

  @impl true
  def get_content(_auth, %{backend_data: %{lock_null: true}}, _opts) do
    {:error, %Davy.Error{code: :not_found}}
  end

  def get_content(_auth, resource, opts) do
    %{volume_name: volume_name, file_path: file_path} = resource.backend_data
    read_opts = range_to_read_opts(opts)

    case try_stream_read(volume_name, file_path, read_opts) do
      {:ok, %{stream: stream}} ->
        {:ok, stream}

      {:error, :not_found} ->
        {:error, %Davy.Error{code: :not_found}}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  @impl true
  def put_content(auth, path, body, opts) do
    # Delegate to the streaming callback. The buffered-body callers have
    # already materialised the bytes in memory; wrap them as a
    # single-element stream so the write path can continue to be a
    # single `write_file_streamed/4` call. The stream doesn't introduce
    # any new buffering — the body is already one value.
    #
    # `body` is iodata from Plug; the streaming write path's chunker NIF
    # requires a binary segment, so collapse iodata to a binary at the
    # stream boundary.
    stream = Stream.map([body], &IO.iodata_to_binary/1)
    put_content_stream(auth, path, stream, opts)
  end

  @impl true
  def put_content_stream(_auth, [volume_name | rest] = path, stream, opts) do
    file_path = "/" <> Enum.join(rest, "/")
    write_opts = put_content_opts(opts)

    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, meta} <- streaming_write(volume_name, file_path, stream, write_opts) do
      LockStore.promote_lock_null(path, meta.id)
      {:ok, file_resource(volume_name, volume.id, rest, meta)}
    else
      {:error, :not_found} ->
        {:error, %Davy.Error{code: :conflict, message: "Volume not found"}}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  def put_content_stream(_auth, [], _stream, _opts) do
    {:error, %Davy.Error{code: :forbidden, message: "Cannot write to root"}}
  end

  defp streaming_write(volume_name, file_path, stream, write_opts) do
    case call_core_stream(:write_file_streamed, [volume_name, file_path, stream, write_opts]) do
      {:error, :not_available} ->
        # Streams cannot cross Erlang distribution. When the core node is
        # remote we drain the stream into a binary and use the batch API
        # — same memory characteristics as the non-streaming code path.
        # audit:bounded cross-node fallback tracked in #299 (streaming write RPC)
        body = stream |> Enum.to_list() |> IO.iodata_to_binary()
        call_core(:write_file, [volume_name, file_path, body, write_opts])

      other ->
        other
    end
  end

  defp call_core_stream(function, args) do
    case Application.get_env(:neonfs_webdav, :core_call_fn) do
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

  defp put_content_opts(opts) do
    case Map.get(opts, :content_type) do
      nil -> []
      "application/octet-stream" -> []
      ct -> [content_type: ct]
    end
  end

  @impl true
  def delete(_auth, %{backend_data: %{type: :root}}) do
    {:error, %Davy.Error{code: :forbidden, message: "Cannot delete root"}}
  end

  def delete(_auth, %{backend_data: %{type: :volume}}) do
    {:error, %Davy.Error{code: :forbidden, message: "Cannot delete volumes via WebDAV"}}
  end

  def delete(_auth, %{backend_data: %{lock_null: true}} = resource) do
    path = resource.path

    locks = LockStore.get_locks(path)

    Enum.each(locks, fn lock ->
      LockStore.unlock(lock.token)
    end)

    :ok
  end

  def delete(_auth, resource) do
    %{volume_name: volume_name, file_path: file_path} = resource.backend_data

    case call_core(:delete_file, [volume_name, file_path]) do
      :ok -> :ok
      {:error, :not_found} -> :ok
      {:error, _reason} -> {:error, internal_error()}
    end
  end

  @impl true
  def copy(_auth, resource, dest_path, overwrite?) do
    %{volume_name: src_volume, file_path: src_path, meta: src_meta} = resource.backend_data

    with :ok <- validate_dest(dest_path),
         {dest_volume, dest_file_path} = split_dest(dest_path),
         :ok <- check_same_volume(src_volume, dest_volume),
         :ok <- check_overwrite(dest_volume, dest_file_path, overwrite?),
         {:ok, %{stream: stream}} <- try_stream_read(src_volume, src_path, []),
         existed? = resource_exists?(dest_volume, dest_file_path),
         write_opts = content_type_opts(src_meta) ++ metadata_opts(src_meta),
         {:ok, _meta} <-
           streaming_write(dest_volume, dest_file_path, stream, write_opts) do
      if existed?, do: {:ok, :no_content}, else: {:ok, :created}
    else
      {:error, %Davy.Error{}} = err -> err
      {:error, :not_found} -> {:error, %Davy.Error{code: :not_found}}
      {:error, _reason} -> {:error, internal_error()}
    end
  end

  @impl true
  def move(_auth, resource, dest_path, overwrite?) do
    %{volume_name: src_volume, file_path: src_path} = resource.backend_data

    with :ok <- validate_dest(dest_path),
         {dest_volume, dest_file_path} = split_dest(dest_path),
         :ok <- check_same_volume(src_volume, dest_volume),
         :ok <- check_overwrite(dest_volume, dest_file_path, overwrite?) do
      existed? = resource_exists?(dest_volume, dest_file_path)
      move_same_volume(src_volume, src_path, dest_file_path, existed?)
    end
  end

  # --- Collection operations ---

  @impl true
  def create_collection(_auth, [volume_name | rest]) do
    dir_path = "/" <> Enum.join(rest, "/")

    case resolve_volume(volume_name) do
      {:ok, _volume} ->
        case call_core(:mkdir, [volume_name, dir_path]) do
          {:ok, _meta} -> :ok
          :ok -> :ok
          {:error, :already_exists} -> {:error, %Davy.Error{code: :method_not_allowed}}
          {:error, _reason} -> {:error, internal_error()}
        end

      {:error, :not_found} ->
        {:error, %Davy.Error{code: :conflict, message: "Volume not found"}}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  def create_collection(_auth, []) do
    {:error, %Davy.Error{code: :forbidden, message: "Cannot create root collection"}}
  end

  @impl true
  def get_members(_auth, %{backend_data: %{type: :root}}) do
    case call_core(:list_volumes, []) do
      {:ok, volumes} ->
        resources =
          volumes
          |> Enum.reject(& &1.system)
          |> Enum.map(&volume_resource/1)
          |> Enum.sort_by(fn r -> r.display_name end)

        {:ok, resources}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  def get_members(_auth, %{backend_data: %{type: :volume, volume_name: volume_name}} = resource) do
    list_dir_members_with_lock_null(
      volume_name,
      resource.backend_data.volume_id,
      "/",
      [volume_name]
    )
  end

  def get_members(_auth, resource) do
    %{volume_name: volume_name, volume_id: volume_id, file_path: file_path} =
      resource.backend_data

    parent_path = [volume_name | path_to_segments(file_path)]
    list_dir_members_with_lock_null(volume_name, volume_id, file_path, parent_path)
  end

  # --- Private helpers ---

  defp try_stream_read(volume_name, file_path, read_opts) do
    case Application.get_env(:neonfs_webdav, :core_stream_fn) do
      fun when is_function(fun, 3) ->
        fun.(volume_name, file_path, read_opts)

      nil ->
        ChunkReader.read_file_stream(volume_name, file_path, read_opts)
    end
  end

  defp move_same_volume(volume, src_path, dest_path, existed?) do
    case call_core(:rename_file, [volume, src_path, dest_path]) do
      :ok ->
        if existed?, do: {:ok, :no_content}, else: {:ok, :created}

      {:error, :not_found} ->
        {:error, %Davy.Error{code: :not_found}}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  defp check_same_volume(src_volume, dest_volume) when src_volume == dest_volume, do: :ok

  defp check_same_volume(_src_volume, _dest_volume) do
    {:error,
     %Davy.Error{
       code: :bad_gateway,
       message: "Cross-volume COPY/MOVE is not supported"
     }}
  end

  defp call_core(function, args) do
    case Application.get_env(:neonfs_webdav, :core_call_fn) do
      nil -> Router.call(NeonFS.Core, function, args)
      fun when is_function(fun, 2) -> fun.(function, args)
    end
  end

  defp range_to_read_opts(%{range: {start_byte, nil}}), do: [offset: start_byte]

  defp range_to_read_opts(%{range: {start_byte, end_byte}}) do
    [offset: start_byte, length: end_byte - start_byte + 1]
  end

  defp range_to_read_opts(_), do: []

  defp root_resource do
    %Davy.Resource{
      path: [],
      type: :collection,
      display_name: "NeonFS",
      last_modified: nil,
      creation_date: nil,
      backend_data: %{type: :root}
    }
  end

  defp volume_resource(volume) do
    %Davy.Resource{
      path: [volume.name],
      type: :collection,
      display_name: volume.name,
      last_modified: volume.updated_at || volume.created_at,
      creation_date: volume.created_at,
      backend_data: %{type: :volume, volume_name: volume.name, volume_id: volume.id}
    }
  end

  defp file_resource(volume_name, volume_id, path_segments, meta) do
    type = if directory?(meta.mode), do: :collection, else: :file

    %Davy.Resource{
      path: [volume_name | path_segments],
      type: type,
      etag: compute_etag(meta),
      content_type: if(type == :file, do: meta.content_type || "application/octet-stream"),
      content_length: if(type == :file, do: meta.size),
      last_modified: meta.modified_at || meta.created_at,
      creation_date: meta.created_at,
      display_name: List.last(path_segments),
      backend_data: %{
        type: type,
        volume_name: volume_name,
        volume_id: volume_id,
        file_path: "/" <> Enum.join(path_segments, "/"),
        meta: meta
      }
    }
  end

  defp lock_null_resource(volume_name, path_segments) do
    %Davy.Resource{
      path: [volume_name | path_segments],
      type: :file,
      content_type: "application/octet-stream",
      content_length: 0,
      last_modified: nil,
      creation_date: nil,
      display_name: List.last(path_segments),
      backend_data: %{
        type: :file,
        lock_null: true,
        volume_name: volume_name,
        file_path: "/" <> Enum.join(path_segments, "/")
      }
    }
  end

  defp directory?(nil), do: false
  defp directory?(mode), do: (mode &&& @s_ifmt) == @s_ifdir

  defp compute_etag(meta) do
    data = "#{meta.id || ""}:#{meta.version || 0}:#{meta.size || 0}"
    :crypto.hash(:md5, data) |> Base.encode16(case: :lower)
  end

  defp resolve_volume(volume_name) do
    case call_core(:get_volume, [volume_name]) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :not_found}
      {:error, _reason} -> {:error, :internal}
    end
  end

  defp list_dir_members_with_lock_null(volume_name, volume_id, dir_path, parent_path) do
    case call_core(:list_dir, [volume_name, dir_path]) do
      {:ok, entries} ->
        real_resources =
          Enum.map(entries, fn meta ->
            segments = path_to_segments(meta.path)
            file_resource(volume_name, volume_id, segments, meta)
          end)

        lock_null_resources =
          parent_path
          |> LockStore.get_lock_null_paths()
          |> Enum.map(fn [_vol | rest] = _path -> lock_null_resource(volume_name, rest) end)

        {:ok, real_resources ++ lock_null_resources}

      {:error, :not_found} ->
        {:ok, []}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  defp path_to_segments(path) do
    path
    |> String.trim_leading("/")
    |> String.split("/", trim: true)
  end

  defp get_property(resource, {"DAV:", "resourcetype"}) do
    if resource.type == :collection, do: {:ok, :collection}, else: {:ok, nil}
  end

  defp get_property(resource, {"DAV:", "getcontentlength"}) do
    case resource.content_length do
      nil -> {:error, :not_found}
      len -> {:ok, to_string(len)}
    end
  end

  defp get_property(resource, {"DAV:", "getcontenttype"}) do
    case resource.content_type do
      nil -> {:error, :not_found}
      ct -> {:ok, ct}
    end
  end

  defp get_property(resource, {"DAV:", "getetag"}) do
    case resource.etag do
      nil -> {:error, :not_found}
      etag -> {:ok, ~s("#{etag}")}
    end
  end

  defp get_property(resource, {"DAV:", "getlastmodified"}) do
    case resource.last_modified do
      nil -> {:error, :not_found}
      dt -> {:ok, Calendar.strftime(dt, "%a, %d %b %Y %H:%M:%S GMT")}
    end
  end

  defp get_property(resource, {"DAV:", "creationdate"}) do
    case resource.creation_date do
      nil -> {:error, :not_found}
      dt -> {:ok, DateTime.to_iso8601(dt)}
    end
  end

  defp get_property(resource, {"DAV:", "displayname"}) do
    case resource.display_name do
      nil -> {:error, :not_found}
      name -> {:ok, name}
    end
  end

  defp get_property(resource, {namespace, name}) do
    case resource.backend_data do
      %{meta: %{metadata: metadata}} when is_map(metadata) ->
        case Map.fetch(metadata, "#{namespace}#{name}") do
          {:ok, value} -> {:ok, value}
          :error -> {:error, :not_found}
        end

      _ ->
        {:error, :not_found}
    end
  end

  defp validate_dest([_ | _]), do: :ok

  defp validate_dest([]) do
    {:error, %Davy.Error{code: :forbidden, message: "Cannot target root"}}
  end

  defp split_dest([volume_name | rest]) do
    {volume_name, "/" <> Enum.join(rest, "/")}
  end

  defp check_overwrite(volume_name, file_path, overwrite?) do
    if not overwrite? and resource_exists?(volume_name, file_path) do
      {:error, %Davy.Error{code: :precondition_failed}}
    else
      :ok
    end
  end

  defp resource_exists?(volume_name, file_path) do
    case call_core(:get_file_meta, [volume_name, file_path]) do
      {:ok, _} -> true
      _ -> false
    end
  end

  defp content_type_opts(%{content_type: ct}) when is_binary(ct), do: [content_type: ct]
  defp content_type_opts(_meta), do: []

  defp metadata_opts(%{metadata: md}) when is_map(md) and md != %{}, do: [metadata: md]
  defp metadata_opts(_meta), do: []

  defp internal_error do
    %Davy.Error{code: :bad_request, message: "Internal error"}
  end
end
