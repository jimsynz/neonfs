defmodule NeonFS.WebDAV.Backend do
  @moduledoc """
  WebdavServer.Backend implementation that maps WebDAV operations to NeonFS
  core calls.

  The root collection lists NeonFS volumes. The first path segment identifies
  the volume, and remaining segments map to file paths within the volume.
  All communication with core nodes goes through `NeonFS.Client.Router`.
  """

  @behaviour WebdavServer.Backend

  alias NeonFS.Client.Router

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
        {:error, %WebdavServer.Error{code: :not_found}}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  def resolve(_auth, [volume_name | rest]) do
    file_path = "/" <> Enum.join(rest, "/")

    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, meta} <- call_core(:get_file_meta, [volume_name, file_path]) do
      {:ok, file_resource(volume_name, volume.id, rest, meta)}
    else
      {:error, :not_found} ->
        {:error, %WebdavServer.Error{code: :not_found}}

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
  def set_properties(_auth, _resource, _operations) do
    {:error,
     %WebdavServer.Error{code: :forbidden, message: "Property modification not supported"}}
  end

  # --- File operations ---

  @impl true
  def get_content(_auth, resource, _opts) do
    %{volume_name: volume_name, file_path: file_path} = resource.backend_data

    case call_core(:read_file, [volume_name, file_path]) do
      {:ok, content} ->
        {:ok, content}

      {:error, :not_found} ->
        {:error, %WebdavServer.Error{code: :not_found}}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  @impl true
  def put_content(_auth, [volume_name | rest], body, _opts) do
    file_path = "/" <> Enum.join(rest, "/")
    body_binary = IO.iodata_to_binary(body)

    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, meta} <- call_core(:write_file, [volume_name, file_path, body_binary]) do
      {:ok, file_resource(volume_name, volume.id, rest, meta)}
    else
      {:error, :not_found} ->
        {:error, %WebdavServer.Error{code: :conflict, message: "Volume not found"}}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  def put_content(_auth, [], _body, _opts) do
    {:error, %WebdavServer.Error{code: :forbidden, message: "Cannot write to root"}}
  end

  @impl true
  def delete(_auth, %{backend_data: %{type: :root}}) do
    {:error, %WebdavServer.Error{code: :forbidden, message: "Cannot delete root"}}
  end

  def delete(_auth, %{backend_data: %{type: :volume}}) do
    {:error, %WebdavServer.Error{code: :forbidden, message: "Cannot delete volumes via WebDAV"}}
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
    with :ok <- validate_dest(dest_path),
         {dest_volume, dest_file_path} = split_dest(dest_path),
         :ok <- check_overwrite(dest_volume, dest_file_path, overwrite?),
         %{volume_name: src_volume, file_path: src_path} = resource.backend_data,
         {:ok, content} <- call_core(:read_file, [src_volume, src_path]),
         existed? = resource_exists?(dest_volume, dest_file_path),
         {:ok, _meta} <- call_core(:write_file, [dest_volume, dest_file_path, content]) do
      if existed?, do: {:ok, :no_content}, else: {:ok, :created}
    else
      {:error, %WebdavServer.Error{}} = err -> err
      {:error, :not_found} -> {:error, %WebdavServer.Error{code: :not_found}}
      {:error, _reason} -> {:error, internal_error()}
    end
  end

  @impl true
  def move(_auth, resource, dest_path, overwrite?) do
    with :ok <- validate_dest(dest_path),
         {dest_volume, dest_file_path} = split_dest(dest_path),
         :ok <- check_overwrite(dest_volume, dest_file_path, overwrite?) do
      %{volume_name: src_volume, file_path: src_path} = resource.backend_data
      existed? = resource_exists?(dest_volume, dest_file_path)

      if src_volume == dest_volume do
        move_same_volume(src_volume, src_path, dest_file_path, existed?)
      else
        move_cross_volume(src_volume, src_path, dest_volume, dest_file_path, existed?)
      end
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
          {:error, :already_exists} -> {:error, %WebdavServer.Error{code: :method_not_allowed}}
          {:error, _reason} -> {:error, internal_error()}
        end

      {:error, :not_found} ->
        {:error, %WebdavServer.Error{code: :conflict, message: "Volume not found"}}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  def create_collection(_auth, []) do
    {:error, %WebdavServer.Error{code: :forbidden, message: "Cannot create root collection"}}
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
    list_dir_members(volume_name, resource.backend_data.volume_id, "/")
  end

  def get_members(_auth, resource) do
    %{volume_name: volume_name, volume_id: volume_id, file_path: file_path} =
      resource.backend_data

    list_dir_members(volume_name, volume_id, file_path)
  end

  # --- Private helpers ---

  defp move_same_volume(volume, src_path, dest_path, existed?) do
    case call_core(:rename_file, [volume, src_path, dest_path]) do
      :ok ->
        if existed?, do: {:ok, :no_content}, else: {:ok, :created}

      {:error, :not_found} ->
        {:error, %WebdavServer.Error{code: :not_found}}

      {:error, _reason} ->
        {:error, internal_error()}
    end
  end

  defp move_cross_volume(src_volume, src_path, dest_volume, dest_path, existed?) do
    with {:ok, content} <- call_core(:read_file, [src_volume, src_path]),
         {:ok, _meta} <- call_core(:write_file, [dest_volume, dest_path, content]),
         _ <- call_core(:delete_file, [src_volume, src_path]) do
      if existed?, do: {:ok, :no_content}, else: {:ok, :created}
    else
      {:error, :not_found} -> {:error, %WebdavServer.Error{code: :not_found}}
      {:error, _reason} -> {:error, internal_error()}
    end
  end

  defp call_core(function, args) do
    case Application.get_env(:neonfs_webdav, :core_call_fn) do
      nil -> Router.call(NeonFS.Core, function, args)
      fun when is_function(fun, 2) -> fun.(function, args)
    end
  end

  defp root_resource do
    %WebdavServer.Resource{
      path: [],
      type: :collection,
      display_name: "NeonFS",
      last_modified: nil,
      creation_date: nil,
      backend_data: %{type: :root}
    }
  end

  defp volume_resource(volume) do
    %WebdavServer.Resource{
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

    %WebdavServer.Resource{
      path: [volume_name | path_segments],
      type: type,
      etag: compute_etag(meta),
      content_type: if(type == :file, do: "application/octet-stream"),
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

  defp list_dir_members(volume_name, volume_id, dir_path) do
    case call_core(:list_files, [volume_name, dir_path]) do
      {:ok, entries} ->
        resources =
          Enum.map(entries, fn meta ->
            segments = path_to_segments(meta.path)
            file_resource(volume_name, volume_id, segments, meta)
          end)

        {:ok, resources}

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

  defp get_property(_resource, _prop) do
    {:error, :not_found}
  end

  defp validate_dest([_ | _]), do: :ok

  defp validate_dest([]) do
    {:error, %WebdavServer.Error{code: :forbidden, message: "Cannot target root"}}
  end

  defp split_dest([volume_name | rest]) do
    {volume_name, "/" <> Enum.join(rest, "/")}
  end

  defp check_overwrite(volume_name, file_path, overwrite?) do
    if not overwrite? and resource_exists?(volume_name, file_path) do
      {:error, %WebdavServer.Error{code: :precondition_failed}}
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

  defp internal_error do
    %WebdavServer.Error{code: :bad_request, message: "Internal error"}
  end
end
