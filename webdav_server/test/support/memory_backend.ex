defmodule WebdavServer.Test.MemoryBackend do
  @moduledoc false
  @behaviour WebdavServer.Backend

  alias WebdavServer.{Error, Resource}

  @table __MODULE__

  def start do
    if :ets.whereis(@table) != :undefined do
      :ets.delete(@table)
    end

    :ets.new(@table, [:named_table, :set, :public])

    # Create root collection
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    root = %Resource{
      path: [],
      type: :collection,
      creation_date: now,
      last_modified: now
    }

    :ets.insert(@table, {[], root, nil})
    :ok
  end

  def reset, do: start()

  @impl true
  def authenticate(_conn), do: {:ok, %{user: "test"}}

  @impl true
  def resolve(_auth, path) do
    case :ets.lookup(@table, path) do
      [{^path, resource, _content}] -> {:ok, resource}
      [] -> {:error, %Error{code: :not_found}}
    end
  end

  @impl true
  def get_properties(_auth, _resource, properties) do
    Enum.map(properties, fn prop -> {prop, {:error, :not_found}} end)
  end

  @impl true
  def set_properties(_auth, _resource, _operations), do: :ok

  @impl true
  def get_content(_auth, resource, opts) do
    case :ets.lookup(@table, resource.path) do
      [{_, _, content}] when content != nil -> {:ok, apply_range(content, opts)}
      _ -> {:error, %Error{code: :not_found}}
    end
  end

  defp apply_range(content, %{range: {start_byte, nil}}) do
    size = byte_size(content)
    if start_byte >= size, do: <<>>, else: binary_part(content, start_byte, size - start_byte)
  end

  defp apply_range(content, %{range: {start_byte, end_byte}}) do
    clamped_end = min(end_byte, byte_size(content) - 1)
    binary_part(content, start_byte, clamped_end - start_byte + 1)
  end

  defp apply_range(content, _), do: content

  @impl true
  def put_content(_auth, path, body, opts) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    parent = Enum.slice(path, 0..-2//1)

    case :ets.lookup(@table, parent) do
      [{_, %{type: :collection}, _}] ->
        content = IO.iodata_to_binary(body)
        etag = "\"" <> Base.encode16(:crypto.hash(:md5, content), case: :lower) <> "\""

        resource = %Resource{
          path: path,
          type: :file,
          etag: etag,
          content_type: Map.get(opts, :content_type, "application/octet-stream"),
          content_length: byte_size(content),
          last_modified: now,
          creation_date: now,
          display_name: List.last(path)
        }

        :ets.insert(@table, {path, resource, content})
        {:ok, resource}

      [] ->
        {:error, %Error{code: :conflict, message: "Parent collection does not exist"}}
    end
  end

  @impl true
  def delete(_auth, resource) do
    if resource.type == :collection do
      delete_recursive(resource.path)
    else
      :ets.delete(@table, resource.path)
    end

    :ok
  end

  @impl true
  def copy(auth, resource, dest_path, overwrite?) do
    dest_exists? = :ets.lookup(@table, dest_path) != []

    if dest_exists? and not overwrite? do
      {:error, %Error{code: :precondition_failed}}
    else
      status = if dest_exists?, do: :no_content, else: :created
      do_copy(auth, resource, dest_path)
      {:ok, status}
    end
  end

  @impl true
  def move(auth, resource, dest_path, overwrite?) do
    case copy(auth, resource, dest_path, overwrite?) do
      {:ok, status} ->
        delete(auth, resource)
        {:ok, status}

      error ->
        error
    end
  end

  @impl true
  def create_collection(_auth, path) do
    parent = Enum.slice(path, 0..-2//1)

    with {:parent, [{_, %{type: :collection}, _}]} <-
           {:parent, :ets.lookup(@table, parent)},
         {:exists, []} <- {:exists, :ets.lookup(@table, path)} do
      now = DateTime.utc_now() |> DateTime.truncate(:second)

      resource = %Resource{
        path: path,
        type: :collection,
        creation_date: now,
        last_modified: now,
        display_name: List.last(path)
      }

      :ets.insert(@table, {path, resource, nil})
      :ok
    else
      {:parent, []} ->
        {:error, %Error{code: :conflict, message: "Parent collection does not exist"}}

      {:exists, [_]} ->
        {:error, %Error{code: :method_not_allowed, message: "Resource already exists"}}
    end
  end

  @impl true
  def get_members(_auth, resource) do
    prefix = resource.path
    prefix_len = length(prefix)

    members =
      :ets.tab2list(@table)
      |> Enum.filter(fn {path, _res, _content} ->
        path != prefix and
          List.starts_with?(path, prefix) and
          length(path) == prefix_len + 1
      end)
      |> Enum.map(fn {_path, res, _content} -> res end)
      |> Enum.sort_by(& &1.display_name)

    {:ok, members}
  end

  defp delete_recursive(path) do
    :ets.tab2list(@table)
    |> Enum.filter(fn {p, _, _} -> List.starts_with?(p, path) end)
    |> Enum.each(fn {p, _, _} -> :ets.delete(@table, p) end)
  end

  defp do_copy(_auth, resource, dest_path) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    case :ets.lookup(@table, resource.path) do
      [{_, %Resource{} = res, content}] ->
        new_resource = %Resource{
          res
          | path: dest_path,
            last_modified: now,
            display_name: List.last(dest_path)
        }

        :ets.insert(@table, {dest_path, new_resource, content})

        if resource.type == :collection do
          copy_children(resource.path, dest_path, now)
        end

      [] ->
        :ok
    end
  end

  defp copy_children(source_prefix, dest_prefix, now) do
    source_len = length(source_prefix)

    :ets.tab2list(@table)
    |> Enum.filter(fn {path, _, _} ->
      List.starts_with?(path, source_prefix) and path != source_prefix
    end)
    |> Enum.each(fn {path, %Resource{} = res, content} ->
      relative = Enum.drop(path, source_len)
      new_path = dest_prefix ++ relative

      new_resource = %Resource{
        res
        | path: new_path,
          last_modified: now,
          display_name: List.last(new_path)
      }

      :ets.insert(@table, {new_path, new_resource, content})
    end)
  end
end
