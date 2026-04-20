defmodule WebdavServer.Test.StreamingBackend do
  @moduledoc """
  Test backend that implements `put_content_stream/4` so we can assert
  on the stream the handler hands the backend.

  Stores the received segments in an Agent so tests can inspect them
  individually instead of just the assembled binary.
  """
  @behaviour WebdavServer.Backend

  alias WebdavServer.{Error, Resource}

  @table __MODULE__
  @segments __MODULE__.Segments

  def start do
    if :ets.whereis(@table) != :undefined, do: :ets.delete(@table)
    :ets.new(@table, [:named_table, :set, :public])

    case Process.whereis(@segments) do
      nil -> Agent.start_link(fn -> %{} end, name: @segments)
      _pid -> Agent.update(@segments, fn _ -> %{} end)
    end

    now = DateTime.utc_now() |> DateTime.truncate(:second)
    root = %Resource{path: [], type: :collection, creation_date: now, last_modified: now}
    :ets.insert(@table, {[], root, nil})
    :ok
  end

  def reset, do: start()

  @doc "Returns the list of segments received for `path` (in order)."
  @spec segments_for([String.t()]) :: [binary()]
  def segments_for(path), do: Agent.get(@segments, &Map.get(&1, path, []))

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
  def get_content(_auth, resource, _opts) do
    case :ets.lookup(@table, resource.path) do
      [{_, _, content}] when content != nil -> {:ok, content}
      _ -> {:error, %Error{code: :not_found}}
    end
  end

  @impl true
  def put_content(_auth, _path, _body, _opts) do
    {:error, %Error{code: :method_not_allowed, message: "use streaming PUT"}}
  end

  @impl true
  def put_content_stream(_auth, path, stream, opts) do
    parent = Enum.slice(path, 0..-2//1)

    case :ets.lookup(@table, parent) do
      [{_, %{type: :collection}, _}] ->
        segments = Enum.to_list(stream)
        Agent.update(@segments, &Map.put(&1, path, segments))
        content = IO.iodata_to_binary(segments)
        now = DateTime.utc_now() |> DateTime.truncate(:second)
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
  def delete(_auth, _resource), do: :ok

  @impl true
  def copy(_auth, _resource, _dest, _overwrite?), do: {:ok, :created}

  @impl true
  def move(_auth, _resource, _dest, _overwrite?), do: {:ok, :created}

  @impl true
  def create_collection(_auth, _path), do: :ok

  @impl true
  def get_members(_auth, _resource), do: {:ok, []}
end
