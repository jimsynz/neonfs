defmodule NeonFS.S3.MultipartStore do
  @moduledoc """
  ETS-backed store for tracking in-progress multipart uploads.

  Each upload is identified by a unique upload ID and tracks the bucket, key,
  content type, and uploaded parts. Each part records the ordered chunk refs
  its bytes were shipped to core as; `complete_multipart_upload` flattens
  the refs across parts in part-number order and issues a single
  `commit_chunks/4` RPC instead of reading/combining part files. Nothing on
  this path creates a per-part `FileIndex` entry.
  """

  use GenServer

  @table __MODULE__

  @type chunk_ref :: NeonFS.Client.ChunkWriter.chunk_ref()

  @type part_entry :: %{
          required(:etag) => String.t(),
          required(:size) => non_neg_integer(),
          required(:chunk_refs) => [chunk_ref()]
        }

  @type upload_entry :: %{
          bucket: String.t(),
          key: String.t(),
          content_type: String.t(),
          initiated: DateTime.t(),
          parts: %{pos_integer() => part_entry()}
        }

  @doc "Starts the multipart store process."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Creates a new multipart upload and returns the upload ID."
  @spec create(String.t(), String.t(), String.t()) :: String.t()
  def create(bucket, key, content_type \\ "application/octet-stream") do
    upload_id = generate_upload_id()

    entry = %{
      bucket: bucket,
      key: key,
      content_type: content_type,
      initiated: DateTime.utc_now(),
      parts: %{}
    }

    :ets.insert(@table, {upload_id, entry})
    upload_id
  end

  @doc "Retrieves an upload entry by ID."
  @spec get(String.t()) :: {:ok, upload_entry()} | {:error, :not_found}
  def get(upload_id) do
    case :ets.lookup(@table, upload_id) do
      [{^upload_id, entry}] -> {:ok, entry}
      [] -> {:error, :not_found}
    end
  end

  @doc "Records a completed part for a multipart upload."
  @spec put_part(String.t(), pos_integer(), part_entry()) :: :ok | {:error, :not_found}
  def put_part(upload_id, part_number, part) do
    case get(upload_id) do
      {:ok, entry} ->
        updated = %{entry | parts: Map.put(entry.parts, part_number, part)}
        :ets.insert(@table, {upload_id, updated})
        :ok

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @doc "Removes an upload from the store."
  @spec delete(String.t()) :: :ok
  def delete(upload_id) do
    :ets.delete(@table, upload_id)
    :ok
  end

  @doc "Lists all in-progress uploads for a bucket."
  @spec list_for_bucket(String.t()) :: [
          %{key: String.t(), upload_id: String.t(), initiated: DateTime.t()}
        ]
  def list_for_bucket(bucket) do
    :ets.tab2list(@table)
    |> Enum.filter(fn {_id, entry} -> entry.bucket == bucket end)
    |> Enum.map(fn {id, entry} ->
      %{key: entry.key, upload_id: id, initiated: entry.initiated}
    end)
    |> Enum.sort_by(& &1.key)
  end

  # GenServer callbacks

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:named_table, :public, :set])
    {:ok, %{table: table}}
  end

  # Private helpers

  defp generate_upload_id do
    Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
