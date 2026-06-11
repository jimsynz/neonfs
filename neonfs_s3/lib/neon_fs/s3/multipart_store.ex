defmodule NeonFS.S3.MultipartStore do
  @moduledoc """
  Cluster-shared bookkeeping for in-progress multipart uploads (#1177).

  Upload state lives in the Ra-backed cluster KV store (via
  `NeonFS.Client.KV`), not in node-local memory, so a load balancer can
  route `CreateMultipartUpload`, each `UploadPart`, and
  `CompleteMultipartUpload` to different S3 nodes and the upload still
  succeeds. Only bookkeeping is shared — part bytes stream to core as
  chunks while they arrive, and each part records the ordered chunk
  refs its bytes were shipped as. `complete_multipart_upload` flattens
  the refs across parts in part-number order and issues a single
  `commit_chunks/4` RPC. Nothing on this path creates a per-part
  `FileIndex` entry.

  ## Key layout

      s3_multipart:<upload_id>:meta      %{bucket, key, content_type, initiated}
      s3_multipart:<upload_id>:part:<n>  %{part_number, etag, size, chunk_refs}

  Each part is its own key, so concurrent `UploadPart` requests for
  different parts of the same upload — possibly on different S3 nodes —
  never read-modify-write shared state.
  """

  alias NeonFS.Client.KV

  @key_prefix "s3_multipart:"

  @type chunk_ref :: NeonFS.Client.ChunkWriter.chunk_ref()

  @type part_entry :: %{
          required(:etag) => String.t(),
          required(:size) => non_neg_integer(),
          required(:chunk_refs) => [chunk_ref()]
        }

  @type upload_meta :: %{
          bucket: String.t(),
          key: String.t(),
          content_type: String.t(),
          initiated: DateTime.t()
        }

  @type upload_entry :: %{
          bucket: String.t(),
          key: String.t(),
          content_type: String.t(),
          initiated: DateTime.t(),
          parts: %{pos_integer() => part_entry()}
        }

  @doc "Creates a new multipart upload and returns the upload ID."
  @spec create(String.t(), String.t(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def create(bucket, key, content_type \\ "application/octet-stream") do
    upload_id = generate_upload_id()

    meta = %{
      bucket: bucket,
      key: key,
      content_type: content_type,
      initiated: DateTime.utc_now()
    }

    case KV.put(meta_key(upload_id), meta) do
      :ok -> {:ok, upload_id}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Retrieves an upload's metadata (without parts) by ID."
  @spec get_meta(String.t()) :: {:ok, upload_meta()} | {:error, :not_found | term()}
  def get_meta(upload_id) do
    KV.get(meta_key(upload_id))
  end

  @doc "Retrieves an upload entry (metadata plus all recorded parts) by ID."
  @spec get(String.t()) :: {:ok, upload_entry()} | {:error, :not_found | term()}
  def get(upload_id) do
    with {:ok, meta} <- get_meta(upload_id) do
      parts =
        upload_id
        |> part_prefix()
        |> KV.list_prefix()
        |> Map.new(fn {_key, part} ->
          {part.part_number, Map.delete(part, :part_number)}
        end)

      {:ok, Map.put(meta, :parts, parts)}
    end
  end

  @doc "Records a completed part for a multipart upload."
  @spec put_part(String.t(), pos_integer(), part_entry()) ::
          :ok | {:error, :not_found | term()}
  def put_part(upload_id, part_number, part) do
    with {:ok, _meta} <- get_meta(upload_id) do
      KV.put(part_key(upload_id, part_number), Map.put(part, :part_number, part_number))
    end
  end

  @doc "Removes an upload's metadata and all recorded parts from the store."
  @spec delete(String.t()) :: :ok
  def delete(upload_id) do
    upload_id
    |> part_prefix()
    |> KV.list_prefix()
    |> Enum.each(fn {key, _part} -> KV.delete(key) end)

    KV.delete(meta_key(upload_id))
    :ok
  end

  @doc "Lists all in-progress uploads for a bucket."
  @spec list_for_bucket(String.t()) :: [
          %{key: String.t(), upload_id: String.t(), initiated: DateTime.t()}
        ]
  def list_for_bucket(bucket) do
    @key_prefix
    |> KV.list_prefix()
    |> Enum.flat_map(fn
      {@key_prefix <> rest, %{bucket: ^bucket} = meta} ->
        case String.split(rest, ":") do
          [upload_id, "meta"] ->
            [%{key: meta.key, upload_id: upload_id, initiated: meta.initiated}]

          _ ->
            []
        end

      _ ->
        []
    end)
    |> Enum.sort_by(& &1.key)
  end

  defp meta_key(upload_id), do: @key_prefix <> upload_id <> ":meta"

  defp part_prefix(upload_id), do: @key_prefix <> upload_id <> ":part:"

  defp part_key(upload_id, part_number),
    do: part_prefix(upload_id) <> Integer.to_string(part_number)

  defp generate_upload_id do
    Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
