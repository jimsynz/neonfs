defmodule NeonFS.Core.SyncOperation do
  @moduledoc """
  The `fsync`/`sync`/COMMIT durability barrier (#1500).

  Blocks until every chunk of a file has at least the volume's
  `min_copies` durable replicas, driving synchronous replication for any
  chunk that is short (via `Replication.ensure_min_copies/2`). This is the
  core mechanism the interface layer wires its sync operations to (#1502).

  On a `write_ack: :local` volume the extra replicas are placed by a
  fire-and-forget background task after the write acks, so a read — or a
  whole-cluster restart — immediately after the write can miss chunks that
  never made it off the primary. The barrier forces those placements to
  complete before it returns.

  Erasure-coded volumes are a no-op: their shards are written synchronously
  to their target drives on the write path, so there is no background
  replication to wait on.

  ## Telemetry

    * `[:neonfs, :sync_file, :start]` — Measurements: `chunk_count`.
      Metadata: `volume_id`, `file_id`.
    * `[:neonfs, :sync_file, :stop]` — Measurements: `duration`.
      Metadata: `volume_id`, `file_id`, `status` (`:ok` | `:error`).
    * `[:neonfs, :sync_file, :under_replicated]` — Metadata: `chunk_hash`,
      `volume_id`, `file_id`, `reason`.
  """

  alias NeonFS.Core.{FileIndex, Replication, VolumeRegistry}

  @doc """
  Blocks until every chunk of `path` on `volume_id` has at least the
  volume's `min_copies` durable replicas. See the module doc for the
  barrier semantics.
  """
  @spec sync_file(binary(), String.t()) :: :ok | {:error, term()}
  def sync_file(volume_id, path) do
    with {:ok, volume} <- get_volume(volume_id),
         {:ok, file_meta} <- get_file_by_path(volume_id, path) do
      sync_chunks(file_meta, volume)
    end
  end

  @doc """
  `file_id`-keyed counterpart to `sync_file/2` — resolves the file by id so
  a `:detached` file (unlinked while open) is still syncable.
  """
  @spec sync_file_by_id(binary(), binary()) :: :ok | {:error, term()}
  def sync_file_by_id(volume_id, file_id) do
    with {:ok, volume} <- get_volume(volume_id),
         {:ok, file_meta} <- get_file_by_id(volume_id, file_id) do
      sync_chunks(file_meta, volume)
    end
  end

  defp sync_chunks(_file_meta, %{durability: %{type: :erasure}}), do: :ok

  defp sync_chunks(file_meta, volume) do
    metadata = %{volume_id: volume.id, file_id: file_meta.id}
    start = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :sync_file, :start],
      %{chunk_count: length(file_meta.chunks)},
      metadata
    )

    result = ensure_chunks_durable(file_meta, volume)

    :telemetry.execute(
      [:neonfs, :sync_file, :stop],
      %{duration: System.monotonic_time() - start},
      Map.put(metadata, :status, status(result))
    )

    result
  end

  defp ensure_chunks_durable(file_meta, volume) do
    Enum.reduce_while(file_meta.chunks, :ok, fn hash, :ok ->
      case Replication.ensure_min_copies(hash, volume) do
        :ok ->
          {:cont, :ok}

        {:error, reason} = error ->
          emit_under_replicated(hash, volume.id, file_meta.id, reason)
          {:halt, error}
      end
    end)
  end

  defp emit_under_replicated(hash, volume_id, file_id, reason) do
    :telemetry.execute(
      [:neonfs, :sync_file, :under_replicated],
      %{},
      %{chunk_hash: hash, volume_id: volume_id, file_id: file_id, reason: reason}
    )
  end

  defp status(:ok), do: :ok
  defp status({:error, _}), do: :error

  defp get_volume(volume_id) do
    case VolumeRegistry.get(volume_id) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :volume_not_found}
    end
  end

  defp get_file_by_path(volume_id, path) do
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, meta} -> {:ok, meta}
      {:error, :not_found} -> {:error, :file_not_found}
    end
  end

  defp get_file_by_id(volume_id, file_id) do
    case FileIndex.get(volume_id, file_id) do
      {:ok, meta} -> {:ok, meta}
      {:error, :not_found} -> {:error, :file_not_found}
    end
  end
end
