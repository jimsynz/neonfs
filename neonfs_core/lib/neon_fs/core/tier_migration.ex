defmodule NeonFS.Core.TierMigration do
  @moduledoc """
  Tier migration using Reactor sagas for safe, compensatable data movement.

  Orchestrates moving a chunk from one drive/tier to another with proper
  locking, verification, and rollback. Steps:

  1. Acquire lock — prevents concurrent migrations of the same chunk
  2. Copy data — reads from source, writes to target
  3. Verify — confirms the hash matches on the target
  4. Update metadata — updates chunk location in ChunkIndex
  5. Cleanup — removes the chunk from the source drive
  6. Release lock — always runs, even on failure

  Uses an ETS lock table to prevent concurrent migrations of the same chunk.

  ## Usage

      TierMigration.run_migration(%{
        chunk_hash: hash,
        source_drive: "drive1",
        source_node: :"node1@host",
        source_tier: :hot,
        target_drive: "drive2",
        target_node: :"node1@host",
        target_tier: :warm
      })
  """

  require Logger

  alias NeonFS.Core.{BlobStore, ChunkIndex, ChunkMeta}
  alias NeonFS.Core.TierMigration.LockTable
  alias NeonFS.IO.{Operation, Scheduler}

  @type migration_params :: %{
          chunk_hash: binary(),
          source_drive: String.t(),
          source_node: node(),
          source_tier: atom(),
          target_drive: String.t(),
          target_node: node(),
          target_tier: atom()
        }

  @doc """
  Runs a tier migration for the given parameters.

  Returns `{:ok, result}` on success or `{:error, reason}` on failure.
  On failure, completed steps are automatically undone by the Reactor.
  """
  @spec run_migration(migration_params()) :: {:ok, term()} | {:error, term()}
  def run_migration(params) do
    with :ok <- LockTable.acquire_lock(params.chunk_hash) do
      result = execute_migration(params)
      LockTable.release_lock(params.chunk_hash)

      :telemetry.execute(
        [:neonfs, :tier_migration, telemetry_event(result)],
        %{},
        %{
          chunk_hash: params.chunk_hash,
          source_drive: params.source_drive,
          target_drive: params.target_drive,
          source_tier: params.source_tier,
          target_tier: params.target_tier
        }
      )

      result
    end
  end

  @doc """
  Returns a work function suitable for submitting to BackgroundWorker.

      BackgroundWorker.submit(
        TierMigration.work_fn(params),
        priority: :low,
        label: "migrate:abc123"
      )
  """
  @spec work_fn(migration_params()) :: (-> {:ok, term()} | {:error, term()})
  def work_fn(params) do
    fn -> run_migration(params) end
  end

  defp execute_migration(params) do
    local? = params.source_node == params.target_node

    :telemetry.execute(
      [:neonfs, :tier_migration, :start],
      %{},
      %{
        chunk_hash: params.chunk_hash,
        local: local?,
        source_tier: params.source_tier,
        target_tier: params.target_tier
      }
    )

    chunk_meta = resolve_chunk_meta(params)

    params =
      params
      |> Map.put_new_lazy(:volume_id, fn -> derive_volume_id(chunk_meta, params.chunk_hash) end)
      |> Map.put_new(:chunk_meta, chunk_meta)

    chunk_compression = if chunk_meta, do: chunk_meta.compression, else: :none

    with {:ok, data} <- copy_read(params, chunk_compression),
         {:ok, _hash, _info} <- copy_write(params, data, chunk_compression),
         :ok <- verify_copy(params, chunk_compression),
         :ok <- update_metadata(params),
         _ <- cleanup_source(params) do
      {:ok, :migrated}
    else
      {:error, reason} ->
        Logger.warning(
          "Migration failed (#{inspect(reason)}), rolling back",
          chunk_hash: inspect(params.chunk_hash),
          reason: inspect(reason)
        )

        rollback_copy(params)
        {:error, reason}
    end
  end

  defp copy_read(params, compression) do
    blob_store = NeonFS.Core.BlobStore
    tier_str = Atom.to_string(params.source_tier)
    volume_id = Map.get(params, :volume_id, "_migration")
    read_opts = [verify: true, decompress: compression != :none, compression: compression]

    if params.source_node == Node.self() do
      op =
        Operation.new(
          priority: :repair,
          volume_id: volume_id,
          drive_id: params.source_drive,
          type: :read,
          callback: fn ->
            blob_store.read_chunk_with_options(
              params.chunk_hash,
              params.source_drive,
              tier_str,
              read_opts
            )
          end
        )

      Scheduler.submit_sync(op)
    else
      :rpc.call(
        params.source_node,
        blob_store,
        :read_chunk_with_options,
        [
          params.chunk_hash,
          params.source_drive,
          tier_str,
          read_opts
        ],
        10_000
      )
      |> handle_rpc_result()
    end
  end

  defp copy_write(params, data, compression) do
    blob_store = NeonFS.Core.BlobStore
    tier_str = Atom.to_string(params.target_tier)
    volume_id = Map.get(params, :volume_id, "_migration")
    write_opts = compression_write_opts(compression)

    if params.target_node == Node.self() do
      op =
        Operation.new(
          priority: :repair,
          volume_id: volume_id,
          drive_id: params.target_drive,
          type: :write,
          callback: fn ->
            blob_store.write_chunk(data, params.target_drive, tier_str, write_opts)
          end
        )

      Scheduler.submit_sync(op)
    else
      :rpc.call(
        params.target_node,
        blob_store,
        :write_chunk,
        [data, params.target_drive, tier_str, write_opts],
        10_000
      )
      |> handle_rpc_result()
    end
  end

  defp verify_copy(params, compression) do
    blob_store = NeonFS.Core.BlobStore
    tier_str = Atom.to_string(params.target_tier)
    volume_id = Map.get(params, :volume_id, "_migration")
    read_opts = [verify: true, decompress: compression != :none, compression: compression]

    result =
      if params.target_node == Node.self() do
        op =
          Operation.new(
            priority: :repair,
            volume_id: volume_id,
            drive_id: params.target_drive,
            type: :read,
            callback: fn ->
              blob_store.read_chunk_with_options(
                params.chunk_hash,
                params.target_drive,
                tier_str,
                read_opts
              )
            end
          )

        Scheduler.submit_sync(op)
      else
        :rpc.call(
          params.target_node,
          blob_store,
          :read_chunk_with_options,
          [
            params.chunk_hash,
            params.target_drive,
            tier_str,
            read_opts
          ],
          10_000
        )
        |> handle_rpc_result()
      end

    case result do
      {:ok, _data} -> :ok
      {:error, reason} -> {:error, {:verification_failed, reason}}
    end
  end

  defp update_metadata(params) do
    chunk_index = NeonFS.Core.ChunkIndex

    case Map.get(params, :chunk_meta) || lookup_chunk_meta(params) do
      %ChunkMeta{} = chunk_meta ->
        new_location = %{
          node: params.target_node,
          drive_id: params.target_drive,
          tier: params.target_tier
        }

        old_location = %{
          node: params.source_node,
          drive_id: params.source_drive,
          tier: params.source_tier
        }

        updated_locations =
          chunk_meta.locations
          |> Enum.reject(fn loc ->
            loc.node == old_location.node and
              loc.drive_id == old_location.drive_id and
              loc.tier == old_location.tier
          end)
          |> then(fn locs -> [new_location | locs] end)

        chunk_index.update_locations(params.chunk_hash, updated_locations)

      nil ->
        {:error, :chunk_not_found}
    end
  end

  defp cleanup_source(params) do
    blob_store = NeonFS.Core.BlobStore

    delete_opts =
      cleanup_delete_opts(Map.get(params, :volume_id, "_migration"), params.chunk_hash)

    result =
      if params.source_node == Node.self() do
        blob_store.delete_chunk(params.chunk_hash, params.source_drive, delete_opts)
      else
        :rpc.call(
          params.source_node,
          blob_store,
          :delete_chunk,
          [params.chunk_hash, params.source_drive, delete_opts],
          10_000
        )
        |> handle_rpc_result()
      end

    case result do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.warning("Cleanup of source chunk failed (orphaned)", reason: inspect(reason))

        :ok
    end
  end

  defp rollback_copy(params) do
    blob_store = NeonFS.Core.BlobStore

    delete_opts =
      cleanup_delete_opts(Map.get(params, :volume_id, "_migration"), params.chunk_hash)

    result =
      if params.target_node == Node.self() do
        blob_store.delete_chunk(params.chunk_hash, params.target_drive, delete_opts)
      else
        :rpc.call(
          params.target_node,
          blob_store,
          :delete_chunk,
          [params.chunk_hash, params.target_drive, delete_opts],
          10_000
        )
        |> handle_rpc_result()
      end

    case result do
      {:ok, _} -> :ok
      {:error, _} -> :ok
    end
  end

  defp cleanup_delete_opts(volume_id, chunk_hash) do
    case ChunkIndex.lookup_by_hash(chunk_hash) do
      {:ok, chunk_meta} ->
        BlobStore.codec_opts_for_chunk(chunk_meta)

      :not_found ->
        case ChunkIndex.get(volume_id, chunk_hash) do
          {:ok, chunk_meta} -> BlobStore.codec_opts_for_chunk(chunk_meta)
          _ -> []
        end
    end
  end

  # Prefer caller-threaded `:chunk_meta` (drive evacuation passes the
  # ChunkMeta it already read from local ETS). Fall back to local ETS,
  # then to the cluster-wide MetadataReader. The local-ETS path matters
  # mid-evacuation: the volume's tree pages may have been migrated off
  # the evacuating drive before `drive_locations` is rewritten, which
  # makes `ChunkIndex.get/2` (via MetadataReader) fail to read the
  # chunk_meta back even though the chunk is locally present.
  defp resolve_chunk_meta(params) do
    case Map.get(params, :chunk_meta) do
      %ChunkMeta{} = meta -> meta
      _ -> lookup_chunk_meta(params)
    end
  end

  defp lookup_chunk_meta(params) do
    case ChunkIndex.lookup_by_hash(params.chunk_hash) do
      {:ok, %ChunkMeta{} = meta} ->
        meta

      :not_found ->
        volume_id = Map.get(params, :volume_id) || derive_volume_id(nil, params.chunk_hash)

        case ChunkIndex.get(volume_id, params.chunk_hash) do
          {:ok, %ChunkMeta{} = meta} -> meta
          _ -> nil
        end
    end
  end

  defp derive_volume_id(%ChunkMeta{} = meta, _hash) do
    ChunkMeta.any_volume_id(meta) || "_migration"
  end

  defp derive_volume_id(nil, chunk_hash) do
    case :ets.lookup(:chunk_index, chunk_hash) do
      [{^chunk_hash, %ChunkMeta{} = meta}] ->
        ChunkMeta.any_volume_id(meta) || "_migration"

      _ ->
        "_migration"
    end
  rescue
    ArgumentError -> "_migration"
  end

  defp compression_write_opts(:none), do: []
  defp compression_write_opts(:zstd), do: [compression: "zstd", compression_level: 3]

  defp handle_rpc_result({:badrpc, reason}), do: {:error, {:rpc_error, reason}}
  defp handle_rpc_result(result), do: result

  defp telemetry_event({:ok, _}), do: :success
  defp telemetry_event({:error, _}), do: :failure
end
