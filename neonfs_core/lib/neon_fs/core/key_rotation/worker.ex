defmodule NeonFS.Core.KeyRotation.Worker do
  @moduledoc """
  Batch re-encryption worker for volume key rotation.

  Processes encrypted chunks in configurable batches, calling the
  `store_reencrypt_chunk` NIF for each chunk on each local replica.
  The NIF handles the full decrypt-old → encrypt-new cycle in Rust
  without chunk data crossing the NIF boundary.

  After each chunk is re-encrypted, the ChunkMeta crypto field is updated
  with the new nonce and key version. Progress is persisted in Ra after
  each batch so rotation can resume after a node restart.
  """

  require Logger

  alias NeonFS.Core.{BlobStore, ChunkIndex, ChunkMeta, KeyManager, RaSupervisor}
  alias NeonFS.Core.ChunkCrypto

  @default_batch_size 1000
  @default_rate_limit 1000

  @doc """
  Runs the rotation worker for a volume.

  Re-encrypts all chunks that are still on the old key version, in batches.
  Updates progress in Ra after each batch. On completion, clears the rotation
  state and marks the old key as deprecated with a timestamp.
  """
  @spec run(binary(), pos_integer(), pos_integer()) :: :ok | {:error, term()}
  def run(volume_id, old_version, new_version) do
    batch_size = Application.get_env(:neonfs_core, :rotation_batch_size, @default_batch_size)
    rate_limit = Application.get_env(:neonfs_core, :rotation_rate_limit, @default_rate_limit)
    delay_per_chunk = if rate_limit > 0, do: div(1000, rate_limit), else: 0

    with {:ok, old_key} <- KeyManager.get_volume_key(volume_id, old_version),
         {:ok, new_key} <- KeyManager.get_volume_key(volume_id, new_version) do
      chunks = chunks_needing_rotation(volume_id, old_version)
      total = length(chunks)

      Logger.info(
        "Key rotation starting for volume #{volume_id}: " <>
          "#{total} chunks, v#{old_version} → v#{new_version}"
      )

      result =
        chunks
        |> Enum.chunk_every(batch_size)
        |> Enum.reduce_while({0, :ok}, fn batch, {migrated, :ok} ->
          case process_batch(batch, volume_id, old_key, new_key, new_version, delay_per_chunk) do
            {:ok, batch_count} ->
              new_migrated = migrated + batch_count
              update_progress(volume_id, total, new_migrated)

              :telemetry.execute(
                [:neonfs, :rotation, :progress],
                %{migrated: new_migrated, total: total},
                %{volume_id: volume_id}
              )

              {:cont, {new_migrated, :ok}}

            {:error, reason} ->
              Logger.error(
                "Key rotation failed for volume #{volume_id} " <>
                  "at chunk #{migrated}: #{inspect(reason)}"
              )

              {:halt, {migrated, {:error, reason}}}
          end
        end)

      case result do
        {_migrated, :ok} ->
          complete_rotation(volume_id, old_version)

        {_migrated, {:error, reason}} ->
          :telemetry.execute(
            [:neonfs, :rotation, :failed],
            %{},
            %{volume_id: volume_id, reason: reason}
          )

          {:error, reason}
      end
    end
  end

  # Private

  defp chunks_needing_rotation(volume_id, old_version) do
    ChunkIndex.get_chunks_for_volume(volume_id)
    |> Enum.filter(fn chunk ->
      chunk.crypto != nil and chunk.crypto.key_version == old_version
    end)
  end

  defp process_batch(chunks, volume_id, old_key, new_key, new_version, delay_per_chunk) do
    results =
      Enum.map(chunks, fn chunk ->
        result = reencrypt_chunk(chunk, volume_id, old_key, new_key, new_version)

        if delay_per_chunk > 0 do
          Process.sleep(delay_per_chunk)
        end

        result
      end)

    case Enum.find(results, &match?({:error, _}, &1)) do
      nil -> {:ok, length(chunks)}
      error -> error
    end
  end

  defp reencrypt_chunk(%ChunkMeta{} = chunk, _volume_id, old_key, new_key, new_version) do
    old_nonce = chunk.crypto.nonce
    new_nonce = :crypto.strong_rand_bytes(12)

    local_locations =
      Enum.filter(chunk.locations, fn loc -> loc.node == Node.self() end)

    reencrypt_results =
      Enum.map(local_locations, fn loc ->
        tier = Atom.to_string(loc.tier)

        BlobStore.reencrypt_chunk(
          chunk.hash,
          loc.drive_id,
          tier,
          old_key,
          old_nonce,
          new_key,
          new_nonce
        )
      end)

    case Enum.find(reencrypt_results, &match?({:error, _}, &1)) do
      nil ->
        updated_crypto = ChunkCrypto.new(nonce: new_nonce, key_version: new_version)
        updated_stored_size = extract_stored_size(reencrypt_results)
        updated_meta = %{chunk | crypto: updated_crypto, stored_size: updated_stored_size}
        ChunkIndex.put(updated_meta)

      error ->
        error
    end
  end

  defp extract_stored_size(results) do
    case results do
      [{:ok, stored_size} | _] -> stored_size
      _ -> 0
    end
  end

  defp update_progress(volume_id, total, migrated) do
    case query_current_rotation(volume_id) do
      {:ok, existing_rotation} when is_map(existing_rotation) ->
        updated = %{existing_rotation | progress: %{total_chunks: total, migrated: migrated}}
        RaSupervisor.command({:set_rotation_state, volume_id, updated})

      _ ->
        :ok
    end
  end

  defp query_current_rotation(volume_id) do
    RaSupervisor.query(fn state ->
      state.volumes
      |> Map.get(volume_id)
      |> case do
        nil -> nil
        vol_data -> get_in(vol_data, [:encryption, :rotation])
      end
    end)
  end

  defp complete_rotation(volume_id, _old_version) do
    # Clear rotation state
    RaSupervisor.command({:set_rotation_state, volume_id, nil})

    :telemetry.execute(
      [:neonfs, :rotation, :completed],
      %{},
      %{volume_id: volume_id}
    )

    Logger.info("Key rotation completed for volume #{volume_id}")
    :ok
  end
end
