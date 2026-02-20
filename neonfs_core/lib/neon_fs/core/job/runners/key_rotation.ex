defmodule NeonFS.Core.Job.Runners.KeyRotation do
  @moduledoc """
  Job runner for volume key rotation.

  Processes encrypted chunks in batches, calling the `store_reencrypt_chunk`
  NIF for each chunk on each local replica. Each `step/1` call processes
  one batch of chunks.

  The `chunks_needing_rotation/2` query naturally skips already-rotated
  chunks (they have the new key version), making resume after restart
  idempotent.
  """

  @behaviour NeonFS.Core.Job.Runner

  require Logger

  alias NeonFS.Core.{BlobStore, ChunkIndex, ChunkMeta, KeyManager}
  alias NeonFS.Core.ChunkCrypto

  @default_batch_size 1000

  @impl NeonFS.Core.Job.Runner
  def label, do: "key-rotation"

  @impl NeonFS.Core.Job.Runner
  def step(job) do
    volume_id = job.params.volume_id
    old_version = job.params.from_version
    new_version = job.params.to_version

    with {:ok, old_key} <- KeyManager.get_volume_key(volume_id, old_version),
         {:ok, new_key} <- KeyManager.get_volume_key(volume_id, new_version) do
      remaining = chunks_needing_rotation(volume_id, old_version)
      rotate_remaining(job, remaining, old_key, new_key, new_version)
    else
      {:error, reason} -> {:error, reason, job}
    end
  end

  # Private

  defp rotate_remaining(job, [], _old_key, _new_key, _new_version) do
    complete_rotation(job.params.volume_id)
    {:complete, %{job | progress: %{job.progress | description: "Complete"}}}
  end

  defp rotate_remaining(job, remaining, old_key, new_key, new_version) do
    batch = Enum.take(remaining, batch_size())

    case process_batch(batch, old_key, new_key, new_version) do
      {:ok, count} ->
        completed = job.progress.completed + count
        total = max(job.progress.total, completed + length(remaining) - count)

        updated = %{
          job
          | progress: %{
              total: total,
              completed: completed,
              description: "Re-encrypting chunks"
            },
            state: Map.put(job.state, :last_batch_at, DateTime.utc_now())
        }

        :telemetry.execute(
          [:neonfs, :rotation, :progress],
          %{migrated: completed, total: total},
          %{volume_id: job.params.volume_id}
        )

        {:continue, updated}

      {:error, reason} ->
        {:error, reason, job}
    end
  end

  defp chunks_needing_rotation(volume_id, old_version) do
    ChunkIndex.get_chunks_for_volume(volume_id)
    |> Enum.filter(fn chunk ->
      chunk.crypto != nil and chunk.crypto.key_version == old_version
    end)
  end

  defp process_batch(chunks, old_key, new_key, new_version) do
    results =
      Enum.map(chunks, fn chunk ->
        reencrypt_chunk(chunk, old_key, new_key, new_version)
      end)

    case Enum.find(results, &match?({:error, _}, &1)) do
      nil -> {:ok, length(chunks)}
      error -> error
    end
  end

  defp reencrypt_chunk(%ChunkMeta{} = chunk, old_key, new_key, new_version) do
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

  defp complete_rotation(volume_id) do
    :telemetry.execute(
      [:neonfs, :rotation, :completed],
      %{},
      %{volume_id: volume_id}
    )

    Logger.info("Key rotation completed for volume #{volume_id}")
  end

  defp batch_size do
    Application.get_env(:neonfs_core, :rotation_batch_size, @default_batch_size)
  end
end
