defmodule NeonFS.Core.KeyRotation do
  @moduledoc """
  Volume key rotation orchestration.

  Coordinates the lifecycle of a key rotation: generates a new key version,
  sets rotation state on the volume, and submits a background worker to
  re-encrypt all chunks from the old key to the new key.

  Rotation state is persisted in Ra (via `:set_rotation_state` command) so
  it survives node restarts. The `KeyRotation.Worker` module handles the
  actual batch re-encryption.

  Rotation state is read directly from Ra (not VolumeRegistry ETS cache)
  because the `:set_rotation_state` command updates the Ra volume map
  but VolumeRegistry does not sync the rotation field back to its cache.
  """

  alias NeonFS.Core.{
    AuditLog,
    BackgroundWorker,
    ChunkIndex,
    KeyManager,
    RaSupervisor,
    VolumeRegistry
  }

  alias NeonFS.Core.KeyRotation.Worker
  alias NeonFS.Core.VolumeEncryption

  @doc """
  Starts key rotation for a volume.

  Generates a new key version, sets the rotation state on the volume, and
  submits a background worker to re-encrypt all chunks.

  Returns `{:ok, rotation_info}` on success, or `{:error, reason}` if the
  volume is not encrypted or a rotation is already in progress.
  """
  @spec start_rotation(binary()) :: {:ok, map()} | {:error, term()}
  def start_rotation(volume_id) do
    with {:ok, volume} <- VolumeRegistry.get(volume_id),
         :ok <- validate_encrypted(volume),
         :ok <- validate_not_rotating(volume_id),
         old_version = volume.encryption.current_key_version,
         {:ok, new_version} <- KeyManager.rotate_volume_key(volume_id),
         chunks = ChunkIndex.get_chunks_for_volume(volume_id),
         encrypted_chunks = Enum.filter(chunks, &(&1.crypto != nil)),
         total_chunks = length(encrypted_chunks),
         rotation_state = build_rotation_state(old_version, new_version, total_chunks),
         {:ok, :ok, _leader} <-
           RaSupervisor.command({:set_rotation_state, volume_id, rotation_state}),
         {:ok, work_id} <- submit_worker(volume_id, old_version, new_version) do
      :telemetry.execute(
        [:neonfs, :rotation, :started],
        %{total_chunks: total_chunks},
        %{volume_id: volume_id, from_version: old_version, to_version: new_version}
      )

      AuditLog.log_event(
        event_type: :key_rotated,
        actor_uid: 0,
        resource: volume_id,
        details: %{
          from_version: old_version,
          to_version: new_version,
          total_chunks: total_chunks
        }
      )

      {:ok,
       %{
         volume_id: volume_id,
         from_version: old_version,
         to_version: new_version,
         total_chunks: total_chunks,
         work_id: work_id
       }}
    end
  end

  @doc """
  Returns the current rotation status for a volume.

  Reads rotation state directly from Ra for consistency.
  Returns the rotation state map, or `{:error, :no_rotation}` if no rotation
  is in progress.
  """
  @spec rotation_status(binary()) :: {:ok, map()} | {:error, term()}
  def rotation_status(volume_id) do
    case query_rotation_state(volume_id) do
      {:ok, nil} -> {:error, :no_rotation}
      {:ok, rotation} -> {:ok, rotation}
      {:error, _} = error -> error
    end
  end

  # Private

  defp validate_encrypted(volume) do
    if VolumeEncryption.active?(volume.encryption) do
      :ok
    else
      {:error, :not_encrypted}
    end
  end

  defp validate_not_rotating(volume_id) do
    case query_rotation_state(volume_id) do
      {:ok, nil} -> :ok
      {:ok, _rotation} -> {:error, :rotation_in_progress}
      {:error, _} = error -> error
    end
  end

  defp query_rotation_state(volume_id) do
    RaSupervisor.query(fn state ->
      state.volumes
      |> Map.get(volume_id)
      |> case do
        nil -> :volume_not_found
        vol_data -> get_in(vol_data, [:encryption, :rotation])
      end
    end)
    |> translate_rotation_result()
  end

  defp translate_rotation_result({:ok, :volume_not_found}), do: {:error, :not_found}
  defp translate_rotation_result({:ok, rotation}), do: {:ok, rotation}
  defp translate_rotation_result({:error, _} = error), do: error

  defp build_rotation_state(from_version, to_version, total_chunks) do
    %{
      from_version: from_version,
      to_version: to_version,
      started_at: DateTime.utc_now(),
      progress: %{total_chunks: total_chunks, migrated: 0}
    }
  end

  defp submit_worker(volume_id, old_version, new_version) do
    BackgroundWorker.submit(
      fn -> Worker.run(volume_id, old_version, new_version) end,
      priority: :low,
      label: "key_rotation:#{volume_id}"
    )
  end
end
