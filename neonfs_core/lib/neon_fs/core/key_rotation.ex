defmodule NeonFS.Core.KeyRotation do
  @moduledoc """
  Volume key rotation orchestration.

  Coordinates the lifecycle of a key rotation: generates a new key version
  and creates a background job to re-encrypt all chunks from the old key
  to the new key.

  Rotation progress is tracked via the JobTracker system, which persists
  state to ETS+DETS and survives node restarts.
  """

  alias NeonFS.Core.{
    AuditLog,
    ChunkIndex,
    JobTracker,
    KeyManager,
    VolumeRegistry
  }

  alias NeonFS.Core.Job.Runners.KeyRotation, as: KeyRotationRunner
  alias NeonFS.Core.VolumeEncryption

  @doc """
  Starts key rotation for a volume.

  Generates a new key version and creates a background job to re-encrypt
  all chunks.

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
         {:ok, job} <-
           JobTracker.create(KeyRotationRunner, %{
             volume_id: volume_id,
             from_version: old_version,
             to_version: new_version,
             total_chunks: total_chunks
           }) do
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
         job_id: job.id
       }}
    end
  end

  @doc """
  Returns the current rotation status for a volume.

  Queries JobTracker for running key-rotation jobs on this volume.
  Returns the job's progress, or `{:error, :no_rotation}` if no rotation
  is in progress.
  """
  @spec rotation_status(binary()) :: {:ok, map()} | {:error, term()}
  def rotation_status(volume_id) do
    with {:ok, _volume} <- VolumeRegistry.get(volume_id) do
      JobTracker.list(type: KeyRotationRunner, status: [:pending, :running])
      |> Enum.find(fn job -> job.params.volume_id == volume_id end)
      |> case do
        nil ->
          {:error, :no_rotation}

        job ->
          {:ok, format_rotation_status(job)}
      end
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
    case rotation_status(volume_id) do
      {:error, :no_rotation} -> :ok
      {:ok, _} -> {:error, :rotation_in_progress}
    end
  end

  defp format_rotation_status(job) do
    %{
      from_version: job.params.from_version,
      to_version: job.params.to_version,
      started_at: job.started_at,
      progress: %{
        total_chunks: job.progress.total,
        migrated: job.progress.completed
      }
    }
  end
end
