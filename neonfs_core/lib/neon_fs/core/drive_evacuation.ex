defmodule NeonFS.Core.DriveEvacuation do
  @moduledoc """
  Orchestrates graceful drive evacuation (data migration off a drive).

  Before retiring or replacing a physical drive, operators use this module to
  migrate all chunks to other drives, respecting volume redundancy settings.
  Chunks that are over-replicated can simply be pruned rather than migrated.

  ## Usage

      {:ok, job} = DriveEvacuation.start_evacuation(node, drive_id, any_tier: false)
      {:ok, status} = DriveEvacuation.evacuation_status(drive_id)
  """

  require Logger

  alias NeonFS.Core.{
    ChunkIndex,
    DriveRegistry,
    DriveState,
    JobTracker,
    StorageMetrics
  }

  alias NeonFS.Core.Job.Runners.DriveEvacuation, as: EvacuationRunner

  @doc """
  Starts evacuation of all data from a drive.

  Pre-flight checks:
  1. Drive exists
  2. Drive is not already draining
  3. Standby drives are spun up
  4. Sufficient capacity on candidate target drives

  ## Options

    * `:any_tier` — when `true`, allows migration to any tier (default: `false`)
  """
  @spec start_evacuation(node(), String.t(), keyword()) ::
          {:ok, NeonFS.Core.Job.t()} | {:error, term()}
  def start_evacuation(node, drive_id, opts \\ []) do
    any_tier = Keyword.get(opts, :any_tier, false)

    with {:ok, drive} <- get_drive(node, drive_id),
         :ok <- check_not_draining(drive),
         :ok <- ensure_drive_active(node, drive),
         :ok <- check_capacity(node, drive, any_tier),
         :ok <- set_draining(node, drive_id),
         {:ok, total_chunks} <- count_chunks(node, drive_id) do
      params = %{
        node: node,
        drive_id: drive_id,
        any_tier: any_tier,
        total_chunks: total_chunks
      }

      case JobTracker.create(EvacuationRunner, params) do
        {:ok, job} ->
          Logger.info(
            "Started evacuation of drive #{drive_id} on #{node} " <>
              "(#{total_chunks} chunks, any_tier: #{any_tier})"
          )

          {:ok, job}

        {:error, reason} ->
          # Roll back draining state on job creation failure
          restore_active(node, drive_id)
          {:error, reason}
      end
    end
  end

  @doc """
  Returns the status of an active or recent evacuation for a drive.
  """
  @spec evacuation_status(String.t()) :: {:ok, map()} | {:error, :no_evacuation}
  def evacuation_status(drive_id) do
    jobs = JobTracker.list(type: EvacuationRunner)

    case Enum.find(jobs, fn job -> job.params[:drive_id] == drive_id end) do
      nil ->
        {:error, :no_evacuation}

      job ->
        {:ok,
         %{
           job_id: job.id,
           status: job.status,
           progress: job.progress,
           drive_id: drive_id,
           node: job.params[:node],
           any_tier: job.params[:any_tier]
         }}
    end
  end

  ## Private

  defp get_drive(node, drive_id) do
    if node == Node.self() do
      DriveRegistry.get_drive(node, drive_id)
    else
      case :rpc.call(node, DriveRegistry, :get_drive, [node, drive_id], 10_000) do
        {:ok, _drive} = ok -> ok
        {:error, _} = error -> error
        {:badrpc, reason} -> {:error, {:rpc_error, reason}}
      end
    end
  end

  defp check_not_draining(%{state: :draining}),
    do: {:error, :already_draining}

  defp check_not_draining(_drive), do: :ok

  defp ensure_drive_active(node, %{state: :standby, id: drive_id}) do
    if node == Node.self() do
      DriveState.ensure_active(drive_id)
    else
      case :rpc.call(node, DriveState, :ensure_active, [drive_id], 30_000) do
        :ok -> :ok
        {:error, _} = error -> error
        {:badrpc, reason} -> {:error, {:rpc_error, reason}}
      end
    end
  end

  defp ensure_drive_active(_node, _drive), do: :ok

  defp check_capacity(node, drive, any_tier) do
    exclude = [{node, drive.id}]

    available =
      if any_tier do
        StorageMetrics.available_capacity_any_tier(exclude)
      else
        StorageMetrics.available_capacity_for_tier(drive.tier, exclude)
      end

    case available do
      :unlimited ->
        :ok

      bytes when bytes >= drive.used_bytes ->
        :ok

      _ ->
        {:error, :insufficient_capacity}
    end
  end

  defp set_draining(node, drive_id) do
    if node == Node.self() do
      DriveRegistry.update_state(drive_id, :draining)
    else
      case :rpc.call(node, DriveRegistry, :update_state, [drive_id, :draining], 10_000) do
        :ok -> :ok
        {:error, _} = error -> error
        {:badrpc, reason} -> {:error, {:rpc_error, reason}}
      end
    end
  end

  defp count_chunks(node, drive_id) do
    chunks =
      if node == Node.self() do
        ChunkIndex.list_by_drive(node, drive_id)
      else
        case :rpc.call(node, ChunkIndex, :list_by_drive, [node, drive_id], 30_000) do
          result when is_list(result) -> result
          {:badrpc, reason} -> {:error, {:rpc_error, reason}}
        end
      end

    case chunks do
      {:error, _} = error -> error
      list when is_list(list) -> {:ok, length(list)}
    end
  end

  @doc false
  def restore_active(node, drive_id) do
    if node == Node.self() do
      DriveRegistry.update_state(drive_id, :active)
    else
      :rpc.call(node, DriveRegistry, :update_state, [drive_id, :active], 10_000)
    end
  rescue
    _ -> :ok
  end
end
