defmodule NeonFS.Core.DriveEvacuation do
  @moduledoc """
  Orchestrates graceful drive evacuation (data migration off a drive).

  Before retiring or replacing a physical drive, operators use this module to
  migrate all chunks to other drives, respecting volume redundancy settings.
  Chunks that are over-replicated can simply be pruned rather than migrated.

  ## Usage

      {:ok, job} = DriveEvacuation.start_evacuation(node, drive_id)
      {:ok, status} = DriveEvacuation.evacuation_status(drive_id)
  """

  require Logger

  alias NeonFS.Core.{
    ChunkIndex,
    DriveRegistry,
    DriveState,
    JobTracker,
    ReplicaAudit,
    StorageMetrics
  }

  alias NeonFS.Core.Job.Runners.DriveEvacuation, as: EvacuationRunner

  @doc """
  Starts evacuation of all data from a drive.

  Pre-flight checks:
  1. Drive exists
  2. Drive is not already draining
  3. If — and only if — no drive remains to relocate onto, no volume
     depends on this drive for its last copies
     (`ReplicaAudit.guard_removal/3`). Runs before anything with a side
     effect, so a refusal leaves the drive untouched
  4. Standby drives are spun up
  5. Sufficient cluster-wide capacity to absorb the evacuating drive

  Target drive selection prefers a drive on the same tier as the source,
  and falls back to any tier when none is available — evacuation must
  succeed even if no same-tier drive exists on the cluster.

  ## Options

    * `:force` - start even though this drive holds a volume's last
      copies and there is nowhere to relocate them (default `false`).
      Only reachable in that no-target case — with a target, evacuation
      preserves the copy count and is not gated. Never overrides
      `_system` being left with no surviving copy.
  """
  @spec start_evacuation(node(), String.t(), keyword()) ::
          {:ok, NeonFS.Core.Job.t()} | {:error, term()}
  def start_evacuation(node, drive_id, opts \\ []) do
    with {:ok, drive} <- get_drive(node, drive_id),
         :ok <- check_not_draining(drive),
         :ok <- guard_replicas(node, drive_id, opts),
         :ok <- ensure_drive_active(node, drive),
         :ok <- check_capacity(node, drive),
         :ok <- set_draining(node, drive_id),
         {:ok, total_chunks} <- count_chunks(node, drive_id) do
      params = %{
        node: node,
        drive_id: drive_id,
        total_chunks: total_chunks
      }

      case JobTracker.create(EvacuationRunner, params) do
        {:ok, job} ->
          Logger.info(
            "Started evacuation of drive #{drive_id} on #{node} (#{total_chunks} chunks)"
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
           node: job.params[:node]
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

  # Evacuation *relocates*; removal abandons. With somewhere to move the
  # data, evacuation preserves the copy count — that is the whole point of
  # the operation — so guarding it on "what if this drive vanished" would
  # refuse the canonical case: moving a `factor: 1` volume off a drive
  # before retiring it, which is precisely what an operator reaches for
  # evacuation to do.
  #
  # What evacuation cannot survive is having nowhere to put the data. The
  # blobs stay stranded on a draining drive, and finalisation's
  # filesystem-empty check is not what will catch that (#1628). So the
  # replica guard runs only when no target drive exists — which is also
  # the case where `_system` would be stranded, and where its
  # unforceable refusal has to hold.
  #
  # Target availability is read from the same source the runner's
  # `select_target_drive/2` uses, so the guard agrees with what the
  # evacuation would actually attempt.
  defp guard_replicas(node, drive_id, opts) do
    if relocation_target?(node, drive_id) do
      :ok
    else
      ReplicaAudit.guard_removal(node, drive_id,
        force: Keyword.get(opts, :force, false),
        operation: "Evacuating"
      )
    end
  end

  defp relocation_target?(evac_node, evac_drive_id) do
    Enum.any?(DriveRegistry.list_drives(), fn drive ->
      drive.state != :draining and
        not (drive.node == evac_node and drive.id == evac_drive_id)
    end)
  end

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

  defp check_capacity(node, drive) do
    exclude = [{node, drive.id}]

    case StorageMetrics.available_capacity_any_tier(exclude) do
      :unlimited -> :ok
      bytes when bytes >= drive.used_bytes -> :ok
      _ -> {:error, :insufficient_capacity}
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
