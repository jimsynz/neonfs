defmodule NeonFS.Core.Job.Runners.ReplicaRepair do
  @moduledoc """
  Job runner that drives `NeonFS.Core.ReplicaRepair.repair_volume/2`
  in batched cycles, persisting the resume cursor through `JobTracker`
  after each step.

  Mirrors the `NeonFS.Core.Job.Runners.Scrub` shape: each call to
  `step/1` processes one batch and either returns `{:continue, job}`
  with the next-cursor stashed in `job.state[:cursor]`, or
  `{:complete, job}` when the volume's chunk list has been fully
  walked. `JobTracker` calls `step/1` again with the persisted state
  on resume after a node restart.

  ## Params

    * `:volume_id` (required) — the volume to repair.

  ## State

    * `:cursor` — opaque integer offset passed to
      `ReplicaRepair.repair_volume/2`. `nil` on first invocation.
    * `:added` / `:removed` / `:errors` — running totals across
      batches; surfaced in `job.progress` so `cluster repair status`
      can report cumulative work.
  """

  @behaviour NeonFS.Core.Job.Runner

  require Logger

  alias NeonFS.Core.ReplicaRepair

  @default_batch_size 100

  @impl NeonFS.Core.Job.Runner
  def label, do: "replica-repair"

  @impl NeonFS.Core.Job.Runner
  def step(job) do
    volume_id = volume_id_from(job.params)
    cursor = Map.get(job.state, :cursor, 0)

    batch_size =
      Application.get_env(:neonfs_core, :replica_repair_batch_size, @default_batch_size)

    case repair_volume(volume_id, batch_size: batch_size, cursor: cursor) do
      {:ok, %{added: a, removed: r, errors: errs, next_cursor: next}} ->
        merged = merge_state(job.state, a, r, errs, next)
        progress = build_progress(job.progress, merged)

        case next do
          :done ->
            {:complete, %{job | state: merged, progress: %{progress | description: "Complete"}}}

          n when is_integer(n) ->
            {:continue, %{job | state: merged, progress: progress}}
        end

      {:error, reason} ->
        Logger.warning("ReplicaRepair runner: pass-level failure",
          volume_id: volume_id,
          reason: inspect(reason)
        )

        {:error, reason, job}
    end
  end

  @impl NeonFS.Core.Job.Runner
  def on_cancel(_job), do: :ok

  defp volume_id_from(params) do
    params[:volume_id] || params["volume_id"] ||
      raise ArgumentError, "ReplicaRepair runner requires :volume_id param"
  end

  defp repair_volume(volume_id, opts) do
    repair_mod().repair_volume(volume_id, opts)
  end

  defp repair_mod do
    Application.get_env(:neonfs_core, :replica_repair_mod, ReplicaRepair)
  end

  defp merge_state(state, added, removed, errors, next_cursor) do
    state
    |> Map.update(:added, added, &(&1 + added))
    |> Map.update(:removed, removed, &(&1 + removed))
    |> Map.update(:errors, errors, &(&1 ++ errors))
    |> Map.put(:cursor, if(next_cursor == :done, do: nil, else: next_cursor))
  end

  defp build_progress(existing_progress, state) do
    completed = Map.get(state, :added, 0) + Map.get(state, :removed, 0)
    total = max(existing_progress.total || 0, completed)

    %{
      total: total,
      completed: completed,
      description: "Reconciling replica counts"
    }
  end
end
