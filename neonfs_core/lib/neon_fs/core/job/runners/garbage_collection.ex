defmodule NeonFS.Core.Job.Runners.GarbageCollection do
  @moduledoc """
  Job runner for garbage collection.

  Wraps `GarbageCollector.collect/1` as a tracked job. The entire collection
  runs in a single `step/1` call because the mark phase needs a complete
  referenced set before sweeping.

  ## Params

  - `:volume_id` (optional) — restrict collection to a single volume
  """

  @behaviour NeonFS.Core.Job.Runner

  alias NeonFS.Core.GarbageCollector

  @impl NeonFS.Core.Job.Runner
  def label, do: "garbage-collection"

  @impl NeonFS.Core.Job.Runner
  def step(job) do
    opts = collect_opts(job.params)

    case GarbageCollector.collect(opts) do
      {:ok, result} ->
        updated = %{
          job
          | progress: %{
              total: result.chunks_deleted + result.chunks_protected,
              completed: result.chunks_deleted + result.chunks_protected,
              description: "Complete"
            },
            state:
              Map.merge(job.state, %{
                chunks_deleted: result.chunks_deleted,
                stripes_deleted: result.stripes_deleted,
                chunks_protected: result.chunks_protected
              })
        }

        {:complete, updated}
    end
  end

  defp collect_opts(%{volume_id: volume_id}), do: [volume_id: volume_id]
  defp collect_opts(_), do: []
end
