defmodule NeonFS.Core.Job.Runner do
  @moduledoc """
  Behaviour for job runners.

  A runner implements a single callback, `step/1`, which processes one unit
  of work for a job. The JobTracker calls `step/1` in a loop, persisting
  the job after each `{:continue, job}` return.

  Resuming after a restart is just calling `step/1` again with the last
  persisted job state — there is no separate resume path.

  ## Example

      defmodule MyRunner do
        @behaviour NeonFS.Core.Job.Runner

        @impl true
        def step(job) do
          if done?(job) do
            {:complete, job}
          else
            updated = do_work(job)
            {:continue, updated}
          end
        end

        @impl true
        def label, do: "my-runner"
      end
  """

  alias NeonFS.Core.Job

  @doc """
  Processes one unit of work for a job.

  Returns:
  - `{:continue, job}` — more work to do, persist and call again
  - `{:complete, job}` — all done
  - `{:error, reason, job}` — failed, mark as failed
  """
  @callback step(job :: Job.t()) ::
              {:continue, Job.t()}
              | {:complete, Job.t()}
              | {:error, term(), Job.t()}

  @doc """
  Returns a human-readable label for CLI display (e.g. `"key-rotation"`).
  """
  @callback label() :: String.t()

  @doc """
  Called when a job is cancelled. Optional cleanup hook.
  """
  @callback on_cancel(job :: Job.t()) :: :ok
  @optional_callbacks [on_cancel: 1]
end
