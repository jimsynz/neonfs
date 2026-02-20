defmodule NeonFS.Core.Job do
  @moduledoc """
  Represents a long-running, persistent background job.

  Jobs are user-visible operations like key rotation or drive evacuation.
  They survive node restarts, track progress, and are visible across the
  cluster via CLI (`neonfs-cli job list`).

  Each job has a `type` field that is the runner module itself (e.g.
  `NeonFS.Core.Job.Runners.KeyRotation`). The runner implements the
  `NeonFS.Core.Job.Runner` behaviour.
  """

  @type status :: :pending | :running | :paused | :completed | :failed | :cancelled

  @type progress :: %{
          total: non_neg_integer(),
          completed: non_neg_integer(),
          description: String.t()
        }

  @type t :: %__MODULE__{
          id: String.t(),
          type: module(),
          node: node(),
          status: status(),
          progress: progress(),
          params: map(),
          state: map(),
          error: term() | nil,
          created_at: DateTime.t(),
          started_at: DateTime.t() | nil,
          updated_at: DateTime.t(),
          completed_at: DateTime.t() | nil
        }

  @enforce_keys [:id, :type, :node, :status, :progress, :params, :created_at, :updated_at]
  defstruct [
    :id,
    :type,
    :node,
    :status,
    :progress,
    :params,
    :error,
    :created_at,
    :started_at,
    :updated_at,
    :completed_at,
    state: %{}
  ]

  @doc """
  Creates a new job with a generated ID and `:pending` status.
  """
  @spec new(module(), map()) :: t()
  def new(type, params) when is_atom(type) and is_map(params) do
    now = DateTime.utc_now()

    %__MODULE__{
      id: generate_id(),
      type: type,
      node: Node.self(),
      status: :pending,
      progress: %{total: 0, completed: 0, description: ""},
      params: params,
      state: %{},
      created_at: now,
      updated_at: now
    }
  end

  @doc """
  Returns a percentage string for the job's progress.
  """
  @spec progress_percent(t()) :: String.t()
  def progress_percent(%__MODULE__{progress: %{total: 0}}), do: "0%"

  def progress_percent(%__MODULE__{progress: %{total: total, completed: completed}}) do
    percent = div(completed * 100, total)
    "#{percent}%"
  end

  @doc """
  Returns true if the job is in a terminal state.
  """
  @spec terminal?(t()) :: boolean()
  def terminal?(%__MODULE__{status: status}), do: status in [:completed, :failed, :cancelled]

  defp generate_id do
    :crypto.strong_rand_bytes(12) |> Base.encode16(case: :lower)
  end
end
