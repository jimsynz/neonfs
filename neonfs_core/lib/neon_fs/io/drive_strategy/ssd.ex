defmodule NeonFS.IO.DriveStrategy.SSD do
  @moduledoc """
  Parallel FIFO strategy for SSDs.

  SSDs have no seek penalty, so operations are returned in submission order
  with reads and writes freely interleaved. The `max_concurrent` setting
  controls how many operations the drive worker should run in parallel.
  """

  @behaviour NeonFS.IO.DriveStrategy

  alias NeonFS.IO.Operation

  @default_max_concurrent 64

  @type t :: %__MODULE__{
          max_concurrent: pos_integer(),
          queue: :queue.queue(Operation.t())
        }

  defstruct max_concurrent: @default_max_concurrent,
            queue: :queue.new()

  @impl true
  @spec init(keyword()) :: %__MODULE__{}
  def init(config \\ []) do
    %__MODULE__{
      max_concurrent: Keyword.get(config, :max_concurrent, @default_max_concurrent)
    }
  end

  @impl true
  @spec enqueue(%__MODULE__{}, Operation.t()) :: %__MODULE__{}
  def enqueue(%__MODULE__{} = state, %Operation{} = op) do
    %{state | queue: :queue.in(op, state.queue)}
  end

  @impl true
  @spec next_batch(%__MODULE__{}, pos_integer()) :: {[Operation.t()], %__MODULE__{}}
  def next_batch(%__MODULE__{} = state, count) when is_integer(count) and count > 0 do
    limit = min(count, state.max_concurrent)
    {batch, remaining} = dequeue_n(state.queue, limit)
    {batch, %{state | queue: remaining}}
  end

  @impl true
  @spec type() :: :ssd
  def type, do: :ssd

  defp dequeue_n(queue, count) do
    dequeue_n(queue, count, [])
  end

  defp dequeue_n(queue, 0, acc), do: {Enum.reverse(acc), queue}

  defp dequeue_n(queue, count, acc) do
    case :queue.out(queue) do
      {{:value, op}, rest} -> dequeue_n(rest, count - 1, [op | acc])
      {:empty, queue} -> {Enum.reverse(acc), queue}
    end
  end
end
