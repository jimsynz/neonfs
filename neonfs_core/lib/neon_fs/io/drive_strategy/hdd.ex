defmodule NeonFS.IO.DriveStrategy.HDD do
  @moduledoc """
  Elevator scheduling strategy for HDDs.

  Batches reads and writes separately, sorting by chunk hash prefix within
  each batch to minimise seek distance. Reads are served before writes
  within a dispatch cycle.

  ## Algorithm

  1. Collect pending operations into separate read and write queues.
  2. On `next_batch/2`, serve reads first (sorted by hash prefix), then writes.
  3. Within each type, sort by the `chunk_hash` metadata field to approximate
     disk locality.
  4. Operations without a `chunk_hash` sort to the end of their batch.
  """

  @behaviour NeonFS.IO.DriveStrategy

  alias NeonFS.IO.Operation

  @default_batch_size 32

  @type t :: %__MODULE__{
          batch_size: pos_integer(),
          reads: [Operation.t()],
          writes: [Operation.t()]
        }

  defstruct batch_size: @default_batch_size,
            reads: [],
            writes: []

  @impl true
  @spec init(keyword()) :: %__MODULE__{}
  def init(config \\ []) do
    %__MODULE__{
      batch_size: Keyword.get(config, :batch_size, @default_batch_size)
    }
  end

  @impl true
  @spec enqueue(%__MODULE__{}, Operation.t()) :: %__MODULE__{}
  def enqueue(%__MODULE__{} = state, %Operation{type: :read} = op) do
    %{state | reads: [op | state.reads]}
  end

  def enqueue(%__MODULE__{} = state, %Operation{type: :write} = op) do
    %{state | writes: [op | state.writes]}
  end

  @impl true
  @spec next_batch(%__MODULE__{}, pos_integer()) :: {[Operation.t()], %__MODULE__{}}
  def next_batch(%__MODULE__{} = state, count) when is_integer(count) and count > 0 do
    batch_limit = min(count, state.batch_size)

    {read_batch, remaining_reads} = take_sorted(state.reads, batch_limit)
    remaining_count = batch_limit - length(read_batch)

    {write_batch, remaining_writes} = take_sorted(state.writes, remaining_count)

    batch = read_batch ++ write_batch
    new_state = %{state | reads: remaining_reads, writes: remaining_writes}

    {batch, new_state}
  end

  @impl true
  @spec type() :: :hdd
  def type, do: :hdd

  defp take_sorted(ops, 0), do: {[], ops}
  defp take_sorted([], _count), do: {[], []}

  defp take_sorted(ops, count) do
    sorted = Enum.sort_by(ops, &hash_prefix/1)
    Enum.split(sorted, count)
  end

  defp hash_prefix(%Operation{metadata: %{chunk_hash: hash}}) when is_binary(hash), do: hash
  defp hash_prefix(_op), do: <<0xFF, 0xFF, 0xFF, 0xFF>>
end
