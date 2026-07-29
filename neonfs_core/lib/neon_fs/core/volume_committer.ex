defmodule NeonFS.Core.VolumeCommitter do
  @moduledoc """
  Per-volume metadata-commit worker.

  A `PartitionSupervisor` of these owns the commit step for volume roots.
  `FileIndex` routes each volume's flush here, so distinct volumes commit
  **concurrently** (they land on different partitions) while the same
  volume always routes to one process.

  The volume — not the shard — is the routing key because a flush publishes
  every shard it touched in a single consensus round: the serialisation
  point is the volume's root set, and a per-shard worker could no longer own
  a commit that spans its neighbours.

  The workers are stateless: a commit is a single
  `MetadataWriter.apply_batch/3`.
  """

  use GenServer

  alias NeonFS.Core.Volume.MetadataWriter

  @supervisor __MODULE__.Supervisor

  # Generous enough to outlast the writer's CAS-retry backoff so the
  # FileIndex-side call doesn't give up before the worker does.
  @commit_timeout 30_000

  @doc """
  How long a commit may take before the worker itself gives up.

  This bounds the slowest thing a `FileIndex` flush waits on, so every
  `FileIndex` client call that can trigger a flush must allow strictly
  more than this — see `NeonFS.Core.FileIndex.mutation_call_timeout/0`.
  Exposed rather than duplicated so the two cannot drift.
  """
  @spec commit_timeout() :: pos_integer()
  def commit_timeout, do: @commit_timeout

  @doc """
  Child spec for the `PartitionSupervisor` that owns the stateless worker
  pool. Add this to a supervision tree (workers are started and routed by
  `PartitionSupervisor`).
  """
  def pool_spec do
    {PartitionSupervisor, child_spec: __MODULE__, name: @supervisor}
  end

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc """
  Commit `mutations` on `volume_id`'s worker as one atomic publication.
  Returns `%{shard => root_chunk_hash}` for the shards touched, or an error.
  """
  @spec commit(binary(), [MetadataWriter.mutation()], keyword()) ::
          {:ok, %{optional(non_neg_integer()) => binary()}} | MetadataWriter.write_error()
  def commit(volume_id, mutations, writer_opts) do
    GenServer.call(
      {:via, PartitionSupervisor, {@supervisor, volume_id}},
      {:commit, volume_id, mutations, writer_opts},
      @commit_timeout
    )
  end

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:commit, volume_id, mutations, writer_opts}, _from, state) do
    {:reply, MetadataWriter.apply_batch(volume_id, mutations, writer_opts), state}
  end
end
