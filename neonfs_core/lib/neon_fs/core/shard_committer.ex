defmodule NeonFS.Core.ShardCommitter do
  @moduledoc """
  Per-`{volume_id, shard}` metadata-commit worker (#1308).

  A `PartitionSupervisor` of these owns the commit step for volume-root
  shards. `FileIndex` splits a flush's mutations by shard and routes each
  shard's group here, so distinct shards commit **concurrently** (they
  land on different partitions) while the same `{volume, shard}` always
  routes to one process — giving each shard root exactly one writer and
  preserving the no-CAS-thrash invariant (#1260) without a global lock.

  The workers are stateless: a commit is a single
  `MetadataWriter.apply_shard_batch/4` (one root-CAS for that shard).
  """

  use GenServer

  alias NeonFS.Core.Volume.MetadataWriter

  @supervisor __MODULE__.Supervisor

  # Generous enough to outlast the writer's CAS-retry backoff so the
  # FileIndex-side call doesn't give up before the worker does.
  @commit_timeout 30_000

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
  Commit `mutations` (all belonging to `shard`) on `{volume_id, shard}`'s
  worker. Returns the shard's new `root_chunk_hash` or an error.
  """
  @spec commit(binary(), non_neg_integer(), [MetadataWriter.mutation()], keyword()) ::
          {:ok, binary()} | MetadataWriter.write_error()
  def commit(volume_id, shard, mutations, writer_opts) do
    GenServer.call(
      {:via, PartitionSupervisor, {@supervisor, {volume_id, shard}}},
      {:commit, volume_id, shard, mutations, writer_opts},
      @commit_timeout
    )
  end

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:commit, volume_id, shard, mutations, writer_opts}, _from, state) do
    {:reply, MetadataWriter.apply_shard_batch(volume_id, shard, mutations, writer_opts), state}
  end
end
