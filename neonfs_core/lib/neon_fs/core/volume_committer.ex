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

  ## The worker is the volume's serialisation point

  Because every commit for a volume routes through one process, it is also
  the only place a caller's read-modify-write can be checked against what
  the volume currently holds. A `:precondition` in `writer_opts` runs there,
  immediately before the batch: a caller that read a value, computed a new
  one from it and arrived after someone else published a different one is
  refused rather than overwriting them. Checking before the call instead
  would leave exactly the window the check exists to close.

  ## A commit that outruns its deadline is an error, not an exit

  `commit/3` answers a call timeout with a `class: :unavailable` error
  rather than letting the exit propagate. The caller is `FileIndex`, which
  is holding a whole batch's worth of pending replies: an exit there kills
  the index, strands every other caller in that flush waiting for a reply
  that never comes, and takes the process down mid-write. An error reply
  fails the batch, and only the batch.

  The worker keeps going, so a commit reported as timed out may still land.
  That is the honest report — the write's fate is genuinely unknown at that
  point — and it is why the caller's remedy is to check rather than assume.
  """

  use GenServer

  alias NeonFS.Core.Volume.MetadataWriter
  alias NeonFS.Error.Unavailable

  @supervisor __MODULE__.Supervisor

  # Generous enough to outlast the writer's CAS-retry backoff so the
  # FileIndex-side call doesn't give up before the worker does.
  @default_commit_timeout 30_000

  @doc """
  How long a commit may take before it is reported as timed out.

  This bounds the slowest thing a `FileIndex` flush waits on, so every
  `FileIndex` client call that can trigger a flush must allow strictly
  more than this — see `NeonFS.Core.FileIndex.mutation_call_timeout/0`.
  Exposed rather than duplicated so the two cannot drift.

  Configurable as `:neonfs_core, :volume_commit_timeout_ms` — a deployment
  whose metadata writes are genuinely slower than the default has one
  number to raise, rather than a rebuild.
  """
  @spec commit_timeout() :: pos_integer()
  def commit_timeout do
    Application.get_env(:neonfs_core, :volume_commit_timeout_ms, @default_commit_timeout)
  end

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

  `writer_opts` may carry a `:precondition` — a zero-arity function run on
  the worker just before the batch. Anything other than `:ok` from it is
  returned instead of committing.
  """
  @spec commit(binary(), [MetadataWriter.mutation()], keyword()) ::
          {:ok, %{optional(non_neg_integer()) => binary()}} | MetadataWriter.write_error()
  def commit(volume_id, mutations, writer_opts) do
    GenServer.call(
      {:via, PartitionSupervisor, {@supervisor, volume_id}},
      {:commit, volume_id, mutations, writer_opts},
      commit_timeout()
    )
  catch
    :exit, {:timeout, _call} ->
      {:error,
       Unavailable.exception(
         message: "Volume metadata commit timed out",
         details: %{volume_id: volume_id, mutations: length(mutations)}
       )}
  end

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:commit, volume_id, mutations, writer_opts}, _from, state) do
    {precondition, writer_opts} = Keyword.pop(writer_opts, :precondition)

    case check(precondition) do
      :ok -> {:reply, MetadataWriter.apply_batch(volume_id, mutations, writer_opts), state}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  defp check(nil), do: :ok
  defp check(precondition) when is_function(precondition, 0), do: precondition.()
end
