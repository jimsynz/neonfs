defmodule NeonFS.TestSupport.MetadataBench do
  @moduledoc """
  Peer-local telemetry counters for the metadata write path, used by the
  #1292 metadata write-throughput benchmark to attribute Ra
  `:cas_update_volume_root` round-trips (and stale-pointer retries) to a
  burst of file creates.

  `NeonFS.Core.MetadataStateMachine` emits
  `[:neonfs, :ra, :command, :update_volume_root]` on every successful root
  flip (both the unconditional `:update_volume_root` and the CAS
  `:cas_update_volume_root` paths) and
  `[:neonfs, :ra, :command, :cas_update_volume_root_stale]` on every CAS
  conflict. This module attaches a handler that accumulates both counts in
  a peer-local `:counters` array, which can then be read back via RPC.

  The handler must be a module function on the peer's code path — an
  anonymous closure defined in a test module cannot be resolved on the
  peer (its beam is not on disk). Storage is `:counters` in
  `:persistent_term` rather than ETS because the peer-side `attach/0` runs
  in a short-lived RPC process: an ETS table owned by that process would
  vanish the moment the call returned, while the handler fires later from
  Ra processes.

  Usage (from an integration test, on the Ra leader node):

      :ok = PeerCluster.rpc(cluster, :node1, MetadataBench, :attach, [])
      # ... drive a burst of creates ...
      %{root_updates: u, cas_retries: r} =
        PeerCluster.rpc(cluster, :node1, MetadataBench, :snapshot, [])
  """

  @handler_id {__MODULE__, :counter}
  @term_key {__MODULE__, :counters}

  @root_update_event [:neonfs, :ra, :command, :update_volume_root]
  @cas_stale_event [:neonfs, :ra, :command, :cas_update_volume_root_stale]

  @root_update_slot 1
  @cas_retry_slot 2

  @doc """
  Reset the counters and attach the telemetry handler.

  Idempotent; safe to call multiple times. Each call zeroes the counts so
  a benchmark can measure independent workloads against the same cluster.
  """
  @spec attach() :: :ok
  def attach do
    {:ok, _} = Application.ensure_all_started(:telemetry)

    :persistent_term.put(@term_key, :counters.new(2, [:write_concurrency]))

    _ = :telemetry.detach(@handler_id)

    :ok =
      :telemetry.attach_many(
        @handler_id,
        [@root_update_event, @cas_stale_event],
        &__MODULE__.handle_event/4,
        nil
      )
  end

  @doc """
  Detach the handler. Safe to call even if already detached.
  """
  @spec detach() :: :ok
  def detach do
    _ = :telemetry.detach(@handler_id)
    :ok
  end

  @doc """
  Read the accumulated counts since the last `attach/0`.
  """
  @spec snapshot() :: %{root_updates: non_neg_integer(), cas_retries: non_neg_integer()}
  def snapshot do
    counters = :persistent_term.get(@term_key)

    %{
      root_updates: :counters.get(counters, @root_update_slot),
      cas_retries: :counters.get(counters, @cas_retry_slot)
    }
  end

  @doc false
  def handle_event(@root_update_event, _measurements, _metadata, _config) do
    :counters.add(:persistent_term.get(@term_key), @root_update_slot, 1)
  end

  def handle_event(@cas_stale_event, _measurements, _metadata, _config) do
    :counters.add(:persistent_term.get(@term_key), @cas_retry_slot, 1)
  end
end
