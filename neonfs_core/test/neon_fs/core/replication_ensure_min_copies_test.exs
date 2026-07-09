defmodule NeonFS.Core.ReplicationEnsureMinCopiesTest do
  @moduledoc """
  Unit tests for `NeonFS.Core.Replication.ensure_min_copies/2` (#1500) —
  the synchronous per-chunk durability barrier. `ChunkIndex`, `BlobStore`,
  `DriveRegistry`, and `DriveTrust` are stubbed via Mimic so the decision
  logic (already-durable short-circuit, `:trusted`-only counting, and the
  under-replicated verdict) is exercised without a running cluster.

  Real cross-node placement — the `Task.async_stream` write fan-out that
  Mimic can't reach through the child process — is covered end-to-end by
  `NeonFS.Integration.SyncDurabilityTest`.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Core.{BlobStore, ChunkIndex, ChunkMeta, DriveRegistry, DriveTrust, Replication}

  setup :verify_on_exit!

  setup do
    stub(DriveTrust, :unverified, fn -> [] end)
    :ok
  end

  @volume_id "vol-1"
  @local Node.self()

  defp volume(min_copies) do
    %{
      id: @volume_id,
      durability: %{type: :replicate, factor: 3, min_copies: min_copies},
      tiering: %{initial_tier: :hot}
    }
  end

  defp chunk(locations) do
    %ChunkMeta{
      hash: "h1",
      original_size: 4,
      stored_size: 4,
      compression: :none,
      crypto: nil,
      locations: locations,
      target_replicas: 3,
      commit_state: :committed,
      active_write_refs: MapSet.new(),
      volume_ids: MapSet.new([@volume_id]),
      created_at: DateTime.utc_now()
    }
  end

  defp loc(node, drive_id, tier \\ :hot), do: %{node: node, drive_id: drive_id, tier: tier}

  test "returns :ok immediately when a chunk already meets min_copies" do
    stub(ChunkIndex, :get, fn @volume_id, "h1" ->
      {:ok, chunk([loc(@local, "d1"), loc(:n2, "d2")])}
    end)

    reject(&BlobStore.read_chunk/2)
    reject(&BlobStore.read_chunk/3)

    assert :ok = Replication.ensure_min_copies("h1", volume(2))
  end

  test "returns :ok for a single-copy volume that is already satisfied" do
    stub(ChunkIndex, :get, fn @volume_id, "h1" -> {:ok, chunk([loc(@local, "d1")])} end)
    reject(&BlobStore.read_chunk/2)

    assert :ok = Replication.ensure_min_copies("h1", volume(1))
  end

  test "unverified replicas don't count toward min_copies (#1375)" do
    stub(DriveTrust, :unverified, fn -> [{:n2, "d2"}] end)

    # Two physical copies, but n2's is :unverified — only one counts, so the
    # chunk is short of min_copies=2. With no spare drives to top up, the
    # barrier reports the shortfall rather than being fooled into :ok.
    stub(ChunkIndex, :get, fn @volume_id, "h1" ->
      {:ok, chunk([loc(@local, "d1"), loc(:n2, "d2")])}
    end)

    stub(BlobStore, :read_chunk, fn "h1", "d1" -> {:ok, "data"} end)
    stub(DriveRegistry, :drives_for_tier, fn :hot -> [] end)

    assert {:error, {:under_replicated, 1, 2}} = Replication.ensure_min_copies("h1", volume(2))
  end

  test "returns {:error, {:under_replicated, have, want}} when the cluster lacks drives" do
    stub(ChunkIndex, :get, fn @volume_id, "h1" -> {:ok, chunk([loc(@local, "d1")])} end)
    stub(BlobStore, :read_chunk, fn "h1", "d1" -> {:ok, "data"} end)
    stub(DriveRegistry, :drives_for_tier, fn :hot -> [] end)

    assert {:error, {:under_replicated, 1, 3}} = Replication.ensure_min_copies("h1", volume(3))
  end

  test "returns {:error, :not_found} for an unknown chunk" do
    stub(ChunkIndex, :get, fn @volume_id, "h1" -> {:error, :not_found} end)

    assert {:error, :not_found} = Replication.ensure_min_copies("h1", volume(2))
  end
end
