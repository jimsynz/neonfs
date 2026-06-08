defmodule NeonFS.Integration.GCSnapshotLifecycleTest do
  @moduledoc """
  End-to-end multi-root garbage-collection lifecycle on a real Ra-backed
  single-node cluster (#985): write a file, snapshot the volume, delete
  the file from the live head, and run GC — the chunks survive because
  they are still reachable from the snapshot's frozen root. Deleting the
  snapshot and running GC again reclaims them.

  Complements the stubbed-reader unit tests in `garbage_collector_test.exs`
  and `gc_with_snapshots_test.exs` by driving real volume provisioning, a
  real chunked write through the ETS-backed indexes, and the default Ra
  snapshot enumerator end to end.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.{ChunkIndex, GarbageCollector, Snapshot}

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_single_node_cluster(cluster, name: "gc-snap-lifecycle")
    :ok
  end

  test "snapshot pins a deleted file's chunks through GC; deleting the snapshot reclaims them",
       %{cluster: cluster} do
    volume = unique_volume("gc-snap")

    {:ok, %{id: vol_id}} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [volume, %{}])

    path = "/keepsake.bin"
    content = :crypto.strong_rand_bytes(128 * 1024)

    {:ok, %{chunks: chunks}} =
      PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
        volume,
        path,
        content
      ])

    assert chunks != []
    assert Enum.all?(chunks, &chunk_present?(cluster, vol_id, &1))

    {:ok, snap} = PeerCluster.rpc(cluster, :node1, Snapshot, :create, [vol_id, [name: "keep"]])

    # Advance the live root past the file; its chunks are now reachable
    # only from the snapshot's frozen root.
    :ok = PeerCluster.rpc(cluster, :node1, NeonFS.Core, :delete_file, [volume, path])

    assert {:error, :not_found} =
             PeerCluster.rpc(cluster, :node1, NeonFS.Core, :get_file_meta, [volume, path])

    # The mark phase must walk the snapshot root, so every chunk survives.
    assert {:ok, _} =
             PeerCluster.rpc(cluster, :node1, GarbageCollector, :collect, [[volume_id: vol_id]])

    assert Enum.all?(chunks, &chunk_present?(cluster, vol_id, &1)),
           "chunks reachable only via the snapshot must survive GC"

    # Drop the snapshot — nothing references the chunks now.
    :ok = PeerCluster.rpc(cluster, :node1, Snapshot, :delete, [vol_id, snap.id])

    assert {:ok, result} =
             PeerCluster.rpc(cluster, :node1, GarbageCollector, :collect, [[volume_id: vol_id]])

    assert result.chunks_deleted >= length(chunks)

    assert :ok =
             wait_until(
               fn -> Enum.all?(chunks, &(not chunk_present?(cluster, vol_id, &1))) end,
               timeout: 5_000
             )
  end

  defp unique_volume(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"

  defp chunk_present?(cluster, vol_id, hash) do
    PeerCluster.rpc(cluster, :node1, ChunkIndex, :exists?, [vol_id, hash])
  end
end
