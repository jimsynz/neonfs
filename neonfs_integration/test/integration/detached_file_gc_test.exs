defmodule NeonFS.Integration.DetachedFileGCTest do
  @moduledoc """
  Peer-cluster end-to-end test for the unlink-while-open story
  (sub-issue #644 of #638). Validates that:

    1. A file pinned on `node1` and deleted from `node2` becomes
       invisible by path everywhere but stays reachable by `file_id`
       on every node.
    2. Releasing the pin on `node1` propagates through the namespace-
       coordinator's release telemetry to `DetachedFileGC` on every
       core node, which decrements the tombstone's
       `pinned_claim_ids`. When the list empties the FileMeta is
       finally purged, leaving the chunks reachable by the existing
       chunk GC.

  The test mirrors the `claim_pinned` peer-cluster shape from
  `namespace_coordinator_pinned_test.exs` (#637) but exercises the
  whole core-side write path. FUSE / NFS handler wiring is out of
  scope for this issue (#639, #640).
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.FileIndex

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "detached-file-gc-test")
    :ok
  end

  test "pin on node1 + delete from node2 + release pin: full lifecycle",
       %{cluster: cluster} do
    volume = unique_volume("dgc")
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.Core, :create_volume, [volume])

    path = "/handle.bin"
    file_id = write_file_and_get_id(cluster, :node1, volume, path)
    vol_id = volume_id(cluster, volume)

    # Pin on node1 — same shape as a FUSE peer holding an open fd.
    holder = start_holder(cluster, :node1)
    {:ok, %{claim_id: pin_id}} = pin(cluster, :node1, volume, path, holder)

    try do
      # Delete from node2 (path coordination is cluster-wide).
      :ok = PeerCluster.rpc(cluster, :node2, NeonFS.Core, :delete_file, [volume, path])

      # Path 404 from every node, but file_id lookup still works.
      for node <- [:node1, :node2, :node3] do
        assert {:error, %{class: :not_found}} =
                 PeerCluster.rpc(cluster, node, NeonFS.Core, :get_file_meta, [volume, path]),
               "path should be 404 on #{inspect(node)} after pinned delete"

        assert {:ok, %{detached: true, pinned_claim_ids: ids}} =
                 PeerCluster.rpc(cluster, node, FileIndex, :get, [vol_id, file_id]),
               "file_id should resolve on #{inspect(node)} while pinned"

        assert pin_id in ids
      end

      # Release the pin on node1. The release-telemetry event fires
      # via the namespace coordinator's existing path; the
      # `DetachedFileGC` handler on every core node decrements its
      # local copy and purges when the list empties.
      :ok = PeerCluster.rpc(cluster, :node1, NeonFS.Core, :unpin_file, [pin_id])

      # Wait for the GC to converge across all nodes — the telemetry
      # fires on Ra command commit, which replicates asynchronously.
      assert :ok =
               wait_until(fn -> file_gone_everywhere?(cluster, vol_id, file_id) end,
                 timeout: 5_000
               )
    after
      Agent.stop(holder, :normal, 1_000)
    end
  end

  test "earlier pin release leaves the file detached when others remain",
       %{cluster: cluster} do
    volume = unique_volume("dgc-multi")
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.Core, :create_volume, [volume])

    path = "/multi-handle.bin"
    file_id = write_file_and_get_id(cluster, :node1, volume, path)
    vol_id = volume_id(cluster, volume)

    holder1 = start_holder(cluster, :node1)
    holder2 = start_holder(cluster, :node2)

    {:ok, %{claim_id: pin1}} = pin(cluster, :node1, volume, path, holder1)
    {:ok, %{claim_id: pin2}} = pin(cluster, :node2, volume, path, holder2)

    try do
      :ok = PeerCluster.rpc(cluster, :node1, NeonFS.Core, :delete_file, [volume, path])

      # Both pins captured in the tombstone snapshot.
      assert {:ok, %{detached: true, pinned_claim_ids: snapshot}} =
               PeerCluster.rpc(cluster, :node3, FileIndex, :get, [vol_id, file_id])

      assert pin1 in snapshot and pin2 in snapshot

      # Release one pin — file remains detached because pin2 still holds.
      :ok = PeerCluster.rpc(cluster, :node1, NeonFS.Core, :unpin_file, [pin1])

      assert :ok =
               wait_until(
                 fn ->
                   case PeerCluster.rpc(cluster, :node3, FileIndex, :get, [vol_id, file_id]) do
                     {:ok, %{detached: true, pinned_claim_ids: [^pin2]}} -> true
                     _ -> false
                   end
                 end,
                 timeout: 5_000
               )

      # Release the second pin — file purged.
      :ok = PeerCluster.rpc(cluster, :node2, NeonFS.Core, :unpin_file, [pin2])

      assert :ok =
               wait_until(fn -> file_gone_everywhere?(cluster, vol_id, file_id) end,
                 timeout: 5_000
               )
    after
      Agent.stop(holder1, :normal, 1_000)
      Agent.stop(holder2, :normal, 1_000)
    end
  end

  # Pins are keyed by file identity (#1605), so the name a handle was
  # opened under stops mattering the moment it is taken: rename on one
  # node, unlink from a third, and the pin taken on the first still
  # tombstones the file instead of losing its chunks.
  test "rename on node2 then unlink on node3 detaches the file pinned on node1",
       %{cluster: cluster} do
    volume = unique_volume("dgc-rename")
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.Core, :create_volume, [volume])

    path = "/opened-as.bin"
    renamed = "/renamed-to.bin"
    file_id = write_file_and_get_id(cluster, :node1, volume, path)
    vol_id = volume_id(cluster, volume)

    holder = start_holder(cluster, :node1)
    {:ok, %{file_id: ^file_id, claim_id: pin_id}} = pin(cluster, :node1, volume, path, holder)

    try do
      :ok =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core, :rename_file, [volume, path, renamed])

      :ok = PeerCluster.rpc(cluster, :node3, NeonFS.Core, :delete_file, [volume, renamed])

      assert {:ok, %{detached: true, pinned_claim_ids: ids}} =
               PeerCluster.rpc(cluster, :node2, FileIndex, :get, [vol_id, file_id])

      assert pin_id in ids

      :ok = PeerCluster.rpc(cluster, :node1, NeonFS.Core, :unpin_file, [pin_id])

      assert :ok =
               wait_until(fn -> file_gone_everywhere?(cluster, vol_id, file_id) end,
                 timeout: 5_000
               )
    after
      Agent.stop(holder, :normal, 1_000)
    end
  end

  ## Helpers

  defp pin(cluster, node_name, volume, path, holder) do
    PeerCluster.rpc(cluster, node_name, NeonFS.Core, :pin_file, [volume, path, holder])
  end

  defp unique_volume(prefix) do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end

  defp write_file_and_get_id(cluster, node_name, volume, path) do
    {:ok, %{id: id}} =
      PeerCluster.rpc(cluster, node_name, NeonFS.Core, :write_file_streamed, [
        volume,
        path,
        ["payload"]
      ])

    id
  end

  defp volume_id(cluster, volume) do
    {:ok, %{id: id}} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core, :get_volume, [volume])

    id
  end

  defp start_holder(cluster, node_name) do
    {:ok, pid} = PeerCluster.rpc(cluster, node_name, Agent, :start, [&Map.new/0])
    pid
  end

  defp file_gone_everywhere?(cluster, vol_id, file_id) do
    Enum.all?([:node1, :node2, :node3], fn node ->
      match?(
        {:error, :not_found},
        PeerCluster.rpc(cluster, node, FileIndex, :get, [vol_id, file_id])
      )
    end)
  end
end
