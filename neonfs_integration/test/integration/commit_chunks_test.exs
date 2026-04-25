defmodule NeonFS.Integration.CommitChunksTest do
  @moduledoc """
  Peer-cluster integration test for `NeonFS.Core.commit_chunks/4` —
  the write-side counterpart to `read_file_refs/3` introduced in #410.

  Simulates the interface-side chunking flow established by the #408
  design decision: from `node2`, chunks are shipped directly to
  `node1`'s blob store via `Router.data_call(:put_chunk, …)` (the same
  data-plane path `NeonFS.Client.ChunkWriter` will use when it lands).
  A `commit_chunks/4` RPC on `node1` then materialises `ChunkIndex`
  entries for those hashes and lays down the `FileIndex` entry. Finally
  `node2` reads the file back via `read_file_stream/3` and asserts the
  bytes round-trip byte-identically.

  This test complements the in-package unit tests in
  `neonfs_core/test/neon_fs/core/commit_chunks_test.exs` — those
  exercise the reconcile / commit logic on a single node; this one
  proves the cross-node happy path works over the real data plane.
  """

  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Integration.PeerCluster

  @moduletag timeout: 300_000
  # Two nodes are enough: node1 is the core that accepts put_chunk + commit_chunks,
  # node2 is the interface-side caller driving the data plane. node3 was only
  # involved in the `wait_for_data_plane` mesh check.
  @moduletag nodes: 2
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    init_data_transfer_cluster(cluster)
    %{}
  end

  describe "commit_chunks/4 cross-node round-trip" do
    test "node2 writes chunks to node1, commits via RPC, reads file back", %{cluster: cluster} do
      volume_name = "commit-chunks-vol-#{System.unique_integer([:positive])}"

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [volume_name, %{}])

      assert_eventually timeout: 10_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
               volume_name
             ]) do
          {:ok, _vol} -> true
          _ -> false
        end
      end

      node1_info = PeerCluster.get_node!(cluster, :node1)
      wait_for_pool(cluster, :node2, node1_info.node)

      # Chunk a synthetic multi-KiB payload locally on node2. A real
      # `ChunkWriter` would feed this stream through the FastCDC
      # chunker; here we just pick two deterministic pieces to verify
      # the commit path handles multiple chunks in order.
      chunk_a = :binary.copy(<<0x5A>>, 3_000)
      chunk_b = :binary.copy(<<0xA5>>, 4_500)

      hash_a = :crypto.hash(:sha256, chunk_a)
      hash_b = :crypto.hash(:sha256, chunk_b)

      for {hash, data} <- [{hash_a, chunk_a}, {hash_b, chunk_b}] do
        assert :ok ==
                 PeerCluster.rpc(cluster, :node2, NeonFS.Client.Router, :data_call, [
                   node1_info.node,
                   :put_chunk,
                   [hash: hash, volume_id: "default", write_id: nil, tier: "hot", data: data]
                 ])
      end

      locations = %{
        hash_a => [%{node: node1_info.node, drive_id: "default", tier: :hot}],
        hash_b => [%{node: node1_info.node, drive_id: "default", tier: :hot}]
      }

      total_size = byte_size(chunk_a) + byte_size(chunk_b)
      path = "/commit-chunks/cross-node.bin"

      assert {:ok, file_meta} =
               PeerCluster.rpc(cluster, :node1, NeonFS.Core, :commit_chunks, [
                 volume_name,
                 path,
                 [hash_a, hash_b],
                 [total_size: total_size, locations: locations]
               ])

      assert file_meta.size == total_size
      assert file_meta.chunks == [hash_a, hash_b]

      # Read back from node2 — node2 is a core node so it has
      # `NeonFS.Core` directly; the underlying chunk fetches still
      # traverse the data plane because the chunks live on node1.
      # Buffered variant keeps the result serialisable across RPC.
      assert {:ok, assembled} =
               PeerCluster.rpc(cluster, :node2, NeonFS.Core, :read_file, [volume_name, path])

      assert assembled == chunk_a <> chunk_b
    end

    test "commit_chunks returns {:missing_chunk, hash} when a hash was never written",
         %{cluster: cluster} do
      volume_name = "commit-chunks-missing-vol-#{System.unique_integer([:positive])}"

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [volume_name, %{}])

      assert_eventually timeout: 10_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
               volume_name
             ]) do
          {:ok, _vol} -> true
          _ -> false
        end
      end

      node1_info = PeerCluster.get_node!(cluster, :node1)
      wait_for_pool(cluster, :node2, node1_info.node)

      real = :binary.copy(<<0x01>>, 1_024)
      real_hash = :crypto.hash(:sha256, real)
      phantom_hash = :crypto.hash(:sha256, "never-written-across-nodes")

      assert :ok ==
               PeerCluster.rpc(cluster, :node2, NeonFS.Client.Router, :data_call, [
                 node1_info.node,
                 :put_chunk,
                 [
                   hash: real_hash,
                   volume_id: "default",
                   write_id: nil,
                   tier: "hot",
                   data: real
                 ]
               ])

      locations = %{
        real_hash => [%{node: node1_info.node, drive_id: "default", tier: :hot}],
        phantom_hash => [%{node: node1_info.node, drive_id: "default", tier: :hot}]
      }

      assert {:error, {:missing_chunk, ^phantom_hash}} =
               PeerCluster.rpc(cluster, :node1, NeonFS.Core, :commit_chunks, [
                 volume_name,
                 "/commit-chunks/missing.bin",
                 [real_hash, phantom_hash],
                 [total_size: byte_size(real) + 1, locations: locations]
               ])
    end
  end

  ## Private helpers — copied from DataTransferTest because the
  ## helpers live in that test file, not in ClusterCase.

  defp init_data_transfer_cluster(cluster) do
    :ok = init_multi_node_cluster(cluster)
    wait_for_data_plane(cluster)
    :ok
  end

  defp wait_for_data_plane(cluster) do
    node_names = Enum.map(cluster.nodes, & &1.name)

    assert_eventually timeout: 30_000 do
      Enum.all?(node_names, fn node_name ->
        node_has_all_peer_pools?(cluster, node_name)
      end)
    end
  end

  defp node_has_all_peer_pools?(cluster, node_name) do
    other_nodes =
      cluster.nodes
      |> Enum.map(& &1.name)
      |> List.delete(node_name)
      |> Enum.map(&PeerCluster.get_node!(cluster, &1).node)

    Enum.all?(other_nodes, fn peer_node ->
      match?(
        {:ok, _pool},
        PeerCluster.rpc(cluster, node_name, NeonFS.Transport.PoolManager, :get_pool, [peer_node])
      )
    end)
  end
end
