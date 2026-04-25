defmodule NeonFS.Integration.CommitChunksCodecTest do
  @moduledoc """
  Peer-cluster regression test for #481 — `CommitChunks.create_chunk_meta/3`
  must stamp the codec (compression + encryption) that `put_chunk`
  actually used, not the hard-coded `compression: :none, crypto: nil`
  shape it wrote before this change.

  Without the fix, a volume with zstd compression (the default) would
  round-trip through interface-side chunking as follows:

    1. `ChunkWriter.write_file_stream/4` on node2 ships chunks to node1.
    2. `put_chunk` on node1 compresses and stores them under the zstd
       codec filename suffix.
    3. `commit_chunks/4` creates `ChunkMeta` with `compression: :none`.
    4. `read_file` on node2 computes the raw-suffix path and fails to
       find the file, surfacing `{:error, :all_replicas_failed}`.

  This test asserts the post-fix behaviour: the committed `ChunkMeta`
  carries `compression: :zstd`, the stored blob is smaller than the
  plaintext (compression actually happened), and the round-trip
  produces byte-identical output.
  """

  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Client.{ChunkWriter, ServiceInfo}
  alias NeonFS.Integration.PeerCluster

  @moduletag timeout: 300_000
  # Two nodes are enough: node1 is the core that accepts put_chunk + commit_chunks,
  # node2 is the interface-side chunker driving the data plane. node3 was only
  # involved in the `wait_for_data_plane` mesh check.
  @moduletag nodes: 2
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    init_data_transfer_cluster(cluster)
    %{}
  end

  describe "ChunkWriter → CommitChunks on a zstd-compressed volume" do
    test "round-trip preserves bytes and the ChunkMeta records :zstd", %{cluster: cluster} do
      volume_name = "commit-chunks-codec-vol-#{System.unique_integer([:positive])}"

      # Force-enable zstd at min_size 0 so every chunk is compressed
      # even if the fixed chunker emits small blocks.
      volume_opts = %{compression: %{algorithm: :zstd, level: 3, min_size: 0}}

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          volume_name,
          volume_opts
        ])

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
      wait_for_discovery(cluster, :node2)

      # Highly-compressible payload so we can assert stored_size <<
      # original_size and know the codec was genuinely applied.
      chunk_size = 32_768
      payload = :binary.copy(<<0x42>>, 2 * chunk_size)

      path = "/commit-chunks-codec/round-trip.bin"

      assert {:ok, refs} =
               PeerCluster.rpc(cluster, :node2, ChunkWriter, :write_file_stream, [
                 volume_name,
                 path,
                 [payload],
                 [
                   target_node: node1_info.node,
                   drive_id: "default",
                   strategy: "fixed",
                   strategy_param: chunk_size
                 ]
               ])

      assert Enum.all?(refs, &(&1.codec.compression == :zstd))
      assert Enum.all?(refs, &(&1.codec.original_size == chunk_size))

      %{hashes: hashes, locations: locations, chunk_codecs: chunk_codecs, total_size: total_size} =
        ChunkWriter.chunk_refs_to_commit_opts(refs)

      assert Map.keys(chunk_codecs) |> length() == length(Enum.uniq(hashes))

      assert {:ok, file_meta} =
               PeerCluster.rpc(cluster, :node1, NeonFS.Core, :commit_chunks, [
                 volume_name,
                 path,
                 hashes,
                 [
                   total_size: total_size,
                   locations: locations,
                   chunk_codecs: chunk_codecs
                 ]
               ])

      assert file_meta.size == byte_size(payload)

      # Inspect the committed ChunkMeta directly: compression atom and
      # a stored_size strictly smaller than original_size proves the
      # fix propagated the codec info and the blob really was
      # compressed.
      for hash <- Enum.uniq(hashes) do
        assert {:ok, chunk_meta} =
                 PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :get, [hash])

        assert chunk_meta.compression == :zstd
        assert chunk_meta.crypto == nil
        assert chunk_meta.original_size == chunk_size
        assert chunk_meta.stored_size < chunk_meta.original_size
      end

      assert {:ok, assembled} =
               PeerCluster.rpc(cluster, :node2, NeonFS.Core, :read_file, [volume_name, path])

      assert assembled == payload
    end
  end

  ## Private helpers

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

  defp wait_for_discovery(cluster, node_name) do
    # See Codebase-Patterns.md §Testing: the peer-cluster harness
    # doesn't populate NeonFS.Client.Connection.bootstrap_nodes, so
    # Discovery's cache stays empty until we seed it. #482 tracks
    # fixing this at the harness level.
    services = PeerCluster.rpc(cluster, node_name, NeonFS.Core.ServiceRegistry, :list, [])
    infos = Enum.map(services, &ServiceInfo.from_map/1)
    PeerCluster.rpc(cluster, node_name, NeonFS.Client.Connection, :sync_services, [infos])
    PeerCluster.rpc(cluster, node_name, NeonFS.Client.Discovery, :refresh, [])

    assert_eventually timeout: 30_000 do
      case PeerCluster.rpc(cluster, node_name, NeonFS.Client.Discovery, :get_core_nodes, []) do
        [_ | _] -> true
        _ -> false
      end
    end
  end
end
