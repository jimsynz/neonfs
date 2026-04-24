defmodule NeonFS.Integration.ChunkWriterTest do
  @moduledoc """
  Peer-cluster integration smoke test for
  `NeonFS.Client.ChunkWriter.write_file_stream/4` (landed in #450).

  Unit tests against a mocked `Router` / `Discovery` cover the
  branching in `do_stream` / `process_emitted` / `abort_written`. This
  test proves the real data-plane wiring: interface-side chunking on
  `node2` pushes chunks to `node1` over TLS via `Router.data_call`, a
  `commit_chunks/4` RPC on `node1` materialises the `ChunkIndex` and
  `FileIndex` entries, and `node2` reads the file back byte-identically.

  Mirrors the shape of `NeonFS.Integration.CommitChunksTest` from
  #454 — the difference is that chunking / `put_chunk` is driven by
  `ChunkWriter.write_file_stream/4` rather than hand-rolled calls to
  `Router.data_call`.
  """

  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Client.ChunkWriter
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

  describe "write_file_stream/4 cross-node round-trip" do
    test "node2 chunks locally, writes to node1, commits, reads back", %{cluster: cluster} do
      volume_name = "chunk-writer-vol-#{System.unique_integer([:positive])}"

      # Disable the default zstd compression: `put_chunk` with
      # `processing_volume_id` applies the volume's codec pipeline on
      # write, but `CommitChunks.create_chunk_meta/3` hard-codes
      # `compression: :none` on the resulting `ChunkMeta`. The codec
      # mismatch makes the read path hash-verify raw bytes against the
      # hash of compressed bytes → `:all_replicas_failed`. Tracked as a
      # known gap in the `CommitChunks` module docstring.
      volume_opts = %{compression: %{algorithm: :none, level: 0, min_size: 0}}

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

      chunk_size = 262_144
      payload = :crypto.strong_rand_bytes(6 * chunk_size)
      expected_chunk_count = div(byte_size(payload), chunk_size)

      # Feed the stream as several segments so the incremental chunker
      # is exercised across feed boundaries rather than a single blob.
      segments =
        payload
        |> chunks_of(128_000)

      path = "/chunk-writer/round-trip.bin"

      assert {:ok, refs} =
               PeerCluster.rpc(cluster, :node2, ChunkWriter, :write_file_stream, [
                 volume_name,
                 path,
                 segments,
                 [
                   target_node: node1_info.node,
                   drive_id: "default",
                   strategy: "fixed",
                   strategy_param: chunk_size
                 ]
               ])

      assert length(refs) == expected_chunk_count

      Enum.each(refs, fn ref ->
        assert byte_size(ref.hash) == 32
        assert ref.locations == [%{node: node1_info.node, drive_id: "default", tier: :hot}]
        assert ref.size == chunk_size
      end)

      %{hashes: hashes, locations: locations, total_size: total_size} =
        ChunkWriter.chunk_refs_to_commit_opts(refs)

      assert total_size == byte_size(payload)

      assert {:ok, file_meta} =
               PeerCluster.rpc(cluster, :node1, NeonFS.Core, :commit_chunks, [
                 volume_name,
                 path,
                 hashes,
                 [total_size: total_size, locations: locations]
               ])

      assert file_meta.size == total_size
      assert file_meta.chunks == hashes

      # Read back from node2 — node2 is a core node but the chunks
      # live on node1, so the fetches traverse the real data plane.
      # The buffered `read_file/3` variant keeps the result
      # serialisable across RPC. See Codebase-Patterns.md §Testing.
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

  defp wait_for_pool(cluster, from_node, target_node) do
    assert_eventually timeout: 30_000 do
      case PeerCluster.rpc(cluster, from_node, NeonFS.Transport.PoolManager, :get_pool, [
             target_node
           ]) do
        {:ok, _pool} -> true
        _ -> false
      end
    end
  end

  defp chunks_of(binary, size) when byte_size(binary) <= size, do: [binary]

  defp chunks_of(binary, size) do
    <<head::binary-size(size), rest::binary>> = binary
    [head | chunks_of(rest, size)]
  end
end
