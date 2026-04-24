defmodule NeonFS.Integration.ChunkerParityTest do
  @moduledoc """
  Peer-cluster parity test for the two streaming-write paths (#479):

    1. **Co-located path** — `NeonFS.Core.write_file_streamed/4`
       chunks and stores on the same core node.
    2. **Interface-side path** — `NeonFS.Client.ChunkWriter.write_file_stream/4`
       chunks on the caller (node2) and pushes each chunk to node1
       over the TLS data plane, followed by
       `NeonFS.Core.commit_chunks/4` to finalise.

  Both paths feed the same `Enumerable.t()` through
  `NeonFS.Client.Chunker.Native`, so they must emit the identical
  ordered hash list. This test asserts that invariant across a handful
  of representative input sizes and strategies, guarding against
  future drift in either call-site that would otherwise only surface
  as data-corruption bugs.

  Volumes are created with compression / encryption disabled so the
  test is insensitive to #481's codec propagation — the hashes are
  computed by the chunker on plaintext bytes regardless.
  """

  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Client.{ChunkWriter, ServiceInfo}
  alias NeonFS.Integration.PeerCluster

  @moduletag timeout: 300_000
  # The whole test operates against node1 (core) and node2 (interface-side chunker);
  # node3 was only involved in the `wait_for_data_plane` mesh check. Two nodes are
  # enough to exercise the cross-node round-trip.
  @moduletag nodes: 2
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    init_data_transfer_cluster(cluster)
    %{}
  end

  describe "streaming-write paths produce identical chunk hashes" do
    test "parity holds across representative strategies and sizes", %{cluster: cluster} do
      node1_info = PeerCluster.get_node!(cluster, :node1)
      wait_for_pool(cluster, :node2, node1_info.node)
      wait_for_discovery(cluster, :node2)

      volume_name = "parity-vol-#{System.unique_integer([:positive])}"

      # Disable codec so the test is insensitive to #481's codec
      # propagation work — chunker output is defined on plaintext
      # regardless, and the read path (which #481 fixes) is not
      # exercised here.
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

      # The two paths take different option keys:
      #   - write_file_streamed:     `:chunk_strategy` (atom or tuple)
      #   - ChunkWriter.write_file_stream: `:strategy` (string) + `:strategy_param`
      # Pair them up so the underlying `Chunker.Native.chunker_init/2`
      # call is given equivalent args on both sides.
      scenarios = [
        # Small single-chunk fixed: proves the trivial case.
        %{
          label: "fixed 4KiB × 1",
          streamed: {:fixed, 4_096},
          writer: ["fixed", 4_096],
          size: 4_096
        },
        # Multi-chunk fixed: boundary behaviour matches between paths.
        %{
          label: "fixed 4KiB × 3",
          streamed: {:fixed, 4_096},
          writer: ["fixed", 4_096],
          size: 3 * 4_096
        },
        # Larger fixed: exercises larger payload through the TLS pipe.
        %{
          label: "fixed 64KiB × 4",
          streamed: {:fixed, 65_536},
          writer: ["fixed", 65_536],
          size: 4 * 65_536
        },
        # "single" emits exactly one chunk regardless of feed shape.
        %{
          label: "single × 50KiB",
          streamed: :single,
          writer: ["single", 0],
          size: 50_000
        }
      ]

      for %{label: label, streamed: streamed_strat, writer: [w_strat, w_param], size: size} <-
            scenarios do
        payload = :crypto.strong_rand_bytes(size)
        # Feed as 5 roughly-equal segments so chunker_feed boundaries
        # differ from chunk boundaries on both paths.
        segment_size = max(div(size, 5), 1)
        segments = chunks_of(payload, segment_size)

        # Path A — co-located chunking on node1.
        path_a = "/parity/#{:erlang.phash2(label)}/streamed.bin"

        {:ok, meta_a} =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core, :write_file_streamed, [
            volume_name,
            path_a,
            segments,
            [chunk_strategy: streamed_strat]
          ])

        # Path B — chunking on node2, pushing to node1 over TLS,
        # finalising via commit_chunks on node1.
        path_b = "/parity/#{:erlang.phash2(label)}/chunk-writer.bin"

        chunk_writer_opts = [
          target_node: node1_info.node,
          drive_id: "default",
          strategy: w_strat,
          strategy_param: w_param
        ]

        {:ok, refs} =
          PeerCluster.rpc(cluster, :node2, ChunkWriter, :write_file_stream, [
            volume_name,
            path_b,
            segments,
            chunk_writer_opts
          ])

        %{hashes: hashes, locations: locations, total_size: total_size} =
          ChunkWriter.chunk_refs_to_commit_opts(refs)

        {:ok, meta_b} =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core, :commit_chunks, [
            volume_name,
            path_b,
            hashes,
            [total_size: total_size, locations: locations]
          ])

        assert meta_a.chunks == meta_b.chunks,
               "chunk-hash parity violated for #{label} " <>
                 "(co-located=#{length(meta_a.chunks)}, " <>
                 "interface-side=#{length(meta_b.chunks)})"

        assert meta_a.size == meta_b.size,
               "file size parity violated for #{label} " <>
                 "(co-located=#{meta_a.size}, interface-side=#{meta_b.size})"
      end
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

  defp chunks_of(binary, size) when byte_size(binary) <= size, do: [binary]

  defp chunks_of(binary, size) do
    <<head::binary-size(size), rest::binary>> = binary
    [head | chunks_of(rest, size)]
  end
end
