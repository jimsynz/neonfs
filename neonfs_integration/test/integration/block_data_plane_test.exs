defmodule NeonFS.Integration.BlockDataPlaneTest do
  @moduledoc """
  The device's IO boundary, end to end across nodes.

  A block node resolves the device's extents through core and then moves
  their bytes itself, over the TLS data plane. Unit tests on either side
  cover the halves — the extent arithmetic against a stubbed cluster, the
  refs and the verified commit against a local one. What only a peer cluster
  can show is the two composing: `node2` asking `node1` what an extent is,
  pushing a rewritten extent to `node1`'s blob store over the data plane,
  and `node1` verifying that claim before publishing a map that points at
  it.

  A block volume is uncompressed by construction, which is what makes this
  possible at all: a compressed chunk's stored bytes do not hash to its id,
  so the data plane cannot serve it and `read_refs` says so for the caller
  to route around.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Client.{ChunkReader, ChunkWriter}
  alias NeonFS.Core.BlockBacking
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 2
  @moduletag :integration

  @chunk 131_072
  @block 4096
  @device_extents 4

  setup %{cluster: cluster} do
    volume = "block-data-plane-#{System.unique_integer([:positive])}"

    :ok =
      init_multi_node_cluster(cluster,
        volumes: [
          {volume,
           %{
             type: :block,
             max_size: @device_extents * @chunk,
             durability: %{type: :replicate, factor: 1, min_copies: 1}
           }}
        ]
      )

    wait_for_data_plane(cluster)

    device_path = rpc(cluster, :node1, BlockBacking, :device_path, [])
    {:ok, volume: volume, device_path: device_path}
  end

  test "node2 writes an extent over the data plane and node1 publishes it", %{
    cluster: cluster,
    volume: volume,
    device_path: device_path
  } do
    node1 = PeerCluster.get_node!(cluster, :node1).node
    wait_for_pool(cluster, :node2, node1)

    # A fresh device is all holes, and core says so rather than omitting
    # them — the caller has to emit their zeroes.
    assert {:ok, %{chunk_bytes: @chunk, extents: [hole]}} =
             rpc(cluster, :node1, BlockBacking, :read_refs, [volume, device_path, 0, @chunk])

    assert hole.target == :hole
    assert hole.width == @chunk
    assert hole.locations == []

    payload = :crypto.strong_rand_bytes(@chunk)

    assert {:ok, [ref]} =
             rpc(cluster, :node2, ChunkWriter, :write_chunks, [
               volume,
               [payload],
               [target_node: node1, drive_id: "default"]
             ])

    assert ref.size == @chunk
    assert ref.locations == [%{node: node1, drive_id: "default", tier: :hot}]

    %{locations: locations, chunk_codecs: codecs} = ChunkWriter.chunk_refs_to_commit_opts([ref])

    assert {:ok, %{chunks_published: 1}} =
             rpc(cluster, :node1, BlockBacking, :commit_written, [
               volume,
               device_path,
               [{0, ref.hash}],
               [locations: locations, chunk_codecs: codecs, expect: [{0, :hole}]]
             ])

    # The map now names the chunk, and the refs carry the locations a reader
    # dials — which is the whole point of the boundary.
    assert {:ok, %{extents: [written]}} =
             rpc(cluster, :node1, BlockBacking, :read_refs, [volume, device_path, 0, @chunk])

    assert written.target == {:chunk, ref.hash}
    assert written.locations != []
    assert written.compression == :none
    refute written.encrypted

    # node2 pulls the extent back over the data plane and gets exactly what
    # it pushed. `read_refs` came from node1, the bytes did not.
    assert {:ok, ^payload} =
             rpc(cluster, :node2, ChunkReader, :fetch_chunk, [volume, written])

    assert {:ok, ^payload} =
             rpc(cluster, :node1, BlockBacking, :read, [volume, device_path, 0, @chunk])
  end

  # A writer's report is the very thing in doubt when a chunk turns out to be
  # missing, so the commit checks it rather than trusting it. Publishing a map
  # over data that is not there leaves a read with no correct answer to give.
  test "a commit naming a chunk nobody holds publishes nothing", %{
    cluster: cluster,
    volume: volume,
    device_path: device_path
  } do
    node1 = PeerCluster.get_node!(cluster, :node1).node
    absent = :crypto.hash(:sha256, "never pushed anywhere")

    assert {:error, {:missing_chunk, ^absent}} =
             rpc(cluster, :node1, BlockBacking, :commit_written, [
               volume,
               device_path,
               [{1, absent}],
               [locations: %{absent => [%{node: node1, drive_id: "default", tier: :hot}]}]
             ])

    assert {:ok, %{extents: [ref]}} =
             rpc(cluster, :node1, BlockBacking, :read_refs, [volume, device_path, @chunk, @block])

    assert ref.target == :hole
  end

  defp rpc(cluster, node, module, function, args) do
    PeerCluster.rpc(cluster, node, module, function, args, 120_000)
  end
end
