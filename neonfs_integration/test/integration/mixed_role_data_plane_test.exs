defmodule NeonFS.Integration.MixedRoleDataPlaneTest do
  @moduledoc """
  Smoke test for `NeonFS.TestSupport.ClusterCase.init_mixed_role_cluster/2`
  (#524).

  Spawns a 2-peer cluster — one `:neonfs_core`, one `:neonfs_s3` —
  initialises the mixed-role data plane (cluster_init on the core peer,
  `Cluster.Join.join_cluster_rpc/3` from the interface peer with
  `type: :s3`, then explicit `PoolManager.ensure_pool/2`) and proves that
  the interface peer can ship chunks to the core peer over the real TLS
  data plane.

  Asserts:

    * `NeonFS.Client.ChunkWriter.write_file_stream/4` from the S3 peer
      succeeds against the core peer's data-plane `Listener` — exercises
      `Router.data_call(:put_chunk, …)` end-to-end across mixed roles;
    * `NeonFS.Core.commit_chunks/4` on the core peer materialises the
      `FileIndex` entry with the right size;
    * `NeonFS.Core.read_file/2` on the core peer reads the bytes back
      identically to what was streamed in.

  This is the prerequisite the parent `#499` peak-RSS test needs before
  it can land — substantive RSS bounds and cross-interface readback
  (S3-write-then-WebDAV-read) are out of scope here. Uses plain
  `ExUnit.Case` rather than `ClusterCase` because the per-test default
  cluster setup would conflict with the mixed-role spawn.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Client.ChunkWriter
  alias NeonFS.Integration.{ClusterCase, PeerCluster}

  @moduletag timeout: 180_000

  test "S3 peer ships chunks to core peer over TLS data plane and reads back" do
    cluster =
      PeerCluster.start_cluster!(2,
        roles: %{
          node1: [:neonfs_core],
          node2: [:neonfs_s3]
        }
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)

    :ok = ClusterCase.init_mixed_role_cluster(cluster, name: "mixed-role-smoke")

    # Disable the default zstd compression: `put_chunk` with
    # `processing_volume_id` applies the volume's codec pipeline on
    # write, but `CommitChunks.create_chunk_meta/3` hard-codes
    # `compression: :none` on the resulting `ChunkMeta`. The codec
    # mismatch makes the read path hash-verify raw bytes against the
    # hash of compressed bytes → `:all_replicas_failed`. Same workaround
    # as `chunk_writer_test.exs`; tracked in the `CommitChunks` module
    # docstring.
    volume_name = "mr-vol-#{System.unique_integer([:positive])}"
    volume_opts = %{compression: %{algorithm: :none, level: 0, min_size: 0}}

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume_name,
        volume_opts
      ])

    :ok =
      ClusterCase.wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
                 volume_name
               ]) do
            {:ok, _vol} -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

    chunk_size = 65_536
    payload = :crypto.strong_rand_bytes(2 * chunk_size)
    segments = chunks_of(payload, 8_192)
    path = "/mixed-role/round-trip.bin"

    core_node = PeerCluster.get_node!(cluster, :node1).node

    assert {:ok, refs} =
             PeerCluster.rpc(cluster, :node2, ChunkWriter, :write_file_stream, [
               volume_name,
               path,
               segments,
               [
                 target_node: core_node,
                 drive_id: "default",
                 strategy: "fixed",
                 strategy_param: chunk_size
               ]
             ])

    assert length(refs) == div(byte_size(payload), chunk_size)

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

    assert {:ok, assembled} =
             PeerCluster.rpc(cluster, :node1, NeonFS.Core, :read_file, [volume_name, path])

    assert assembled == payload
  end

  defp chunks_of(binary, size) when byte_size(binary) <= size, do: [binary]

  defp chunks_of(binary, size) do
    <<head::binary-size(size), rest::binary>> = binary
    [head | chunks_of(rest, size)]
  end
end
