defmodule NeonFS.Integration.PerVolumeMetadataTest do
  @moduledoc """
  End-to-end peer-cluster verification of #794 — per-volume metadata
  durability.

  Asserts that creating a volume with `replicate: factor=N` causes the
  volume's bootstrap entry to record N drive locations and the volume's
  **root segment chunk** lands on each of those drives' on-disk
  `blobs/` trees.

  ## Cluster shape

  Three core peers, one drive per peer. The `replicate:N` cases pick
  exactly N of the three available drives; chunks for `replicate:1`
  land on the single bootstrapping drive, `replicate:2` lands on two
  of three, and `replicate:3` lands on every drive.

  ## Out of scope

  - Index-tree-node fan-out: `Volume.MetadataWriter.do_apply_index_op/5`
    currently writes the index-tree chunks (FileIndex / ChunkIndex /
    StripeIndex pages) through a single `pick_store_handle/2` instead
    of the multi-replica `replicate_segment` path. That's tracked in
    #903 — once it lands the assertions in this test extend to cover
    every chunk hash referenced from the volume's index-tree roots.
  - Drive-evacuation re-replication (#793): drains belong with that
    sub-issue's runner; this test verifies the **write-time** durability
    contract only.
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag cluster_mode: :per_test
  @moduletag :integration

  setup %{cluster: cluster} do
    :ok =
      init_multi_node_cluster(cluster,
        name: "per-volume-metadata-test",
        volumes: [
          {"vol-n1",
           %{
             durability: %{type: :replicate, factor: 1, min_copies: 1},
             compression: %{algorithm: :none}
           }},
          {"vol-n2",
           %{
             durability: %{type: :replicate, factor: 2, min_copies: 1},
             compression: %{algorithm: :none}
           }},
          {"vol-n3",
           %{
             durability: %{type: :replicate, factor: 3, min_copies: 2},
             compression: %{algorithm: :none}
           }}
        ]
      )

    :ok
  end

  describe "root segment chunk replicates to each volume's configured factor" do
    test "replicate:1 root segment lands on exactly one drive", %{cluster: cluster} do
      assert_root_segment_replication(cluster, "vol-n1", 1)
    end

    test "replicate:2 root segment lands on exactly two drives", %{cluster: cluster} do
      assert_root_segment_replication(cluster, "vol-n2", 2)
    end

    test "replicate:3 root segment lands on all three drives", %{cluster: cluster} do
      assert_root_segment_replication(cluster, "vol-n3", 3)
    end
  end

  ## Helpers

  defp assert_root_segment_replication(cluster, volume_name, expected_factor) do
    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [volume_name])

    # Touch each volume so the writer actually emits a root segment
    # chunk into the index trees — without a write nothing replicates.
    {:ok, _file} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core, :write_file_at, [
        volume_name,
        "/seed.bin",
        0,
        :crypto.strong_rand_bytes(64),
        []
      ])

    %{root_chunk_hash: root_hash, drive_locations: locations} =
      wait_for_root_chunk_hash(cluster, volume.id)

    # The bootstrap entry's `drive_locations` should record exactly
    # `expected_factor` drives. Anything else means the durability
    # contract was already broken at provisioning time.
    assert length(locations) == expected_factor,
           "volume #{volume_name} should have #{expected_factor} drive_locations, " <>
             "got #{length(locations)}: #{inspect(locations)}"

    # Each recorded drive should hold the root segment chunk on disk.
    for %{node: node_atom, drive_id: drive_id} <- locations do
      drive_path = drive_path_for(cluster, node_atom, drive_id)

      assert chunk_on_disk?(drive_path, root_hash),
             "root segment #{Base.encode16(root_hash, case: :lower)} for " <>
               "#{volume_name} not found on drive #{drive_id} at #{drive_path}/blobs"
    end
  end

  defp wait_for_root_chunk_hash(cluster, volume_id) do
    :ok =
      wait_until(
        fn ->
          case Map.get(volume_roots(cluster), volume_id) do
            %{root_chunk_hash: hash} when is_binary(hash) and byte_size(hash) > 0 -> true
            _ -> false
          end
        end,
        timeout: 30_000
      )

    Map.fetch!(volume_roots(cluster), volume_id)
  end

  defp volume_roots(cluster) do
    {:ok, roots} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :local_query, [
        &MetadataStateMachine.get_volume_roots/1
      ])

    roots
  end

  defp drive_path_for(cluster, node_atom, drive_id) do
    {:ok, drives_map} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :local_query, [
        &MetadataStateMachine.get_drives/1
      ])

    drive =
      drives_map
      |> Map.values()
      |> Enum.find(fn d -> d.node == node_atom and d.drive_id == drive_id end)

    case drive do
      %{path: path} when is_binary(path) ->
        path

      _ ->
        # Fallback: the test harness lays out drives at
        # `<cluster.data_dir>/<peer_name>/blobs/`. Each peer alias is
        # `nodeN`, so we can reconstruct the path from cluster state
        # without an explicit RPC.
        peer_name =
          cluster.nodes
          |> Enum.find(fn ni -> ni.node == node_atom end)
          |> Map.fetch!(:name)
          |> Atom.to_string()

        Path.join([cluster.data_dir, peer_name, "blobs"])
    end
  end

  defp chunk_on_disk?(drive_path, hash) do
    hex = Base.encode16(hash, case: :lower)

    [drive_path, "blobs", "**", "#{hex}.*"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.any?()
  end
end
