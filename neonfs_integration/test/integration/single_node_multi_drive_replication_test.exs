defmodule NeonFS.Integration.SingleNodeMultiDriveReplicationTest do
  @moduledoc """
  End-to-end peer-cluster verification of #1032 — a single node with two
  drives satisfies `replicate:2` by placing each copy on a distinct
  drive, and `volume create --replicas 2` is accepted without
  `--allow-under-replicated` once the drive count covers the factor.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.{ChunkIndex, MetadataStateMachine}
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 180_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :per_test
  @moduletag :integration

  setup %{cluster: cluster} do
    :ok = cluster_init_idempotent(cluster, :node1, "multi-drive")

    :ok = wait_for_cluster_stable(cluster)

    drive_path = Path.join([cluster.data_dir, "node1", "drive1"])
    File.mkdir_p!(drive_path)

    {:ok, _drive} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_add_drive, [
        %{"id" => "drive1", "path" => drive_path, "tier" => "hot"}
      ])

    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :local_query, [
                 &MetadataStateMachine.get_drives/1
               ]) do
            {:ok, drives} -> map_size(drives) >= 2
            _ -> false
          end
        end,
        timeout: 15_000
      )

    :ok
  end

  test "replicate:2 places both copies on distinct drives of one node", %{cluster: cluster} do
    # The create succeeds without `--allow-under-replicated`: the gate
    # now counts drives (two) rather than core nodes (one).
    assert {:ok, _volume} =
             PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
               "vol",
               %{"durability" => "replicate:2"}
             ])

    {:ok, file} =
      PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
        "vol",
        "/f.bin",
        :crypto.strong_rand_bytes(64)
      ])

    assert [hash | _] = file.chunks
    volume_id = file.volume_id

    :ok =
      wait_until(
        fn -> distinct_drive_count(cluster, volume_id, hash) == 2 end,
        timeout: 15_000
      )

    {:ok, chunk_meta} =
      PeerCluster.rpc(cluster, :node1, ChunkIndex, :get, [volume_id, hash])

    drive_ids = chunk_meta.locations |> Enum.map(& &1.drive_id) |> Enum.uniq()
    nodes = chunk_meta.locations |> Enum.map(& &1.node) |> Enum.uniq()

    assert length(drive_ids) == 2,
           "expected 2 distinct drives, got #{inspect(chunk_meta.locations)}"

    assert nodes == [PeerCluster.get_node!(cluster, :node1).node],
           "both replicas should be on the single node, got #{inspect(nodes)}"
  end

  defp distinct_drive_count(cluster, volume_id, hash) do
    case PeerCluster.rpc(cluster, :node1, ChunkIndex, :get, [volume_id, hash]) do
      {:ok, chunk_meta} ->
        chunk_meta.locations |> Enum.map(& &1.drive_id) |> Enum.uniq() |> length()

      _ ->
        0
    end
  end
end
