defmodule NeonFS.Integration.ReconstructFromDiskTest do
  @moduledoc """
  Peer-cluster integration coverage for `cluster reconstruct-from-disk`
  (#840 / #788).

  The full disaster-recovery story (stop every node, wipe each
  Ra dir, restart, run reconstruction) layers a cluster restart on
  top of two reconstruction primitives that are independently
  testable; this test covers the two primitives against a real
  populated cluster:

    1. `--dry-run` walks the on-disk drive, decodes every root
       segment chunk, and reports the discovered drives + volumes
       without submitting anything to Ra.
    2. `--yes --overwrite-ra-state` submits the full set of
       `:register_drive` + `:register_volume_root` commands and
       leaves the bootstrap layer with the same `volume_roots`
       contents as before reconstruction ran.

  The Ra-wipe-restart half is captured separately as a follow-up;
  the on-disk walker + handler + Ra-submit path is the part that
  was missing end-to-end coverage.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 120_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :per_test
  @moduletag :integration

  setup %{cluster: cluster} do
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["dr-recon"])

    :ok = wait_for_cluster_stable(cluster)

    drive_path = Path.join([cluster.data_dir, "node1", "drive1"])
    File.mkdir_p!(drive_path)

    {:ok, _drive} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_add_drive, [
        %{"id" => "drive1", "path" => drive_path, "tier" => "hot"}
      ])

    # Wait for both drives to be in the bootstrap layer: `default`
    # from `cluster_init`, plus the `drive1` we just added. With
    # both registered, `replicate:2` volumes land their root
    # segments on both drives, which means the reconstruction
    # walker (which only reads the `:drives` app-env, see comment
    # below) finds the volume roots on `drive1`.
    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :local_query, [
                 &MetadataStateMachine.get_drives/1
               ]) do
            {:ok, drives} when is_map(drives) and map_size(drives) >= 2 -> true
            _ -> false
          end
        end,
        timeout: 30_000
      )

    # `Reconstruction` reads the drive list from app env, not from
    # `DriveRegistry` — that's the disaster contract: Ra may be gone,
    # so the local config is the only source of truth for which
    # paths to walk. Mirror the production deployment pattern by
    # populating the env after the drive is registered so the
    # reconstruction handler walks our test drive.
    :ok =
      PeerCluster.rpc(cluster, :node1, Application, :put_env, [
        :neonfs_core,
        :drives,
        [%{id: "drive1", path: drive_path, tier: :hot, capacity: 0}]
      ])

    %{cluster: cluster, drive_path: drive_path}
  end

  describe "cluster reconstruct-from-disk on a populated cluster" do
    test "--dry-run identifies every registered drive and volume root", %{cluster: cluster} do
      {volume_id_a, _hash_a} = create_volume_and_capture_root(cluster, "recon-vol-a")
      {volume_id_b, _hash_b} = create_volume_and_capture_root(cluster, "recon-vol-b")

      {:ok, summary} =
        PeerCluster.rpc(
          cluster,
          :node1,
          NeonFS.CLI.Handler,
          :handle_cluster_reconstruct_from_disk,
          [%{"dry_run" => true}]
        )

      assert summary.dry_run == true
      assert summary.commands_submitted == 0
      assert summary.commands_failed == []
      assert summary.volumes >= 2
      assert summary.drives >= 1
      assert summary.commands == summary.drives + summary.volumes

      ra_volume_ids = volume_roots(cluster) |> Map.keys() |> MapSet.new()
      assert volume_id_a in ra_volume_ids
      assert volume_id_b in ra_volume_ids
    end

    test "--yes --overwrite-ra-state preserves the existing volume_roots set",
         %{cluster: cluster} do
      {volume_id_a, hash_a} = create_volume_and_capture_root(cluster, "recon-apply-a")
      {volume_id_b, hash_b} = create_volume_and_capture_root(cluster, "recon-apply-b")

      pre_roots = volume_roots(cluster)
      assert Map.has_key?(pre_roots, volume_id_a)
      assert Map.has_key?(pre_roots, volume_id_b)

      {:ok, summary} =
        PeerCluster.rpc(
          cluster,
          :node1,
          NeonFS.CLI.Handler,
          :handle_cluster_reconstruct_from_disk,
          [%{"yes" => true, "overwrite_ra_state" => true}]
        )

      assert summary.dry_run == false
      assert summary.commands_submitted == summary.commands
      assert summary.commands_failed == []

      post_roots = volume_roots(cluster)
      assert post_roots[volume_id_a].root_chunk_hash == hash_a
      assert post_roots[volume_id_b].root_chunk_hash == hash_b
    end
  end

  ## Helpers

  defp create_volume_and_capture_root(cluster, volume_name) do
    # `replicate:2` puts a root-segment replica on every drive in
    # the bootstrap layer (`default` + `drive1`), so the
    # reconstruction walker (which only walks `drive1` per the
    # disaster contract) finds the chunk.
    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume_name,
        %{"durability" => "replicate:2"}
      ])

    volume_id = volume.id
    assert is_binary(volume_id)

    :ok =
      wait_until(
        fn ->
          case Map.get(volume_roots(cluster), volume_id) do
            %{root_chunk_hash: hash} when is_binary(hash) -> true
            _ -> false
          end
        end,
        timeout: 30_000
      )

    %{root_chunk_hash: hash} = Map.fetch!(volume_roots(cluster), volume_id)
    {volume_id, hash}
  end

  defp volume_roots(cluster) do
    {:ok, roots} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :local_query, [
        &MetadataStateMachine.get_volume_roots/1
      ])

    roots
  end
end
