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
      PeerCluster.rpc_until_ready(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, [
        "dr-recon"
      ])

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

    # Runs at the live (default) shard count: reconstruction recovers
    # per-shard identity from disk via `RootSegment.shard` (#1313).
    %{cluster: cluster, drive_path: drive_path}
  end

  describe "cluster reconstruct-from-disk on a populated cluster" do
    test "--dry-run identifies every registered drive and all shard roots", %{cluster: cluster} do
      {volume_id_a, roots_a} = create_volume_and_capture_roots(cluster, "recon-vol-a")
      {volume_id_b, _roots_b} = create_volume_and_capture_roots(cluster, "recon-vol-b")

      shards = shard_count(cluster)
      assert map_size(roots_a) == shards

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

      # One root command per shard per discovered volume — proving all N
      # shard roots are recovered from disk, not just shard 0 (#1313).
      assert summary.commands == summary.drives + shards * summary.volumes

      ra_volume_ids = volume_roots(cluster) |> Map.keys() |> MapSet.new()
      assert volume_id_a in ra_volume_ids
      assert volume_id_b in ra_volume_ids
    end

    test "--yes --overwrite-ra-state restores every shard root, including a diverged shard",
         %{cluster: cluster} do
      {volume_id_a, _} = create_volume_and_capture_roots(cluster, "recon-apply-a")
      {volume_id_b, _} = create_volume_and_capture_roots(cluster, "recon-apply-b")

      # Write a file so some of vol-a's shards diverge from the shared
      # empty root — exercises the "restore shard n by its recorded index"
      # path, not just the empty-chunk fill.
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "recon-apply-a",
          "/f.bin",
          :crypto.strong_rand_bytes(64)
        ])

      pre_a = shard_hashes(cluster, volume_id_a)
      pre_b = shard_hashes(cluster, volume_id_b)
      # The write diverged at least one shard from the shared empty root.
      assert pre_a |> Map.values() |> Enum.uniq() |> length() >= 2

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

      # Every shard root — diverged and empty alike — is restored to its
      # pre-reconstruction chunk hash.
      assert shard_hashes(cluster, volume_id_a) == pre_a
      assert shard_hashes(cluster, volume_id_b) == pre_b
    end
  end

  ## Helpers

  defp create_volume_and_capture_roots(cluster, volume_name) do
    # `replicate:2` puts a root-segment replica on every drive in
    # the bootstrap layer (`default` + `drive1`), so the
    # reconstruction walker (which only walks `drive1` per the
    # disaster contract) finds the chunk.
    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume_name,
        # `replicate:2` on a single-node test cluster lands metadata
        # replicas on `default` + `drive1`; chunk replication never
        # runs here so the #1015 under-replication refusal needs to
        # be opted-out.
        %{"durability" => "replicate:2", "allow_under_replicated" => true}
      ])

    volume_id = volume.id
    assert is_binary(volume_id)
    shards = shard_count(cluster)

    # Provisioning registers all N shard roots up front (they share the
    # one empty root chunk); wait until they're all in the bootstrap layer.
    :ok =
      wait_until(
        fn -> map_size(get_in(volume_roots(cluster), [volume_id]) || %{}) == shards end,
        timeout: 30_000
      )

    {volume_id, get_in(volume_roots(cluster), [volume_id])}
  end

  defp shard_hashes(cluster, volume_id) do
    volume_roots(cluster)
    |> Map.fetch!(volume_id)
    |> Map.new(fn {shard, entry} -> {shard, entry.root_chunk_hash} end)
  end

  defp shard_count(cluster) do
    PeerCluster.rpc(cluster, :node1, NeonFS.Core.Volume.Shard, :count, [])
  end

  defp volume_roots(cluster) do
    {:ok, roots} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :local_query, [
        &MetadataStateMachine.get_volume_roots/1
      ])

    roots
  end
end
