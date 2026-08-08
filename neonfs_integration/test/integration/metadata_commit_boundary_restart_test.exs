defmodule NeonFS.Integration.MetadataCommitBoundaryRestartTest do
  @moduledoc """
  The last cross-shard commit boundary: a publication whose post-commit
  effects never ran, followed by the loss of the whole node that
  published it.

  `metadata_commit_boundary_test.exs` proves that killing `FileIndex`
  between consensus and its effects still leaves a complete operation.
  This proves the same thing survives the node going away afterwards —
  the case where the local ETS materialisation and the process that would
  have finished the job are both gone at once. Recovery here has to come
  from the replicated root set, because there is nothing else left to
  come from.

  A whole-node restart mutates cluster state permanently, so this needs
  a `:per_test` cluster; the shared-cluster boundary tests live next
  door. Same split as `partition_test.exs` / `partition_restart_test.exs`.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.{FileIndex, FileMeta, VolumeRegistry}
  alias NeonFS.Integration.CommitBoundary
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag :integration

  @volume "commit-boundary-restart"
  @nodes [:node1, :node2, :node3]
  @read_timeout 60_000

  setup %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, volumes: [{@volume, %{}}])

    {:ok, volume} = PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, [@volume])

    %{volume: volume}
  end

  test "publications that lost their effects survive the publishing node's restart",
       %{cluster: cluster, volume: volume} do
    created = "/restart/created.bin"
    created_id = UUIDv7.generate()

    renamed_dir = "/restart-renamed"
    renamed_id = UUIDv7.generate()

    create_on(cluster, :node2, volume.id, Path.join(renamed_dir, "before.bin"), renamed_id)

    # Both crashes land on node1 — it is the node about to be restarted,
    # so its metadata is the metadata with the most ways to be lost.
    assert {:ok, _} =
             crash_before_effects(cluster, :node1, volume.id, FileIndex, :create, [
               FileMeta.new(volume.id, created, id: created_id)
             ])

    assert {:ok, _} =
             crash_before_effects(cluster, :node1, volume.id, FileIndex, :rename, [
               volume.id,
               renamed_dir,
               "before.bin",
               "after.bin"
             ])

    {:ok, cluster} = PeerCluster.restart_node(cluster, :node1)
    stabilise_after_restart(cluster)

    assert_file_present(cluster, volume.id, created, created_id)
    assert_file_present(cluster, volume.id, Path.join(renamed_dir, "after.bin"), renamed_id)
    assert_path_absent(cluster, volume.id, Path.join(renamed_dir, "before.bin"))

    # The restarted node is not merely readable — it can still publish.
    # Both crashed operations leased a conflict key, and both leases went
    # out with the entries that published them, so a commit now is the
    # node having recovered rather than a lease having timed out.
    assert_commits(cluster, :node1, volume.id, "/restart/after-restart.bin")
  end

  # ─── Helpers ─────────────────────────────────────────────────────────

  defp crash_before_effects(cluster, node, volume_id, module, function, args) do
    PeerCluster.rpc(
      cluster,
      node,
      CommitBoundary,
      :crash_before_effects,
      [volume_id, module, function, args],
      120_000
    )
  end

  defp create_on(cluster, node, volume_id, path, file_id) do
    file = FileMeta.new(volume_id, path, id: file_id)
    assert {:ok, _} = PeerCluster.rpc(cluster, node, FileIndex, :create, [file])
    :ok
  end

  defp assert_commits(cluster, node, volume_id, path) do
    file_id = UUIDv7.generate()
    :ok = create_on(cluster, node, volume_id, path, file_id)
    assert_file_present(cluster, volume_id, path, file_id)
  end

  # `get_by_path/2` resolves the dirent and then the `FileMeta` behind it,
  # so it fails on a dangling dirent; `get/2` reads the `FileMeta`
  # directly. Asserting both, on every node, is what rules out a surviving
  # participant subset.
  defp assert_file_present(cluster, volume_id, path, file_id) do
    for node <- @nodes do
      assert_eventually timeout: @read_timeout do
        match?(
          {:ok, %FileMeta{id: ^file_id, path: ^path}},
          PeerCluster.rpc(cluster, node, FileIndex, :get_by_path, [volume_id, path])
        )
      end

      assert {:ok, %FileMeta{id: ^file_id, path: ^path}} =
               PeerCluster.rpc(cluster, node, FileIndex, :get, [volume_id, file_id]),
             "#{node} resolves the dirent for #{path} but not a matching FileMeta"
    end
  end

  defp assert_path_absent(cluster, volume_id, path) do
    for node <- @nodes do
      assert_eventually timeout: @read_timeout do
        PeerCluster.rpc(cluster, node, FileIndex, :get_by_path, [volume_id, path]) ==
          {:error, :not_found}
      end
    end
  end
end
