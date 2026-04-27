defmodule NeonFS.Integration.RemoveNodeTest do
  @moduledoc """
  Peer-cluster integration tests for `NeonFS.CLI.Handler.handle_remove_node/2`
  — the decommission CLI introduced in #455. The unit tests in
  `neonfs_core/test/neon_fs/cli/handler_test.exs` cover the safety-gate
  refusals against an empty cluster; these tests exercise the live Ra
  `:remove_member/3` path against a real 3-node quorum.

  Every test uses `:per_test` cluster mode because the `--force` case
  actually mutates membership and the other cases rely on the cluster
  state being deterministic (no residual Ra state from a prior test's
  removal). The per-test bootstrap cost is roughly 6 s per test; three
  tests total keeps the module under 30 s.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 3

  setup %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster)

    # `init_multi_node_cluster` boots peer nodes with no `:drives` app-
    # config and the join flow doesn't register any — so without this
    # step the drives-present safety gate in `handle_remove_node/2`
    # always passes, and the "refuses to remove a follower that still
    # owns drives" / "--force removes a follower with drives attached"
    # tests exercise a false premise. Register a default drive on each
    # follower via `DriveManager.add_drive` (which propagates through
    # the Ra-replicated `DriveRegistry`) so the safety gate sees real
    # drives on the target node when the handler runs on node1.
    for follower <- [:node2, :node3] do
      register_default_drive!(cluster, follower)
    end

    %{}
  end

  defp register_default_drive!(cluster, follower) do
    data_dir =
      PeerCluster.rpc(cluster, follower, Application, :get_env, [:neonfs_core, :data_dir])

    drive_path = Path.join(data_dir, "drive_default")
    :ok = PeerCluster.rpc(cluster, follower, File, :mkdir_p!, [drive_path])

    # Drive IDs are unique across the cluster (DriveRegistry is Ra-
    # replicated and rejects duplicates with `:duplicate_drive_id`).
    # Suffix the ID with the node alias so each follower lands a
    # distinct drive.
    drive_id = "default-#{follower}"

    {:ok, _drive} =
      PeerCluster.rpc(cluster, follower, NeonFS.Core.DriveManager, :add_drive, [
        %{id: drive_id, path: drive_path, tier: :hot, capacity: "1G"}
      ])

    :ok
  end

  describe "handle_remove_node/2 — live quorum" do
    test "refuses to remove the current Ra leader", %{cluster: cluster} do
      # Find the leader by asking node1.
      {:ok, _members, {_cluster_name, leader_node}} =
        PeerCluster.rpc(cluster, :node1, :ra, :members, [
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :server_id, []),
          2_000
        ])

      # Attempt removal from a peer that is not the leader — choose the
      # first non-leader node in the cluster's known members.
      caller_name =
        Enum.find([:node2, :node3, :node1], fn name ->
          PeerCluster.get_node!(cluster, name).node != leader_node
        end)

      assert {:error, %NeonFS.Error.Unavailable{message: msg}} =
               PeerCluster.rpc(
                 cluster,
                 caller_name,
                 NeonFS.CLI.Handler,
                 :handle_remove_node,
                 [Atom.to_string(leader_node)]
               )

      assert msg =~ "Ra leader"

      # Ra membership is unchanged — three members still.
      assert_eventually timeout: 5_000 do
        case PeerCluster.rpc(cluster, :node1, :ra, :members, [
               PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :server_id, []),
               2_000
             ]) do
          {:ok, members, _} -> length(members) == 3
          _ -> false
        end
      end
    end

    test "refuses to remove a follower that still owns drives", %{cluster: cluster} do
      # `setup` registers a default drive on every follower; no
      # evacuation has been done, so the drives-present check should
      # fire regardless of chunk counts.
      target = PeerCluster.get_node!(cluster, :node3).node

      assert {:error, %NeonFS.Error.Unavailable{message: msg}} =
               PeerCluster.rpc(
                 cluster,
                 :node1,
                 NeonFS.CLI.Handler,
                 :handle_remove_node,
                 [Atom.to_string(target)]
               )

      assert msg =~ "still owns"
      assert msg =~ "drive"
    end

    test "--force removes a follower even with drives still attached", %{cluster: cluster} do
      # Pick a follower to remove. We look up the leader and choose the
      # first non-leader node in the canonical list.
      {:ok, pre_members, {_cluster_name, leader_node}} =
        PeerCluster.rpc(cluster, :node1, :ra, :members, [
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :server_id, []),
          2_000
        ])

      assert length(pre_members) == 3

      target_name =
        Enum.find([:node2, :node3], fn name ->
          PeerCluster.get_node!(cluster, name).node != leader_node
        end)

      target_atom = PeerCluster.get_node!(cluster, target_name).node

      # Execute removal via the leader so the Ra command lands on the
      # authoritative member. The handler routes through `:ra.remove_member/3`
      # which must be applied via consensus.
      {_cluster_name, leader_name} =
        {elem(
           PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :server_id, []),
           0
         ),
         Enum.find([:node1, :node2, :node3], fn n ->
           PeerCluster.get_node!(cluster, n).node == leader_node
         end)}

      assert {:ok, result} =
               PeerCluster.rpc(
                 cluster,
                 leader_name,
                 NeonFS.CLI.Handler,
                 :handle_remove_node,
                 [Atom.to_string(target_atom), %{"force" => true}]
               )

      assert result.status == "removed"
      assert result.node == Atom.to_string(target_atom)
      assert result.remaining_members == 2

      # Verify Ra membership has actually dropped to 2, consensus-applied.
      assert_eventually timeout: 10_000 do
        case PeerCluster.rpc(cluster, leader_name, :ra, :members, [
               PeerCluster.rpc(cluster, leader_name, NeonFS.Core.RaSupervisor, :server_id, []),
               2_000
             ]) do
          {:ok, members, _leader} ->
            length(members) == 2 and
              not Enum.any?(members, fn {_name, node} -> node == target_atom end)

          _ ->
            false
        end
      end
    end
  end
end
