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

  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Integration.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 3

  setup %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster)
    %{}
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
      # node3 was added to the cluster with its default drive; no
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
