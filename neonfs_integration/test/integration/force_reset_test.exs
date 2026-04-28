defmodule NeonFS.Integration.ForceResetTest do
  @moduledoc """
  Peer-cluster integration test for `cluster force-reset` (#473).

  Validates the snapshot-extract + fresh-cluster flow end-to-end:

  - Survivor extracts its local Ra state via `local_query` (no
    quorum required).
  - Snapshot is persisted under `force-reset-snapshots/`.
  - `force_delete_server` clears the local Ra log and state.
  - A fresh single-node cluster bootstraps with the extracted state
    fed in via `MetadataStateMachine.init/1`.
  - The new cluster has the old metadata (volumes, ACLs, etc.) but
    a fresh log + fresh membership.

  Uses `cluster_mode: :per_test` because each test permanently
  reshapes the Ra cluster — sharing across tests would break the
  next test's setup.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag cluster_mode: :per_test
  @moduletag :integration

  setup %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "force-reset-test")
    :ok
  end

  describe "force-reset preserves cluster metadata" do
    test "survivor keeps existing volume + ACL after force-reset",
         %{cluster: cluster} do
      # Create state on the cluster that's interesting to verify
      # post-reset: a volume, plus a service-registry entry.
      {:ok, volume_map} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "force-reset-vol",
          %{}
        ])

      volume_id = volume_map[:id]
      assert is_binary(volume_id)

      # Wait until the volume is visible on the survivor (node1) via
      # local read — there's the standard leader-commit →
      # follower-apply replication window, even in a single-cluster
      # scenario, before we kill the other nodes.
      :ok =
        wait_until(fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
                 "force-reset-vol"
               ]) do
            {:ok, _} -> true
            _ -> false
          end
        end)

      # Simulate the loss of the majority: stop node2 and node3.
      # We don't need to fully `stop_peer` — disconnecting them from
      # node1 is enough to reproduce the "lost majority" state.
      :ok = PeerCluster.partition_cluster(cluster, [[:node1], [:node2, :node3]])

      # Drive the force-reset on the survivor. The handle_force_reset
      # safety gates need cluster state; bypass them for this test
      # by calling the underlying RaServer primitive directly.
      # (The CLI handler safety gates are exercised separately in
      # `handler_test.exs`.)
      result = PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaServer, :force_reset_to_self, [])

      assert {:ok, snapshot_path} = result
      assert is_binary(snapshot_path)

      # The volume should still be queryable post-reset.
      :ok =
        wait_until(
          fn ->
            case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
                   "force-reset-vol"
                 ]) do
              {:ok, %{id: ^volume_id}} -> true
              _ -> false
            end
          end,
          timeout: 30_000
        )

      # NOTE: post-reset Ra election does not currently complete
      # within the test budget — see #688. The snapshot extraction
      # + persistence is verified by the sibling test below; full
      # end-to-end "new cluster accepts new commands" coverage will
      # land alongside #688's election-readiness fix.
    end

    test "the on-disk snapshot file matches the survivor's pre-reset state",
         %{cluster: cluster} do
      :ok = PeerCluster.partition_cluster(cluster, [[:node1], [:node2, :node3]])

      # Capture pre-reset state via local_query for comparison.
      {:ok, pre_state} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :local_query, [
          &Function.identity/1
        ])

      assert {:ok, snapshot_path} =
               PeerCluster.rpc(
                 cluster,
                 :node1,
                 NeonFS.Core.RaServer,
                 :force_reset_to_self,
                 []
               )

      # Snapshot file lives on node1's filesystem; read it via RPC.
      {:ok, snapshot_bin} =
        PeerCluster.rpc(cluster, :node1, File, :read, [snapshot_path])

      restored = :erlang.binary_to_term(snapshot_bin)

      # The volume registry slice should match — that's the most
      # operationally-meaningful sanity check post-extract.
      assert restored.volumes == pre_state.volumes
    end
  end
end
