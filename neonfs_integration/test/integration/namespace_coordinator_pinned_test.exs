defmodule NeonFS.Integration.NamespaceCoordinatorPinnedTest do
  @moduledoc """
  Peer-cluster integration test for `claim_pinned/2` (sub-issue #637
  of #306). Validates that handle-pinned claims replicate across the
  Ra cluster and that holder-tied lifetime works across nodes — when
  the FUSE peer holding the pin dies, *every* node's view of the pin
  disappears, which is what the unlink-while-open story (#306) needs
  to safely reclaim metadata.

  Sibling to `namespace_coordinator_create_test.exs`. Holders are
  short-lived `Agent`s (started without link via `Agent.start/1`) so
  the RPC handler's exit doesn't drop the claim.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.NamespaceCoordinator

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "ns-pinned-test")
    :ok
  end

  describe "cross-node claim_pinned/2" do
    test "pin on node1 is visible on node2 via claims_for_path",
         %{cluster: cluster} do
      path = unique_path("/visible-pin")
      holder = start_holder(cluster, :node1)

      {:ok, claim_id} = claim_pinned_for(cluster, :node1, path, holder)

      try do
        assert {:ok, claims} = claims_for_path(cluster, :node2, path)
        ids = Enum.map(claims, &elem(&1, 0))
        assert claim_id in ids

        assert Enum.all?(claims, fn {_id, %{type: t}} -> t == :pinned end)
      after
        release_claim(cluster, :node1, claim_id)
        Agent.stop(holder, :normal, 1_000)
      end
    end

    test "pins from two different nodes on the same path coexist",
         %{cluster: cluster} do
      path = unique_path("/multi-pin")
      holder1 = start_holder(cluster, :node1)
      holder2 = start_holder(cluster, :node2)

      try do
        {:ok, id1} = claim_pinned_for(cluster, :node1, path, holder1)
        {:ok, id2} = claim_pinned_for(cluster, :node2, path, holder2)

        assert id1 != id2

        # Every node should see both pins.
        for node <- [:node1, :node2, :node3] do
          assert {:ok, claims} = claims_for_path(cluster, node, path)
          ids = Enum.map(claims, &elem(&1, 0))

          assert id1 in ids and id2 in ids,
                 "node #{inspect(node)} did not see both pins: #{inspect(claims)}"
        end

        release_claim(cluster, :node1, id1)
        release_claim(cluster, :node2, id2)
      after
        Agent.stop(holder1, :normal, 1_000)
        Agent.stop(holder2, :normal, 1_000)
      end
    end

    test "holder death on node1 releases pin observed from node2",
         %{cluster: cluster} do
      path = unique_path("/dying-pin")
      holder = start_holder(cluster, :node1)

      {:ok, _claim_id} = claim_pinned_for(cluster, :node1, path, holder)

      # Sanity: node2 sees the pin while the holder lives.
      assert {:ok, [_]} = claims_for_path(cluster, :node2, path)

      # Crash the holder. Its `:DOWN` fires on node1's coordinator,
      # which submits a `release_namespace_claims_for_holder` Ra
      # command. Once that replicates, node2 should see no pins.
      ref = Process.monitor(holder)
      Process.exit(holder, :kill)
      assert_receive {:DOWN, ^ref, :process, ^holder, _}, 5_000

      # Wait for the cleanup to replicate. Poll node2 — coordinator
      # bulk release is asynchronous from the holder DOWN.
      assert :ok =
               wait_until(fn ->
                 match?({:ok, []}, claims_for_path(cluster, :node2, path))
               end)
    end

    test "pin conflicts with an exclusive subtree claim on a different node",
         %{cluster: cluster} do
      base = unique_path("/locked")
      pinned_path = base <> "/file"
      holder_lock = start_holder(cluster, :node1)
      holder_pin = start_holder(cluster, :node2)

      try do
        {:ok, sub_id} =
          PeerCluster.rpc(cluster, :node1, NamespaceCoordinator, :claim_subtree_for, [
            NamespaceCoordinator,
            base,
            :exclusive,
            holder_lock
          ])

        assert {:error, :conflict, ^sub_id} =
                 claim_pinned_for(cluster, :node2, pinned_path, holder_pin)

        release_claim(cluster, :node1, sub_id)

        # After the lock releases, the pin succeeds.
        assert {:ok, pin_id} = claim_pinned_for(cluster, :node2, pinned_path, holder_pin)
        release_claim(cluster, :node2, pin_id)
      after
        Agent.stop(holder_lock, :normal, 1_000)
        Agent.stop(holder_pin, :normal, 1_000)
      end
    end
  end

  ## Helpers

  defp unique_path(prefix) do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end

  defp start_holder(cluster, node_name) do
    {:ok, pid} = PeerCluster.rpc(cluster, node_name, Agent, :start, [&Map.new/0])
    pid
  end

  defp claim_pinned_for(cluster, node_name, path, holder_pid) do
    PeerCluster.rpc(
      cluster,
      node_name,
      NamespaceCoordinator,
      :claim_pinned_for,
      [NamespaceCoordinator, path, holder_pid]
    )
  end

  defp claims_for_path(cluster, node_name, path) do
    PeerCluster.rpc(
      cluster,
      node_name,
      NamespaceCoordinator,
      :claims_for_path,
      [NamespaceCoordinator, path]
    )
  end

  defp release_claim(cluster, node_name, claim_id) do
    PeerCluster.rpc(cluster, node_name, NamespaceCoordinator, :release, [claim_id])
  end
end
