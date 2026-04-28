defmodule NeonFS.Integration.NamespaceCoordinatorByteRangeTest do
  @moduledoc """
  Peer-cluster integration test for `claim_byte_range/4` and
  `query_byte_range/4` (#673). Validates that byte-range claims
  replicate across the Ra cluster, that overlapping ranges with
  conflicting scopes collide cross-node, and that holder-DOWN bulk
  release reaps byte-range claims the same way it reaps `:pinned`
  claims (#642's pattern).

  Sibling to `namespace_coordinator_pinned_test.exs`. Holders are
  short-lived `Agent`s (started without link via `Agent.start/1`)
  so the RPC handler's exit doesn't drop the claim.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.NamespaceCoordinator

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "ns-byte-range-test")
    :ok
  end

  describe "cross-node claim_byte_range/4" do
    test "claim on node1 conflicts with overlapping claim on node2",
         %{cluster: cluster} do
      path = unique_path("/cross-node-conflict")
      holder1 = start_holder(cluster, :node1)
      holder2 = start_holder(cluster, :node2)

      try do
        {:ok, id_a} = claim_byte_range_for(cluster, :node1, path, {0, 100}, :exclusive, holder1)

        assert {:error, :conflict, ^id_a} =
                 claim_byte_range_for(cluster, :node2, path, {50, 100}, :exclusive, holder2)

        release_claim(cluster, :node1, id_a)

        assert {:ok, _} =
                 claim_byte_range_for(cluster, :node2, path, {50, 100}, :exclusive, holder2)
      after
        Agent.stop(holder1, :normal, 1_000)
        Agent.stop(holder2, :normal, 1_000)
      end
    end

    test "non-overlapping ranges from different nodes coexist",
         %{cluster: cluster} do
      path = unique_path("/cross-node-disjoint")
      holder1 = start_holder(cluster, :node1)
      holder2 = start_holder(cluster, :node2)

      try do
        assert {:ok, _} =
                 claim_byte_range_for(cluster, :node1, path, {0, 100}, :exclusive, holder1)

        assert {:ok, _} =
                 claim_byte_range_for(cluster, :node2, path, {200, 100}, :exclusive, holder2)
      after
        Agent.stop(holder1, :normal, 1_000)
        Agent.stop(holder2, :normal, 1_000)
      end
    end

    test "two shared overlapping ranges from different nodes coexist",
         %{cluster: cluster} do
      path = unique_path("/cross-node-shared")
      holder1 = start_holder(cluster, :node1)
      holder2 = start_holder(cluster, :node2)

      try do
        assert {:ok, _} =
                 claim_byte_range_for(cluster, :node1, path, {0, 100}, :shared, holder1)

        assert {:ok, _} =
                 claim_byte_range_for(cluster, :node2, path, {50, 100}, :shared, holder2)
      after
        Agent.stop(holder1, :normal, 1_000)
        Agent.stop(holder2, :normal, 1_000)
      end
    end

    test "holder death on node1 releases byte-range claim observed from node2",
         %{cluster: cluster} do
      path = unique_path("/dying-byte-range")
      holder = start_holder(cluster, :node1)

      {:ok, _claim_id} =
        claim_byte_range_for(cluster, :node1, path, {0, 100}, :exclusive, holder)

      # Sanity: node3 sees the conflict while the holder lives. Wraps
      # `wait_until` per the leader-commit -> follower-apply window
      # documented in the create-test (#666).
      assert :ok =
               wait_until(fn ->
                 case query_byte_range(cluster, :node3, path, {50, 100}, :exclusive) do
                   {:ok, {:locked, _holder, _range, _scope}} -> true
                   _ -> false
                 end
               end)

      ref = Process.monitor(holder)
      Process.exit(holder, :kill)
      assert_receive {:DOWN, ^ref, :process, ^holder, _}, 5_000

      # Once the holder dies, the coordinator's bulk-release path
      # replicates and node3 sees the range as unlocked.
      assert :ok =
               wait_until(fn ->
                 match?(
                   {:ok, :unlocked},
                   query_byte_range(cluster, :node3, path, {50, 100}, :exclusive)
                 )
               end)
    end
  end

  describe "cross-node query_byte_range/4" do
    test "query on node3 sees a claim placed on node1",
         %{cluster: cluster} do
      path = unique_path("/cross-query")
      holder = start_holder(cluster, :node1)

      {:ok, _id} = claim_byte_range_for(cluster, :node1, path, {100, 200}, :exclusive, holder)

      try do
        assert :ok =
                 wait_until(fn ->
                   case query_byte_range(cluster, :node3, path, {150, 100}, :exclusive) do
                     {:ok, {:locked, _holder, {100, 200}, :exclusive}} -> true
                     _ -> false
                   end
                 end)
      after
        Agent.stop(holder, :normal, 1_000)
      end
    end

    test "query reports :unlocked for non-overlapping range",
         %{cluster: cluster} do
      path = unique_path("/cross-unlocked")
      holder = start_holder(cluster, :node1)

      {:ok, _id} = claim_byte_range_for(cluster, :node1, path, {0, 100}, :exclusive, holder)

      try do
        # The holder's claim covers [0, 100); a query at [200, 300)
        # has no conflict.
        assert :ok =
                 wait_until(fn ->
                   match?(
                     {:ok, :unlocked},
                     query_byte_range(cluster, :node3, path, {200, 100}, :exclusive)
                   )
                 end)
      after
        Agent.stop(holder, :normal, 1_000)
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

  defp claim_byte_range_for(cluster, node_name, path, range, scope, holder_pid) do
    PeerCluster.rpc(
      cluster,
      node_name,
      NamespaceCoordinator,
      :claim_byte_range_for,
      [NamespaceCoordinator, path, range, scope, holder_pid]
    )
  end

  defp query_byte_range(cluster, node_name, path, range, scope) do
    PeerCluster.rpc(
      cluster,
      node_name,
      NamespaceCoordinator,
      :query_byte_range,
      [NamespaceCoordinator, path, range, scope]
    )
  end

  defp release_claim(cluster, node_name, claim_id) do
    PeerCluster.rpc(cluster, node_name, NamespaceCoordinator, :release, [claim_id])
  end
end
