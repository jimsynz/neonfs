defmodule NeonFS.Integration.NamespaceCoordinatorCreateTest do
  @moduledoc """
  Peer-cluster integration test for `claim_create/2` (sub-issue #591
  of #303). Validates that the foundation primitive coordinates
  atomic create-if-not-exist across nodes — exactly one of two
  concurrent claim_create calls on different nodes wins for the same
  path; the other gets `{:error, :exists}`.

  Sibling to `namespace_coordinator_test.exs`. Holders are short-lived
  Agents started on each peer so the RPC handler's exit doesn't drop
  the claim.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.NamespaceCoordinator

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "ns-create-test")
    :ok
  end

  describe "cross-node claim_create/2" do
    test "two concurrent claim_create calls on different nodes — exactly one wins",
         %{cluster: cluster} do
      path = unique_path("/race")

      holder1 = start_holder(cluster, :node1)
      holder2 = start_holder(cluster, :node2)

      try do
        # Race the two RPCs in parallel from this test process. The Ra
        # leader serialises the underlying commands, so exactly one
        # `:claim_namespace_create` apply sees a fresh state and wins.
        # The other observes the already-allocated claim and returns
        # `{:error, :exists}`.
        parent = self()

        spawn_link(fn ->
          send(parent, {:result, :node1, claim_create_for(cluster, :node1, path, holder1)})
        end)

        spawn_link(fn ->
          send(parent, {:result, :node2, claim_create_for(cluster, :node2, path, holder2)})
        end)

        result1 =
          receive do
            {:result, :node1, r} -> r
          after
            5_000 -> flunk("node1 claim_create did not return within 5s")
          end

        result2 =
          receive do
            {:result, :node2, r} -> r
          after
            5_000 -> flunk("node2 claim_create did not return within 5s")
          end

        # Exactly one :ok and one :exists, in some order.
        case {result1, result2} do
          {{:ok, claim_id}, {:error, :exists}} ->
            assert is_binary(claim_id)
            release_claim(cluster, :node1, claim_id)

          {{:error, :exists}, {:ok, claim_id}} ->
            assert is_binary(claim_id)
            release_claim(cluster, :node2, claim_id)

          other ->
            flunk("expected exactly one :ok and one :exists, got #{inspect(other)}")
        end
      after
        Agent.stop(holder1, :normal, 1_000)
        Agent.stop(holder2, :normal, 1_000)
      end
    end

    test "claim_create on node1 is observable on node2 via list_claims",
         %{cluster: cluster} do
      path = unique_path("/visible-create")
      holder = start_holder(cluster, :node1)

      {:ok, claim_id} = claim_create_for(cluster, :node1, path, holder)

      try do
        # `NamespaceCoordinator.list_claims/2` reads from the local
        # follower's Ra state via `:ra.local_query`. There's a small
        # window between the leader committing the
        # `:claim_namespace_create` command and node2's follower
        # applying it locally during which the local read returns an
        # empty result. `wait_until` covers that lag without
        # spuriously failing — see #666 for the original race.
        assert :ok =
                 wait_until(fn ->
                   case PeerCluster.rpc(cluster, :node2, NamespaceCoordinator, :list_claims, [
                          NamespaceCoordinator,
                          path
                        ]) do
                     {:ok, claims} -> claim_id in Enum.map(claims, &elem(&1, 0))
                     _ -> false
                   end
                 end)
      after
        release_claim(cluster, :node1, claim_id)
        Agent.stop(holder, :normal, 1_000)
      end
    end

    test "after release, a competing claim_create from a different node succeeds",
         %{cluster: cluster} do
      path = unique_path("/handoff")
      holder1 = start_holder(cluster, :node1)
      holder3 = start_holder(cluster, :node3)

      try do
        {:ok, claim_id} = claim_create_for(cluster, :node1, path, holder1)

        assert {:error, :exists} = claim_create_for(cluster, :node3, path, holder3)

        :ok = release_claim(cluster, :node1, claim_id)

        assert {:ok, _new_claim} = claim_create_for(cluster, :node3, path, holder3)
      after
        Agent.stop(holder1, :normal, 1_000)
        Agent.stop(holder3, :normal, 1_000)
      end
    end
  end

  ## Helpers

  defp unique_path(prefix) do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end

  # `Agent.start/1` (no link) so the holder survives the RPC handler's
  # exit on the peer; `&Map.new/0` rather than a closure because peer
  # nodes don't have this test module loaded.
  defp start_holder(cluster, node_name) do
    {:ok, pid} = PeerCluster.rpc(cluster, node_name, Agent, :start, [&Map.new/0])
    pid
  end

  defp claim_create_for(cluster, node_name, path, holder_pid) do
    PeerCluster.rpc(
      cluster,
      node_name,
      NamespaceCoordinator,
      :claim_create_for,
      [NamespaceCoordinator, path, holder_pid]
    )
  end

  defp release_claim(cluster, node_name, claim_id) do
    PeerCluster.rpc(cluster, node_name, NamespaceCoordinator, :release, [claim_id])
  end
end
