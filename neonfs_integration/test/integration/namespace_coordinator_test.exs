defmodule NeonFS.Integration.NamespaceCoordinatorTest do
  @moduledoc """
  Peer-cluster integration tests for `NeonFS.Core.NamespaceCoordinator`
  — closes the deferred test from #300 (namespace coordinator
  foundation) and gives later coordinator-related sub-issues
  (#561 / claim_rename, #570 / DRSnapshotScheduler) a working harness
  to copy.

  ## Coverage

    * Cross-node visibility — a claim taken on node1 is observable on
      node2/node3 via `list_claims/2`.
    * Cross-node conflict — a competing claim on the same path from
      a different node is rejected with `:conflict`.
    * Multi-granularity — a `claim_subtree/2` on node1 blocks a
      descendant `claim_path/2` from node3.
    * Process-tied lifetime across distribution — when the holder
      pid (created on node1) dies, the leader's coordinator releases
      its claims via the remote `:DOWN` monitor, and a competing
      claim from any node now succeeds.

  Holders are bare `Agent` pids spawned on a specific node so we can
  kill them deterministically; production callers (WebDAV
  LockStore, Core rename helper, …) plug in their own long-lived
  per-node holders.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.NamespaceCoordinator

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "ns-coord-test")
    :ok
  end

  describe "cross-node visibility" do
    test "claim taken on node1 is visible on node2 and node3", %{cluster: cluster} do
      path = unique_path("/visible")
      holder = start_holder(cluster, :node1)

      {:ok, claim_id} = claim_path_for(cluster, :node1, path, :exclusive, holder)

      try do
        for peer <- [:node2, :node3] do
          assert {:ok, claims} =
                   PeerCluster.rpc(cluster, peer, NamespaceCoordinator, :list_claims, [
                     NamespaceCoordinator,
                     path
                   ])

          ids = Enum.map(claims, &elem(&1, 0))

          assert claim_id in ids,
                 "expected node #{inspect(peer)} to see claim #{claim_id} on #{path}, " <>
                   "got: #{inspect(ids)}"
        end
      after
        release_claim(cluster, :node1, claim_id)
        Agent.stop(holder)
      end
    end
  end

  describe "cross-node conflict" do
    test "competing exclusive path-claim from another node is rejected", %{cluster: cluster} do
      path = unique_path("/contended")
      holder1 = start_holder(cluster, :node1)
      holder2 = start_holder(cluster, :node2)

      {:ok, claim_id} = claim_path_for(cluster, :node1, path, :exclusive, holder1)

      try do
        assert {:error, :conflict, ^claim_id} =
                 claim_path_for(cluster, :node2, path, :exclusive, holder2)
      after
        release_claim(cluster, :node1, claim_id)
        Agent.stop(holder1)
        Agent.stop(holder2)
      end
    end

    test "subtree claim on one node blocks a descendant path-claim from another",
         %{cluster: cluster} do
      root = unique_path("/subtree")
      descendant = "#{root}/child"
      holder1 = start_holder(cluster, :node1)
      holder3 = start_holder(cluster, :node3)

      {:ok, claim_id} = claim_subtree_for(cluster, :node1, root, :exclusive, holder1)

      try do
        assert {:error, :conflict, ^claim_id} =
                 claim_path_for(cluster, :node3, descendant, :exclusive, holder3)
      after
        release_claim(cluster, :node1, claim_id)
        Agent.stop(holder1)
        Agent.stop(holder3)
      end
    end
  end

  describe "process-tied lifetime across distribution" do
    test "holder death on node1 releases its claims cluster-wide", %{cluster: cluster} do
      path = unique_path("/lifetime")
      holder = start_holder(cluster, :node1)

      {:ok, _claim_id} = claim_path_for(cluster, :node1, path, :exclusive, holder)

      # Sanity: the claim is held; a competing claim is rejected.
      contender = start_holder(cluster, :node2)

      try do
        assert {:error, :conflict, _} =
                 claim_path_for(cluster, :node2, path, :exclusive, contender)

        # Kill the holder. The leader's coordinator monitors the
        # remote pid via Erlang distribution; the :DOWN handler
        # issues a `release_namespace_claims_for_holder` Ra command.
        Agent.stop(holder)

        # Wait for the release to propagate (Ra apply is async w.r.t.
        # the :DOWN delivery on the leader).
        :ok = wait_for_path_free(cluster, :node2, path, contender)
      after
        Agent.stop(contender, :normal, 1_000)
      end
    end
  end

  ## Helpers

  defp unique_path(prefix) do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end

  # Spawn a long-lived `Agent` on `node_name` to act as the holder for
  # a coordinator claim. Returns the local `pid()` reference; when
  # passed via Erlang distribution it monitors correctly because
  # `Process.monitor/1` handles remote pids natively.
  #
  # Uses `Agent.start/1` (no link) so the holder survives the RPC
  # handler's exit on the peer node — `start_link/1` would link the
  # Agent to the short-lived `:rpc` worker, taking the holder down
  # the moment the RPC returns. The init function is `&Map.new/0`
  # rather than a closure: peer nodes don't have this test module
  # loaded, so an anonymous function defined here can't be decoded
  # on the remote side and would fail with `:undef`.
  defp start_holder(cluster, node_name) do
    {:ok, pid} = PeerCluster.rpc(cluster, node_name, Agent, :start, [&Map.new/0])
    pid
  end

  defp claim_path_for(cluster, node_name, path, scope, holder_pid) do
    PeerCluster.rpc(
      cluster,
      node_name,
      NamespaceCoordinator,
      :claim_path_for,
      [NamespaceCoordinator, path, scope, holder_pid]
    )
  end

  defp claim_subtree_for(cluster, node_name, path, scope, holder_pid) do
    PeerCluster.rpc(
      cluster,
      node_name,
      NamespaceCoordinator,
      :claim_subtree_for,
      [NamespaceCoordinator, path, scope, holder_pid]
    )
  end

  defp release_claim(cluster, node_name, claim_id) do
    PeerCluster.rpc(cluster, node_name, NamespaceCoordinator, :release, [claim_id])
  end

  # Polls `path` from `node_name` (using a fresh holder so a previous
  # release / take cycle doesn't shadow the test). Succeeds when the
  # claim succeeds.
  defp wait_for_path_free(cluster, node_name, path, holder_pid) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    do_wait_for_path_free(cluster, node_name, path, holder_pid, deadline)
  end

  defp do_wait_for_path_free(cluster, node_name, path, holder_pid, deadline) do
    case claim_path_for(cluster, node_name, path, :exclusive, holder_pid) do
      {:ok, claim_id} ->
        release_claim(cluster, node_name, claim_id)
        :ok

      {:error, :conflict, _} ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("claim on #{path} was not released within 5s of holder death")
        else
          Process.sleep(100)
          do_wait_for_path_free(cluster, node_name, path, holder_pid, deadline)
        end
    end
  end
end
