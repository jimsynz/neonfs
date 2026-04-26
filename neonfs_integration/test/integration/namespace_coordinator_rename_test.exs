defmodule NeonFS.Integration.NamespaceCoordinatorRenameTest do
  @moduledoc """
  Peer-cluster integration tests for `NamespaceCoordinator.claim_rename/3`
  (sub-issue #304). Closes the deferred multi-node test from #561.

  Mirrors the harness pattern established by
  `NeonFS.Integration.NamespaceCoordinatorTest` — see that module's
  docstring for the holder-pid mechanics.

  ## Coverage

    * **Cross-node visibility** — both halves of a rename pair allocated
      on node1 are visible to `list_claims/2` on node2 and node3.
    * **Cross-node conflict (overlapping rename)** — a rename on node1
      blocks a competing rename whose src or dst collides, regardless
      of which node the competing call originates from.
    * **Cross-node conflict (overlapping path-claim)** — a rename on
      node1 blocks a plain `claim_path_for/4` on either of its paths
      from another node.
    * **Concurrent renames serialise** — two renames fired in parallel
      from different nodes against the same path produce exactly one
      `{:ok, _}` and one `{:error, :conflict, _}`.
    * **Cycle detection across nodes** — a destination inside the
      source subtree returns `{:error, :einval}` from every node.
    * **Release lifecycle** — after `release_rename/2`, the same src/dst
      can be re-claimed from any node.
  """

  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Core.NamespaceCoordinator

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "ns-coord-rename-test")
    :ok
  end

  describe "cross-node visibility" do
    test "both halves of a rename claim are visible on every node", %{cluster: cluster} do
      {src, dst} = unique_paths("/visible-rename")
      holder = start_holder(cluster, :node1)

      {:ok, {src_id, dst_id}} = claim_rename_for(cluster, :node1, src, dst, holder)

      try do
        for peer <- [:node1, :node2, :node3] do
          ids = listed_ids(cluster, peer, src) ++ listed_ids(cluster, peer, dst)

          assert src_id in ids and dst_id in ids,
                 "expected node #{inspect(peer)} to see both #{src_id} and #{dst_id} " <>
                   "for rename #{src} -> #{dst}, got: #{inspect(ids)}"
        end
      after
        release_rename(cluster, :node1, {src_id, dst_id})
        Agent.stop(holder)
      end
    end
  end

  describe "cross-node conflict" do
    test "rename on node1 rejects a competing rename whose src overlaps", %{cluster: cluster} do
      {src, dst} = unique_paths("/conflict-src")
      {_other_src, other_dst} = unique_paths("/conflict-src-other")
      holder1 = start_holder(cluster, :node1)
      holder2 = start_holder(cluster, :node2)

      {:ok, {src_id, dst_id} = pair} = claim_rename_for(cluster, :node1, src, dst, holder1)

      try do
        # node2 attempts a rename that reuses the same src — must conflict
        # against one of the existing claims (the implementation reports
        # whichever it discovers first).
        assert {:error, :conflict, conflict_id} =
                 claim_rename_for(cluster, :node2, src, other_dst, holder2)

        assert conflict_id in [src_id, dst_id]
      after
        release_rename(cluster, :node1, pair)
        Agent.stop(holder1)
        Agent.stop(holder2)
      end
    end

    test "rename on node1 rejects a competing rename whose dst overlaps", %{cluster: cluster} do
      {src, dst} = unique_paths("/conflict-dst")
      {other_src, _other_dst} = unique_paths("/conflict-dst-other")
      holder1 = start_holder(cluster, :node1)
      holder3 = start_holder(cluster, :node3)

      {:ok, {src_id, dst_id} = pair} = claim_rename_for(cluster, :node1, src, dst, holder1)

      try do
        # node3 attempts a rename that targets the same dst — must conflict.
        assert {:error, :conflict, conflict_id} =
                 claim_rename_for(cluster, :node3, other_src, dst, holder3)

        assert conflict_id in [src_id, dst_id]
      after
        release_rename(cluster, :node1, pair)
        Agent.stop(holder1)
        Agent.stop(holder3)
      end
    end

    test "rename on node1 blocks a plain path-claim on either side from another node",
         %{cluster: cluster} do
      {src, dst} = unique_paths("/conflict-path")
      holder1 = start_holder(cluster, :node1)
      holder2 = start_holder(cluster, :node2)
      holder3 = start_holder(cluster, :node3)

      {:ok, {src_id, dst_id} = pair} = claim_rename_for(cluster, :node1, src, dst, holder1)

      try do
        # A plain path-claim on the rename's src from node2 must conflict
        # with the src half of the rename pair.
        assert {:error, :conflict, ^src_id} =
                 claim_path_for(cluster, :node2, src, :exclusive, holder2)

        # …and the same on dst from node3 must conflict with the dst half.
        assert {:error, :conflict, ^dst_id} =
                 claim_path_for(cluster, :node3, dst, :exclusive, holder3)
      after
        release_rename(cluster, :node1, pair)
        Agent.stop(holder1)
        Agent.stop(holder2)
        Agent.stop(holder3)
      end
    end

    test "concurrent renames against the same paths produce exactly one winner",
         %{cluster: cluster} do
      {src, dst} = unique_paths("/concurrent")
      holder1 = start_holder(cluster, :node1)
      holder2 = start_holder(cluster, :node2)

      try do
        # Fire both calls in parallel. Ra serialises commands cluster-wide,
        # so exactly one should win regardless of timing.
        results =
          [
            Task.async(fn -> claim_rename_for(cluster, :node1, src, dst, holder1) end),
            Task.async(fn -> claim_rename_for(cluster, :node2, src, dst, holder2) end)
          ]
          |> Task.await_many(15_000)

        {oks, errors} =
          Enum.split_with(results, fn
            {:ok, _} -> true
            _ -> false
          end)

        assert length(oks) == 1,
               "expected exactly one rename to win, got #{length(oks)}: #{inspect(results)}"

        assert [{:error, :conflict, _conflict_id}] = errors

        [{:ok, pair}] = oks
        release_rename(cluster, :node1, pair)
      after
        Agent.stop(holder1)
        Agent.stop(holder2)
      end
    end
  end

  describe "cycle detection" do
    test "destination inside source subtree returns :einval from every node",
         %{cluster: cluster} do
      root = unique_path("/cycle")
      descendant = "#{root}/child/grandchild"

      for peer <- [:node1, :node2, :node3] do
        holder = start_holder(cluster, peer)

        try do
          assert {:error, :einval} =
                   claim_rename_for(cluster, peer, root, descendant, holder),
                 "expected :einval on #{inspect(peer)} for #{root} -> #{descendant}"
        after
          Agent.stop(holder)
        end
      end
    end
  end

  describe "release lifecycle" do
    test "after release_rename/2, the same src and dst are re-claimable from any node",
         %{cluster: cluster} do
      {src, dst} = unique_paths("/release-cycle")
      holder1 = start_holder(cluster, :node1)
      holder2 = start_holder(cluster, :node2)

      {:ok, pair} = claim_rename_for(cluster, :node1, src, dst, holder1)
      :ok = release_rename(cluster, :node1, pair)

      # Before re-claiming we wait until node2's local Ra view reflects the
      # release; reads on followers are eventually consistent.
      :ok = wait_for_paths_free(cluster, :node2, [src, dst])

      try do
        assert {:ok, {_src_id2, _dst_id2} = pair2} =
                 claim_rename_for(cluster, :node2, src, dst, holder2)

        :ok = release_rename(cluster, :node2, pair2)
      after
        Agent.stop(holder1)
        Agent.stop(holder2)
      end
    end
  end

  ## Helpers

  defp unique_path(prefix) do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end

  # Returns a `{src, dst}` pair that share a unique tag so they're easy to
  # group with `list_claims/2` while staying outside each other's subtrees.
  defp unique_paths(prefix) do
    tag = System.unique_integer([:positive])
    {"#{prefix}-src-#{tag}", "#{prefix}-dst-#{tag}"}
  end

  # Same idiom as the sibling `NamespaceCoordinatorTest`: spawn a long-lived
  # `Agent` on the target node so the holder pid survives the RPC handler
  # exit. `Agent.start/1` (no link) is required — `start_link/1` would tie
  # the holder's lifetime to the short-lived `:rpc` worker. The init
  # function is `&Map.new/0` rather than a closure so peer nodes (which
  # don't have this test module loaded) can decode the MFA.
  defp start_holder(cluster, node_name) do
    {:ok, pid} = PeerCluster.rpc(cluster, node_name, Agent, :start, [&Map.new/0])
    pid
  end

  defp claim_rename_for(cluster, node_name, src, dst, holder_pid) do
    PeerCluster.rpc(
      cluster,
      node_name,
      NamespaceCoordinator,
      :claim_rename_for,
      [NamespaceCoordinator, src, dst, holder_pid]
    )
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

  defp release_rename(cluster, node_name, pair) do
    PeerCluster.rpc(cluster, node_name, NamespaceCoordinator, :release_rename, [
      NamespaceCoordinator,
      pair
    ])
  end

  defp release_claim(cluster, node_name, claim_id) do
    PeerCluster.rpc(cluster, node_name, NamespaceCoordinator, :release, [claim_id])
  end

  defp listed_ids(cluster, node_name, prefix) do
    {:ok, claims} =
      PeerCluster.rpc(cluster, node_name, NamespaceCoordinator, :list_claims, [
        NamespaceCoordinator,
        prefix
      ])

    Enum.map(claims, &elem(&1, 0))
  end

  # Polls until each of `paths` is re-claimable on `node_name`. Ra apply
  # is asynchronous w.r.t. the local follower's read-side state, so a
  # successful `release_rename/2` on the leader doesn't guarantee that
  # `node_name`'s coordinator has applied the release yet. Mirrors the
  # `wait_for_path_free/4` helper used in the sibling namespace
  # coordinator test.
  defp wait_for_paths_free(cluster, node_name, paths) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    holder = start_holder(cluster, node_name)

    try do
      Enum.each(paths, fn path ->
        :ok = do_wait_for_path_free(cluster, node_name, path, holder, deadline)
      end)
    after
      Agent.stop(holder)
    end
  end

  defp do_wait_for_path_free(cluster, node_name, path, holder, deadline) do
    case claim_path_for(cluster, node_name, path, :exclusive, holder) do
      {:ok, claim_id} ->
        :ok = release_claim(cluster, node_name, claim_id)
        :ok

      {:error, :conflict, _} ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("path #{path} did not become re-claimable on #{inspect(node_name)} within 5s")
        else
          Process.sleep(50)
          do_wait_for_path_free(cluster, node_name, path, holder, deadline)
        end
    end
  end
end
