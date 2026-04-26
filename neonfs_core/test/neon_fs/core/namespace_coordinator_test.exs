defmodule NeonFS.Core.NamespaceCoordinatorTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{NamespaceCoordinator, RaServer}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()

    name = :"namespace_coordinator_#{System.unique_integer([:positive])}"
    {:ok, pid} = NamespaceCoordinator.start_link(name: name)
    Process.unlink(pid)

    on_exit(fn ->
      try do
        if Process.alive?(pid), do: GenServer.stop(pid, :shutdown, 1_000)
      catch
        :exit, _ -> :ok
      end

      cleanup_test_dirs()
    end)

    {:ok, server: name}
  end

  describe "claim_path/3" do
    test "returns a claim id on first claim", %{server: server} do
      assert {:ok, "ns-claim-" <> _} = NamespaceCoordinator.claim_path(server, "/a", :exclusive)
    end

    test "two shared claims on the same path coexist", %{server: server} do
      assert {:ok, _} = NamespaceCoordinator.claim_path(server, "/shared", :shared)
      assert {:ok, _} = NamespaceCoordinator.claim_path(server, "/shared", :shared)
    end

    test "exclusive blocks any subsequent claim on the same path", %{server: server} do
      assert {:ok, claim_a} = NamespaceCoordinator.claim_path(server, "/excl", :exclusive)

      assert {:error, :conflict, ^claim_a} =
               NamespaceCoordinator.claim_path(server, "/excl", :exclusive)

      assert {:error, :conflict, ^claim_a} =
               NamespaceCoordinator.claim_path(server, "/excl", :shared)
    end

    test "shared blocks a subsequent exclusive on the same path", %{server: server} do
      assert {:ok, claim_a} = NamespaceCoordinator.claim_path(server, "/p", :shared)

      assert {:error, :conflict, ^claim_a} =
               NamespaceCoordinator.claim_path(server, "/p", :exclusive)
    end

    test "after release, a previously-blocked claim succeeds", %{server: server} do
      {:ok, claim_a} = NamespaceCoordinator.claim_path(server, "/release-test", :exclusive)
      :ok = NamespaceCoordinator.release(server, claim_a)

      assert {:ok, _claim_b} =
               NamespaceCoordinator.claim_path(server, "/release-test", :exclusive)
    end
  end

  describe "claim_subtree/3" do
    test "subtree blocks a path claim under it", %{server: server} do
      assert {:ok, parent} = NamespaceCoordinator.claim_subtree(server, "/a", :exclusive)

      assert {:error, :conflict, ^parent} =
               NamespaceCoordinator.claim_path(server, "/a/b", :exclusive)

      assert {:error, :conflict, ^parent} =
               NamespaceCoordinator.claim_path(server, "/a/b/c", :shared)
    end

    test "path claim under a subtree blocks the subtree", %{server: server} do
      assert {:ok, child} = NamespaceCoordinator.claim_path(server, "/a/b", :exclusive)

      assert {:error, :conflict, ^child} =
               NamespaceCoordinator.claim_subtree(server, "/a", :exclusive)
    end

    test "two subtrees with overlapping roots conflict", %{server: server} do
      assert {:ok, outer} = NamespaceCoordinator.claim_subtree(server, "/a", :exclusive)

      assert {:error, :conflict, ^outer} =
               NamespaceCoordinator.claim_subtree(server, "/a/b", :exclusive)
    end

    test "two shared subtrees with overlapping roots are compatible", %{server: server} do
      assert {:ok, _} = NamespaceCoordinator.claim_subtree(server, "/sh", :shared)
      assert {:ok, _} = NamespaceCoordinator.claim_subtree(server, "/sh/x", :shared)
    end

    test "non-overlapping subtrees never conflict", %{server: server} do
      assert {:ok, _} = NamespaceCoordinator.claim_subtree(server, "/dir-a", :exclusive)
      assert {:ok, _} = NamespaceCoordinator.claim_subtree(server, "/dir-b", :exclusive)
    end

    test "the root subtree (`/`) covers everything", %{server: server} do
      assert {:ok, root} = NamespaceCoordinator.claim_subtree(server, "/", :exclusive)

      assert {:error, :conflict, ^root} =
               NamespaceCoordinator.claim_path(server, "/anything", :shared)

      assert {:error, :conflict, ^root} =
               NamespaceCoordinator.claim_subtree(server, "/some/deep/sub", :shared)
    end

    test "path claim with the same string as a subtree claim conflicts", %{server: server} do
      # subtree(/x) covers /x itself, so claim_path(/x) should conflict.
      assert {:ok, sub} = NamespaceCoordinator.claim_subtree(server, "/x", :exclusive)

      assert {:error, :conflict, ^sub} =
               NamespaceCoordinator.claim_path(server, "/x", :shared)
    end

    test "string-prefix collisions that aren't subtree members don't conflict",
         %{server: server} do
      # subtree(/users) does NOT cover /usersgroups (sibling, not child).
      assert {:ok, _} = NamespaceCoordinator.claim_subtree(server, "/users", :exclusive)
      assert {:ok, _} = NamespaceCoordinator.claim_path(server, "/usersgroups", :exclusive)
    end
  end

  describe "release/2" do
    test "is idempotent for unknown ids", %{server: server} do
      assert :ok = NamespaceCoordinator.release(server, "ns-claim-does-not-exist")
    end

    test "is idempotent for already-released ids", %{server: server} do
      {:ok, claim} = NamespaceCoordinator.claim_path(server, "/idem", :exclusive)
      assert :ok = NamespaceCoordinator.release(server, claim)
      assert :ok = NamespaceCoordinator.release(server, claim)
    end
  end

  describe "list_claims/2" do
    test "returns every claim with no prefix", %{server: server} do
      {:ok, _} = NamespaceCoordinator.claim_path(server, "/list/a", :exclusive)
      {:ok, _} = NamespaceCoordinator.claim_path(server, "/list/b", :exclusive)
      {:ok, _} = NamespaceCoordinator.claim_subtree(server, "/list/c", :shared)

      assert {:ok, claims} = NamespaceCoordinator.list_claims(server, "/list/")
      paths = claims |> Enum.map(fn {_id, c} -> c.path end) |> Enum.sort()

      assert paths == ["/list/a", "/list/b", "/list/c"]
    end

    test "filters by path prefix", %{server: server} do
      {:ok, _} = NamespaceCoordinator.claim_path(server, "/keep/x", :exclusive)
      {:ok, _} = NamespaceCoordinator.claim_path(server, "/skip/y", :exclusive)

      assert {:ok, [{_id, %{path: "/keep/x"}}]} =
               NamespaceCoordinator.list_claims(server, "/keep")
    end
  end

  describe "process-tied lifetime" do
    test "claims are released when the holder process dies", %{server: server} do
      parent = self()

      {holder, monitor_ref} =
        spawn_monitor(fn ->
          {:ok, claim_id} = NamespaceCoordinator.claim_path(server, "/lifetime", :exclusive)
          send(parent, {:claimed, claim_id})

          receive do
            :exit -> :ok
          end
        end)

      assert_receive {:claimed, _claim_id}, 1_000

      # While the holder is alive, the claim blocks a competing claim.
      assert {:error, :conflict, _} =
               NamespaceCoordinator.claim_path(server, "/lifetime", :exclusive)

      send(holder, :exit)
      assert_receive {:DOWN, ^monitor_ref, :process, ^holder, _}, 1_000

      # The coordinator's :DOWN handler runs in its own mailbox; sync
      # the GenServer to make sure the release command has run before
      # we test for it.
      :sys.get_state(server)

      assert {:ok, _new_claim} =
               NamespaceCoordinator.claim_path(server, "/lifetime", :exclusive)
    end

    test "releasing one claim of a multi-claim holder leaves the others alive",
         %{server: server} do
      {:ok, claim_a} = NamespaceCoordinator.claim_path(server, "/multi/a", :exclusive)
      {:ok, _claim_b} = NamespaceCoordinator.claim_path(server, "/multi/b", :exclusive)

      :ok = NamespaceCoordinator.release(server, claim_a)

      # /multi/b should still be claimed.
      assert {:error, :conflict, _} =
               NamespaceCoordinator.claim_path(server, "/multi/b", :exclusive)

      assert {:ok, _} = NamespaceCoordinator.claim_path(server, "/multi/a", :exclusive)
    end
  end

  # `claim_rename/3` is the namespace-coordinator primitive for atomic
  # cross-directory rename — sub-issue #304. The two paths must be
  # pinned together (no half-claimed window) and the destination must
  # not sit inside the source's own subtree.
  describe "claim_rename/3" do
    test "pins src + dst as a paired claim", %{server: server} do
      assert {:ok, {src_id, dst_id}} =
               NamespaceCoordinator.claim_rename(server, "/from", "/to")

      assert is_binary(src_id) and is_binary(dst_id)
      assert src_id != dst_id

      # Both paths are blocked while the rename claim is held.
      assert {:error, :conflict, ^src_id} =
               NamespaceCoordinator.claim_path(server, "/from", :exclusive)

      assert {:error, :conflict, ^dst_id} =
               NamespaceCoordinator.claim_path(server, "/to", :exclusive)
    end

    test "rejects rename into the source's own subtree (cycle)", %{server: server} do
      # /a -> /a/b/c is a cycle — destination sits under the source.
      assert {:error, :einval} =
               NamespaceCoordinator.claim_rename(server, "/a", "/a/b/c")

      # Self-rename is also a cycle by the same rule (dst == src).
      assert {:error, :einval} = NamespaceCoordinator.claim_rename(server, "/a", "/a")
    end

    test "non-cycle cross-directory renames succeed", %{server: server} do
      # Sibling directory move — not a cycle.
      assert {:ok, _} = NamespaceCoordinator.claim_rename(server, "/dir-a", "/dir-b")
    end

    test "fails atomically when the source path is already claimed", %{server: server} do
      {:ok, src_claim} = NamespaceCoordinator.claim_path(server, "/locked-src", :exclusive)

      assert {:error, :conflict, ^src_claim} =
               NamespaceCoordinator.claim_rename(server, "/locked-src", "/free-dst")

      # The destination must NOT have been pinned — atomic failure.
      assert {:ok, _} = NamespaceCoordinator.claim_path(server, "/free-dst", :exclusive)
    end

    test "fails atomically when the destination path is already claimed", %{server: server} do
      {:ok, dst_claim} = NamespaceCoordinator.claim_path(server, "/locked-dst", :exclusive)

      assert {:error, :conflict, ^dst_claim} =
               NamespaceCoordinator.claim_rename(server, "/free-src", "/locked-dst")

      # The source must NOT have been pinned.
      assert {:ok, _} = NamespaceCoordinator.claim_path(server, "/free-src", :exclusive)
    end

    test "fails when dst is inside an existing subtree claim", %{server: server} do
      {:ok, sub_claim} = NamespaceCoordinator.claim_subtree(server, "/protected", :exclusive)

      assert {:error, :conflict, ^sub_claim} =
               NamespaceCoordinator.claim_rename(server, "/free-src", "/protected/x")
    end

    test "release_rename releases both claims", %{server: server} do
      {:ok, claim} = NamespaceCoordinator.claim_rename(server, "/r1-src", "/r1-dst")

      assert :ok = NamespaceCoordinator.release_rename(server, claim)

      # Both paths free again.
      assert {:ok, _} = NamespaceCoordinator.claim_path(server, "/r1-src", :exclusive)
      assert {:ok, _} = NamespaceCoordinator.claim_path(server, "/r1-dst", :exclusive)
    end

    test "release_rename is idempotent", %{server: server} do
      {:ok, claim} = NamespaceCoordinator.claim_rename(server, "/r2-src", "/r2-dst")
      assert :ok = NamespaceCoordinator.release_rename(server, claim)
      assert :ok = NamespaceCoordinator.release_rename(server, claim)
    end

    test "claims released when the holder process dies", %{server: server} do
      parent = self()

      {holder, monitor_ref} =
        spawn_monitor(fn ->
          {:ok, claim} = NamespaceCoordinator.claim_rename(server, "/h-src", "/h-dst")
          send(parent, {:claimed, claim})

          receive do
            :exit -> :ok
          end
        end)

      assert_receive {:claimed, _}, 1_000

      assert {:error, :conflict, _} =
               NamespaceCoordinator.claim_path(server, "/h-src", :exclusive)

      send(holder, :exit)
      assert_receive {:DOWN, ^monitor_ref, :process, ^holder, _}, 1_000
      :sys.get_state(server)

      assert {:ok, _} = NamespaceCoordinator.claim_path(server, "/h-src", :exclusive)
      assert {:ok, _} = NamespaceCoordinator.claim_path(server, "/h-dst", :exclusive)
    end

    test "claim_rename_for/4 honours the explicit holder", %{server: server} do
      {:ok, holder} = Agent.start_link(fn -> nil end)

      {:ok, {src_id, _dst_id}} =
        NamespaceCoordinator.claim_rename_for(server, "/explicit-src", "/explicit-dst", holder)

      assert {:error, :conflict, ^src_id} =
               NamespaceCoordinator.claim_path(server, "/explicit-src", :exclusive)
    end
  end

  # `claim_*_for/4` exists for cross-node callers (e.g. WebDAV via
  # `NeonFS.Client.Router.call/4`): the RPC handler `self()` would die
  # the moment the call returns and take every claim with it. Explicit
  # holder lets callers pass a long-lived pid on their own node so the
  # coordinator monitors something stable. See sub-issue #301.
  describe "claim_path_for/4 / claim_subtree_for/4" do
    test "monitors the explicit holder pid, not the caller", %{server: server} do
      parent = self()

      {holder, monitor_ref} =
        spawn_monitor(fn ->
          send(parent, :ready)

          receive do
            :exit -> :ok
          end
        end)

      assert_receive :ready, 1_000

      {:ok, claim_id} =
        NamespaceCoordinator.claim_subtree_for(server, "/explicit", :exclusive, holder)

      # The caller (this test process) is alive but irrelevant — the
      # coordinator monitors `holder`.
      assert {:error, :conflict, ^claim_id} =
               NamespaceCoordinator.claim_path(server, "/explicit/x", :exclusive)

      send(holder, :exit)
      assert_receive {:DOWN, ^monitor_ref, :process, ^holder, _}, 1_000
      :sys.get_state(server)

      assert {:ok, _} = NamespaceCoordinator.claim_path(server, "/explicit/x", :exclusive)
    end

    test "claim_path_for/4 takes the same path-vs-path semantics", %{server: server} do
      {:ok, holder} = Agent.start_link(fn -> nil end)

      assert {:ok, _} =
               NamespaceCoordinator.claim_path_for(server, "/p", :exclusive, holder)

      assert {:error, :conflict, _} =
               NamespaceCoordinator.claim_path_for(server, "/p", :exclusive, holder)
    end

    test "rejects non-pid holders", %{server: server} do
      assert_raise FunctionClauseError, fn ->
        NamespaceCoordinator.claim_subtree_for(server, "/x", :exclusive, :not_a_pid)
      end
    end
  end
end
