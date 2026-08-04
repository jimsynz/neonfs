defmodule NeonFS.FUSE.IntegrationTest.UnlinkWhileOpenTest do
  @moduledoc """
  End-to-end peer-cluster test for the POSIX unlink-while-open story
  Composes everything that landed in
  earlier slices:

    * `:pinned` namespace claim primitive.
    * `FileMeta` `:detached` state + `delete_file` pin-check.
    * Pin-release-triggered GC.
    * `Core.read_file_by_id` / `write_file_at_by_id`.
    * FUSE handler pin lifecycle + `file_id`-keyed read/write.

  ## Cluster shape

  One core peer (`node1: :neonfs_core`) holds the metadata and chunks.
  The "two FUSE peers" of the issue's scope sketch collapse to a
  single test-runner BEAM running the FUSE `Handler` GenServer, plus
  direct `Core.delete_file` / `Core.get_file_meta` calls to simulate
  the second peer's unlinker / lookuper. The streaming RPC path —
  Handler → Router → core RPC → Ra-coordinated state — is the
  subject under test, and it's observable end-to-end with one
  Handler and one core. A second Handler would only test a different
  pid as the holder, which the existing `claim_pinned`-via-RPC
  primitive already covers in
  `neonfs_integration/test/integration/namespace_coordinator_pinned_test.exs`.
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  import NeonFS.FUSE.TestSupport.HandlerOp, only: [op_timeout: 0]

  alias NeonFS.Client.{Connection, CostFunction, Discovery, Router}
  alias NeonFS.Core.FileIndex
  alias NeonFS.FUSE.{Handler, InodeTable}

  @moduletag timeout: 180_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = cluster_init_idempotent(cluster, :node1, "unlink-while-open")

    :ok = wait_for_cluster_stable(cluster)

    volume_name = "unlink-while-open-vol-#{System.unique_integer([:positive])}"

    # Compression off + durability=1 so the chunk fetch path
    # exercises the data-plane (not the Erlang-RPC fallback) and so
    # quorum writes are happy with the single core peer.
    volume_opts = %{
      compression: %{algorithm: :none, level: 0, min_size: 0},
      durability: %{type: :replicate, factor: 1, min_copies: 1}
    }

    {:ok, volume_map} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume_name,
        volume_opts
      ])

    %{volume_id: volume_map[:id], volume_name: volume_name}
  end

  setup %{cluster: cluster, volume_id: volume_id, volume_name: volume_name} do
    core_node = PeerCluster.get_node!(cluster, :node1).node

    start_supervised!({Connection, bootstrap_nodes: [core_node]})
    start_supervised!(Discovery)
    start_supervised!(CostFunction)
    start_supervised!(InodeTable)

    :ok = wait_until(fn -> match?({:ok, _}, Connection.connected_core_node()) end)

    :ok =
      wait_until(
        fn -> match?([_ | _], Discovery.get_core_nodes()) end,
        timeout: 10_000
      )

    handler =
      start_supervised!(
        {Handler, volume: volume_id, volume_name: volume_name, test_notify: self()}
      )

    {:ok, parent_inode} = InodeTable.allocate_inode(volume_id, "/")

    {:ok,
     handler: handler, parent_inode: parent_inode, volume_id: volume_id, volume_name: volume_name}
  end

  describe "unlink-while-open across the BEAM FUSE stack" do
    test "open + unlink-from-elsewhere + read-via-fh + release purges chunks", ctx do
      %{
        handler: handler,
        parent_inode: parent_inode,
        volume_id: volume_id,
        volume_name: volume_name
      } = ctx

      # ── Step 1: create + write the file via the FUSE handler.
      file_name = "doomed.bin"
      file_path = "/" <> file_name
      payload = "the bytes that survive the unlink"

      send(
        handler,
        {:fuse_op, 1,
         {"create",
          %{
            "parent" => parent_inode,
            "name" => file_name,
            "mode" => 0o644,
            "flags" => 0
          }}}
      )

      assert_receive {:fuse_op_complete, 1,
                      {"entry_ok", %{"ino" => file_inode, "fh" => create_fh}}},
                     op_timeout()

      assert is_integer(create_fh) and create_fh >= 1

      send(
        handler,
        {:fuse_op, 2,
         {"write", %{"ino" => file_inode, "offset" => 0, "data" => payload, "fh" => create_fh}}}
      )

      assert_receive {:fuse_op_complete, 2, {"write_ok", %{"size" => size}}}, op_timeout()
      assert size == byte_size(payload)

      # Capture the file_id off the wire — Core's RPC returns a
      # FileMeta from get_by_path that we use to compare against
      # the post-unlink-by-id lookups.
      assert {:ok, %{id: file_id}} =
               Router.call(NeonFS.Core, :get_file_meta, [volume_name, file_path])

      # ── Step 2: open the file again with a fresh fh — same path,
      # different open. Mirrors what a second `cat` would do; the
      # claim id from `create_fh` is not the same as the one this
      # `open` pins. (The `release` later drops the open's pin; the
      # create's pin hangs on until the GenServer dies.)
      send(handler, {:fuse_op, 3, {"open", %{"ino" => file_inode}}})
      assert_receive {:fuse_op_complete, 3, {"open_ok", %{"fh" => read_fh}}}, op_timeout()
      assert read_fh != create_fh

      # ── Step 3: read via the open fh — sanity check.
      send(
        handler,
        {:fuse_op, 4,
         {"read", %{"ino" => file_inode, "offset" => 0, "size" => 1024, "fh" => read_fh}}}
      )

      assert_receive {:fuse_op_complete, 4, {"read_ok", %{"data" => ^payload}}}, op_timeout()

      # ── Step 4: from "node3" (i.e. directly via Core RPC, simulating
      # an unlink issued by another FUSE peer), delete the file.
      assert :ok =
               Router.call(NeonFS.Core, :delete_file, [volume_name, file_path])

      # ── Step 5a: path-based lookup goes 404 from "node3"'s view.
      assert {:error, %{class: :not_found}} =
               Router.call(NeonFS.Core, :get_file_meta, [volume_name, file_path])

      # ── Step 5b: file_id-based read still works through the open
      # fh — chunks are reachable while the pin holds.
      send(
        handler,
        {:fuse_op, 5,
         {"read", %{"ino" => file_inode, "offset" => 0, "size" => 1024, "fh" => read_fh}}}
      )

      assert_receive {:fuse_op_complete, 5, {"read_ok", %{"data" => ^payload}}}, op_timeout()

      # ── Step 5c: a fresh `open` of the same path MUST see ENOENT —
      # the file is detached (no directory entry). This is the
      # opposite-invariant sanity check from the issue's scope.
      send(
        handler,
        {:fuse_op, 6, {"lookup", %{"parent" => parent_inode, "name" => file_name}}}
      )

      assert_receive {:fuse_op_complete, 6, {"error", %{"errno" => 2}}}, op_timeout()

      # ── Step 6: release the read fh. The handler drops its
      # `:pinned` claim. `create_fh` still holds another pin, so
      # the file remains detached but reachable by id.
      send(handler, {:fuse_op, 7, {"release", %{"fh" => read_fh}}})
      assert_receive {:fuse_op_complete, 7, {"ok", %{}}}, op_timeout()

      # File still exists by id (create_fh's pin is alive).
      assert {:ok, %{detached: true, id: ^file_id}} =
               Router.call(FileIndex, :get, [volume_id, file_id])

      # ── Step 7: release the create fh too — last pin drops, GC
      # fires through the release-telemetry handler.
      send(handler, {:fuse_op, 8, {"release", %{"fh" => create_fh}}})
      assert_receive {:fuse_op_complete, 8, {"ok", %{}}}, op_timeout()

      # Wait for the pin-release telemetry to propagate across Ra
      # commit + DetachedFileGC handler. Local poll suffices —
      # there's only one core peer.
      assert :ok =
               wait_until(
                 fn ->
                   match?(
                     {:error, :not_found},
                     Router.call(FileIndex, :get, [volume_id, file_id])
                   )
                 end,
                 timeout: op_timeout()
               )
    end

    # The case above unlinks via `Core.delete_file` directly, standing in
    # for another peer. That left the mount's *own* `unlink` untested, and
    # it was the one path that got it wrong: it deleted by file id through
    # `FileIndex.delete/1`, skipping the facade that checks `:pinned`
    # claims, so a file unlinked through the mount lost its chunks out from
    # under an open fd.
    test "unlink issued through the mount detaches rather than hard-deleting", ctx do
      %{
        handler: handler,
        parent_inode: parent_inode,
        volume_id: volume_id,
        volume_name: volume_name
      } = ctx

      file_name = "unlinked-by-fuse.bin"
      file_path = "/" <> file_name
      payload = "bytes an open fd must still see"

      send(
        handler,
        {:fuse_op, 1,
         {"create",
          %{"parent" => parent_inode, "name" => file_name, "mode" => 0o644, "flags" => 0}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", %{"ino" => file_inode, "fh" => fh}}},
                     op_timeout()

      send(
        handler,
        {:fuse_op, 2,
         {"write", %{"ino" => file_inode, "offset" => 0, "data" => payload, "fh" => fh}}}
      )

      assert_receive {:fuse_op_complete, 2, {"write_ok", %{}}}, op_timeout()

      assert {:ok, %{id: file_id}} =
               Router.call(NeonFS.Core, :get_file_meta, [volume_name, file_path])

      # The unlink under test: the FUSE opcode, with the handle still open.
      send(
        handler,
        {:fuse_op, 3, {"unlink", %{"parent" => parent_inode, "name" => file_name}}}
      )

      assert_receive {:fuse_op_complete, 3, {"ok", %{}}}, op_timeout()

      # Detached, not deleted — the assertion that failed before the fix.
      assert {:ok, %{detached: true, id: ^file_id}} =
               Router.call(FileIndex, :get, [volume_id, file_id])

      # The name is gone, so a path lookup must miss.
      assert {:error, %{class: :not_found}} =
               Router.call(NeonFS.Core, :get_file_meta, [volume_name, file_path])

      # And the open handle still reads its bytes.
      send(
        handler,
        {:fuse_op, 4, {"read", %{"ino" => file_inode, "offset" => 0, "size" => 1024, "fh" => fh}}}
      )

      assert_receive {:fuse_op_complete, 4, {"read_ok", %{"data" => ^payload}}}, op_timeout()

      # Last close drops the pin and the detached metadata is collected.
      send(handler, {:fuse_op, 5, {"release", %{"fh" => fh}}})
      assert_receive {:fuse_op_complete, 5, {"ok", %{}}}, op_timeout()

      assert :ok =
               wait_until(
                 fn ->
                   match?(
                     {:error, :not_found},
                     Router.call(FileIndex, :get, [volume_id, file_id])
                   )
                 end,
                 timeout: op_timeout()
               )
    end

    # The pin is keyed by file identity, so a rename cannot strand it —
    # but that only helps if the operations reached through the handle
    # stop resolving the old name. `read` and `getattr` both did, so a
    # rename issued anywhere in the cluster broke a perfectly good fd.
    test "operations through an open handle survive a rename", ctx do
      %{
        handler: handler,
        parent_inode: parent_inode,
        volume_name: volume_name
      } = ctx

      old_name = "before-rename.bin"
      new_name = "after-rename.bin"
      payload = "bytes that outlive the name"

      send(
        handler,
        {:fuse_op, 1,
         {"create",
          %{"parent" => parent_inode, "name" => old_name, "mode" => 0o644, "flags" => 0}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", %{"ino" => file_inode, "fh" => fh}}},
                     op_timeout()

      send(
        handler,
        {:fuse_op, 2,
         {"write", %{"ino" => file_inode, "offset" => 0, "data" => payload, "fh" => fh}}}
      )

      assert_receive {:fuse_op_complete, 2, {"write_ok", %{}}}, op_timeout()

      # Rename from elsewhere in the cluster, while the handle is open.
      assert :ok =
               Router.call(NeonFS.Core, :rename_file, [
                 volume_name,
                 "/" <> old_name,
                 "/" <> new_name
               ])

      # The old name is gone, so anything that resolves by path must miss.
      assert {:error, %{class: :not_found}} =
               Router.call(NeonFS.Core, :get_file_meta, [volume_name, "/" <> old_name])

      # Read through the original handle — by file id, over the data plane.
      send(
        handler,
        {:fuse_op, 3, {"read", %{"ino" => file_inode, "offset" => 0, "size" => 1024, "fh" => fh}}}
      )

      assert_receive {:fuse_op_complete, 3, {"read_ok", %{"data" => ^payload}}}, op_timeout()

      # `getattr` through the handle: the kernel sets FUSE_GETATTR_FH, so
      # the handler answers from the file id rather than the stale inode.
      send(handler, {:fuse_op, 4, {"getattr", %{"ino" => file_inode, "fh" => fh}}})

      assert_receive {:fuse_op_complete, 4, {"attr_ok", %{"size" => size}}}, op_timeout()
      assert size == byte_size(payload)

      # A write through the handle. `write_dispatch/5` routes a registered
      # `fh` to the by-id facade, so the append lands on the file the
      # handle was opened on — the name it was opened *under* is gone.
      appended = "and bytes appended after it"
      grown = payload <> appended

      send(
        handler,
        {:fuse_op, 5,
         {"write",
          %{
            "ino" => file_inode,
            "offset" => byte_size(payload),
            "data" => appended,
            "fh" => fh
          }}}
      )

      assert_receive {:fuse_op_complete, 5, {"write_ok", %{}}}, op_timeout()

      send(
        handler,
        {:fuse_op, 6, {"read", %{"ino" => file_inode, "offset" => 0, "size" => 1024, "fh" => fh}}}
      )

      assert_receive {:fuse_op_complete, 6, {"read_ok", %{"data" => ^grown}}}, op_timeout()

      # Resolving the *new* name is what proves the by-id write hit the
      # renamed file rather than recreating the old one: a path-routed
      # write would have left this size at the pre-append value.
      assert {:ok, %{size: grown_size}} =
               Router.call(NeonFS.Core, :get_file_meta, [volume_name, "/" <> new_name])

      assert grown_size == byte_size(grown)

      # `fsync` keys off the tracked `file_id` as well, so the durability
      # barrier has to find the file under a name it has never seen.
      send(handler, {:fuse_op, 7, {"fsync", %{"fh" => fh}}})
      assert_receive {:fuse_op_complete, 7, {"ok", %{}}}, op_timeout()

      # And a truncate through the handle, which routes to the by-id facade.
      send(
        handler,
        {:fuse_op, 8, {"setattr", %{"ino" => file_inode, "fh" => fh, "size" => 4}}}
      )

      assert_receive {:fuse_op_complete, 8, {"attr_ok", %{"size" => 4}}}, op_timeout()

      send(handler, {:fuse_op, 9, {"release", %{"fh" => fh}}})
      assert_receive {:fuse_op_complete, 9, {"ok", %{}}}, op_timeout()
    end

    # A directory created through the mount used to be a `file:` record with
    # `S_IFDIR` in its mode and a `type: :file` dirent, so `plan_rmdir/2`
    # refused it and no other interface could remove it — while a directory
    # created anywhere else was a `dir:` record that FUSE's own rmdir could
    # not remove either. Both directions now work because there is one
    # representation.
    test "a directory created through the mount is removable from elsewhere", ctx do
      %{
        handler: handler,
        parent_inode: parent_inode,
        volume_id: volume_id,
        volume_name: volume_name
      } =
        ctx

      send(
        handler,
        {:fuse_op, 1,
         {"mkdir", %{"parent" => parent_inode, "name" => "made-by-fuse", "mode" => 0o755}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", %{"kind" => "directory"}}}, op_timeout()

      # The dirent must say `:dir`, which is what every other interface reads.
      assert {:ok, children} = Router.call(FileIndex, :list_dir, [volume_id, "/"])
      assert children["made-by-fuse"].type == :dir

      # And core can remove it — this returned {:error, :not_a_directory}
      # when the mount made directories out of files.
      assert :ok = Router.call(NeonFS.Core, :delete_file, [volume_name, "/made-by-fuse"])

      assert {:error, %{class: :not_found}} =
               Router.call(NeonFS.Core, :get_file_meta, [volume_name, "/made-by-fuse"])
    end

    test "a directory created elsewhere is removable through the mount", ctx do
      %{handler: handler, parent_inode: parent_inode, volume_id: volume_id} = ctx

      assert {:ok, _} = Router.call(FileIndex, :mkdir, [volume_id, "/made-by-core"])
      {:ok, _inode} = InodeTable.allocate_inode(volume_id, "/made-by-core")

      send(
        handler,
        {:fuse_op, 2, {"rmdir", %{"parent" => parent_inode, "name" => "made-by-core"}}}
      )

      assert_receive {:fuse_op_complete, 2, {"ok", %{}}}, op_timeout()

      assert {:ok, children} = Router.call(FileIndex, :list_dir, [volume_id, "/"])
      refute Map.has_key?(children, "made-by-core")
    end
  end
end
