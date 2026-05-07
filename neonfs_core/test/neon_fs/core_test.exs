defmodule NeonFS.CoreTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core
  alias NeonFS.Core.{FileIndex, NamespaceCoordinator, RaServer, VolumeRegistry}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_drive_registry()
    start_blob_store()
    start_chunk_index()
    start_file_index()
    start_stripe_index()
    start_volume_registry()
    ensure_chunk_access_tracker()

    on_exit(fn -> cleanup_test_dirs() end)

    vol_name = "facade-test-#{:rand.uniform(999_999)}"
    {:ok, volume} = VolumeRegistry.create(vol_name, [])

    {:ok, volume: volume, volume_name: vol_name}
  end

  # --- Volume operations ---

  describe "list_volumes/0" do
    test "returns all volumes", %{volume_name: vol_name} do
      {:ok, volumes} = Core.list_volumes()
      names = Enum.map(volumes, & &1.name)
      assert vol_name in names
    end
  end

  describe "get_volume/1" do
    test "returns volume by name", %{volume_name: vol_name} do
      assert {:ok, volume} = Core.get_volume(vol_name)
      assert volume.name == vol_name
    end

    test "returns error for unknown volume" do
      assert {:error, :not_found} = Core.get_volume("nonexistent-volume")
    end
  end

  describe "create_volume/1" do
    test "creates a new volume" do
      name = "create-test-#{:rand.uniform(999_999)}"
      assert {:ok, volume} = Core.create_volume(name)
      assert volume.name == name
    end
  end

  describe "create_volume/2" do
    test "creates a volume with options" do
      name = "create-opts-#{:rand.uniform(999_999)}"
      assert {:ok, volume} = Core.create_volume(name, replication_factor: 1)
      assert volume.name == name
    end
  end

  describe "delete_volume/1" do
    test "deletes volume by name" do
      name = "delete-test-#{:rand.uniform(999_999)}"
      {:ok, _} = VolumeRegistry.create(name, [])
      assert :ok = Core.delete_volume(name)
      assert {:error, :not_found} = Core.get_volume(name)
    end

    test "returns error for unknown volume" do
      assert {:error, :not_found} = Core.delete_volume("nonexistent-volume")
    end
  end

  describe "volume_exists?/1" do
    test "returns true for existing volume", %{volume_name: vol_name} do
      assert Core.volume_exists?(vol_name)
    end

    test "returns false for nonexistent volume" do
      refute Core.volume_exists?("nonexistent-volume")
    end
  end

  # --- File operations ---

  describe "write_file/3 and read_file/3" do
    test "round-trips file content", %{volume_name: vol_name} do
      content = "Hello from the facade"
      assert {:ok, meta} = Core.write_file_streamed(vol_name, "/test.txt", [content])
      assert meta.path == "/test.txt"

      assert {:ok, ^content} = Core.read_file(vol_name, "/test.txt", [])
    end
  end

  describe "write_file/4" do
    test "writes file with content type option", %{volume_name: vol_name} do
      assert {:ok, meta} =
               Core.write_file_streamed(vol_name, "/typed.html", ["<h1>Hi</h1>"],
                 content_type: "text/html"
               )

      assert meta.content_type == "text/html"
    end
  end

  describe "read_file/3 with offset and length" do
    test "reads partial content from a file", %{volume_name: vol_name} do
      {:ok, _} = Core.write_file_streamed(vol_name, "/partial.txt", ["0123456789"])
      assert {:ok, "345"} = Core.read_file(vol_name, "/partial.txt", offset: 3, length: 3)
    end

    test "reads from offset to end when length is :all", %{volume_name: vol_name} do
      {:ok, _} = Core.write_file_streamed(vol_name, "/tail.txt", ["ABCDEFGHIJ"])
      assert {:ok, "FGHIJ"} = Core.read_file(vol_name, "/tail.txt", offset: 5, length: :all)
    end

    test "returns error for nonexistent volume" do
      assert {:error, :not_found} = Core.read_file("no-such-volume", "/file.txt", offset: 0)
    end
  end

  describe "read_file/3 errors" do
    test "returns error for nonexistent volume" do
      assert {:error, :not_found} = Core.read_file("no-such-volume", "/file.txt", [])
    end
  end

  # `_by_id` variants resolve through `FileIndex.get/1` instead of the
  # path-based dir-entry walk, so they keep working when a path has
  # been detached by another peer's `delete_file`. The unlink-while-
  # open story (#638 / #644) needs this so FUSE / NFSv4 fd holders
  # can drain their cached handle even after another peer unlinks the
  # path.
  describe "read_file_by_id/3" do
    test "reads file content keyed by file_id", %{volume_name: vol_name} do
      {:ok, %{id: id}} =
        Core.write_file_streamed(vol_name, "/by-id.txt", ["abcdef"])

      assert {:ok, "abcdef"} = Core.read_file_by_id(vol_name, id)
    end

    test "honours offset and length", %{volume_name: vol_name} do
      {:ok, %{id: id}} =
        Core.write_file_streamed(vol_name, "/by-id-range.txt", ["0123456789"])

      assert {:ok, "345"} = Core.read_file_by_id(vol_name, id, offset: 3, length: 3)
    end

    test "still resolves a detached file", %{volume_name: vol_name} do
      {:ok, %{id: id}} =
        Core.write_file_streamed(vol_name, "/detached-read.txt", ["payload"])

      :ok = mark_detached_directly(id)

      # Path-based read goes 404, but file_id read still works — that's
      # the unlink-while-open invariant.
      assert {:error, _} = Core.read_file(vol_name, "/detached-read.txt")
      assert {:ok, "payload"} = Core.read_file_by_id(vol_name, id)
    end

    test "returns :wrong_volume for a file_id from another volume",
         %{volume_name: vol_name} do
      other = "other-vol-#{:rand.uniform(999_999)}"
      {:ok, _} = VolumeRegistry.create(other, [])
      {:ok, %{id: id}} = Core.write_file_streamed(other, "/elsewhere.txt", ["x"])

      assert {:error, :wrong_volume} = Core.read_file_by_id(vol_name, id)
    end

    test "returns error for an unknown file_id", %{volume_name: vol_name} do
      assert {:error, _} = Core.read_file_by_id(vol_name, "nonexistent-id")
    end

    test "returns error for a nonexistent volume" do
      assert {:error, :not_found} = Core.read_file_by_id("no-such-volume", "any-id")
    end
  end

  describe "read_file_refs_by_id/3" do
    test "returns file refs keyed by file_id", %{volume_name: vol_name} do
      {:ok, %{id: id}} =
        Core.write_file_streamed(vol_name, "/refs-by-id.txt", ["bytes"])

      assert {:ok, %{file_size: 5, chunks: chunks}} =
               Core.read_file_refs_by_id(vol_name, id)

      assert is_list(chunks)
    end
  end

  describe "write_file_at_by_id/5" do
    test "writes bytes at offset against an existing file_id",
         %{volume_name: vol_name} do
      {:ok, %{id: id}} =
        Core.write_file_streamed(vol_name, "/wb-id.txt", ["aaaaaaaa"])

      assert {:ok, _} = Core.write_file_at_by_id(vol_name, id, 2, "BB")
      assert {:ok, "aaBBaaaa"} = Core.read_file_by_id(vol_name, id)
    end

    test "rejects writes against a file_id from another volume",
         %{volume_name: vol_name} do
      other = "other-vol-write-#{:rand.uniform(999_999)}"
      {:ok, _} = VolumeRegistry.create(other, [])
      {:ok, %{id: id}} = Core.write_file_streamed(other, "/sister.txt", ["x"])

      assert {:error, :wrong_volume} = Core.write_file_at_by_id(vol_name, id, 0, "y")
    end

    test "returns :not_found for an unknown file_id", %{volume_name: vol_name} do
      assert {:error, :not_found} = Core.write_file_at_by_id(vol_name, "nonexistent-id", 0, "x")
    end
  end

  defp mark_detached_directly(file_id) do
    case FileIndex.mark_detached(file_id, ["sentinel-pin"]) do
      {:ok, _} -> :ok
      other -> flunk("mark_detached failed: #{inspect(other)}")
    end
  end

  describe "delete_file/2" do
    test "deletes a file", %{volume_name: vol_name} do
      {:ok, _} = Core.write_file_streamed(vol_name, "/to-delete.txt", ["data"])
      assert :ok = Core.delete_file(vol_name, "/to-delete.txt")
      assert {:error, :not_found} = Core.get_file_meta(vol_name, "/to-delete.txt")
    end

    test "returns error for nonexistent file", %{volume_name: vol_name} do
      assert {:error, :not_found} = Core.delete_file(vol_name, "/missing.txt")
    end
  end

  describe "get_file_meta/2" do
    test "returns metadata for a file", %{volume_name: vol_name} do
      {:ok, _} = Core.write_file_streamed(vol_name, "/meta-test.txt", ["content"])
      assert {:ok, meta} = Core.get_file_meta(vol_name, "/meta-test.txt")
      assert meta.path == "/meta-test.txt"
      assert meta.size == byte_size("content")
    end

    test "returns error for nonexistent file", %{volume_name: vol_name} do
      assert {:error, :not_found} = Core.get_file_meta(vol_name, "/nope.txt")
    end
  end

  describe "list_files_recursive/2" do
    test "lists files under path", %{volume_name: vol_name} do
      {:ok, _} = Core.write_file_streamed(vol_name, "/a.txt", ["aaa"])
      {:ok, _} = Core.write_file_streamed(vol_name, "/b.txt", ["bbb"])

      assert {:ok, entries} = Core.list_files_recursive(vol_name, "/")
      paths = Enum.map(entries, & &1.path)
      assert "/a.txt" in paths
      assert "/b.txt" in paths
    end

    test "lists files in subdirectory", %{volume_name: vol_name} do
      {:ok, _} = Core.mkdir(vol_name, "/docs")
      {:ok, _} = Core.write_file_streamed(vol_name, "/docs/readme.txt", ["read me"])

      assert {:ok, entries} = Core.list_files_recursive(vol_name, "/docs")
      paths = Enum.map(entries, & &1.path)
      assert "/docs/readme.txt" in paths
    end

    test "returns empty list for path with no files", %{volume_name: vol_name} do
      {:ok, _} = Core.mkdir(vol_name, "/empty")
      assert {:ok, []} = Core.list_files_recursive(vol_name, "/empty")
    end

    test "includes nested files at any depth", %{volume_name: vol_name} do
      {:ok, _} = Core.mkdir(vol_name, "/parent")
      {:ok, _} = Core.mkdir(vol_name, "/parent/child")
      {:ok, _} = Core.write_file_streamed(vol_name, "/parent/child/deep.txt", ["deep"])

      assert {:ok, entries} = Core.list_files_recursive(vol_name, "/parent")
      paths = Enum.map(entries, & &1.path)
      assert "/parent/child/deep.txt" in paths
    end

    test "normalises path without leading slash", %{volume_name: vol_name} do
      {:ok, _} = Core.write_file_streamed(vol_name, "/file.txt", ["data"])
      assert {:ok, [_ | _]} = Core.list_files_recursive(vol_name, "/")
    end

    test "returns all descendants while list_dir returns only direct children", %{
      volume_name: vol_name
    } do
      {:ok, _} = Core.mkdir(vol_name, "/sub")
      {:ok, _} = Core.mkdir(vol_name, "/sub/deep")
      {:ok, _} = Core.write_file_streamed(vol_name, "/top.txt", ["top"])
      {:ok, _} = Core.write_file_streamed(vol_name, "/sub/mid.txt", ["mid"])
      {:ok, _} = Core.write_file_streamed(vol_name, "/sub/deep/bottom.txt", ["bottom"])

      {:ok, recursive_entries} = Core.list_files_recursive(vol_name, "/")
      recursive_paths = Enum.map(recursive_entries, & &1.path) |> Enum.sort()

      assert "/sub/deep/bottom.txt" in recursive_paths
      assert "/sub/mid.txt" in recursive_paths
      assert "/top.txt" in recursive_paths

      {:ok, dir_entries} = Core.list_dir(vol_name, "/")
      dir_paths = Enum.map(dir_entries, & &1.path) |> Enum.sort()

      assert "/top.txt" in dir_paths
      assert "/sub" in dir_paths
      refute "/sub/mid.txt" in dir_paths
      refute "/sub/deep/bottom.txt" in dir_paths
    end
  end

  describe "mkdir/2" do
    test "creates a directory", %{volume_name: vol_name} do
      assert {:ok, _dir_entry} = Core.mkdir(vol_name, "/new-dir")
    end

    test "returns error for nonexistent volume" do
      assert {:error, :not_found} = Core.mkdir("no-such-volume", "/dir")
    end
  end

  # `mkdir` and `delete_file` (for directories) are wrapped in
  # `NamespaceCoordinator` claims — sub-issue #305. The default test
  # setup stops Ra, so this describe block re-enables it to exercise
  # the coordinator-integration path that the unit tests above can't
  # reach.
  describe "namespace coordinator integration (mkdir/delete_file)" do
    setup %{volume: volume} do
      ensure_node_named()
      start_ra()
      :ok = RaServer.init_cluster()
      {:ok, _} = NamespaceCoordinator.start_link()

      on_exit(fn ->
        case Process.whereis(NamespaceCoordinator) do
          nil -> :ok
          pid -> GenServer.stop(pid, :shutdown, 1_000)
        end
      end)

      {:ok, volume_id: volume.id}
    end

    test "mkdir refuses with :busy when the coordinator already pins the path",
         %{volume_name: vol_name, volume_id: volume_id} do
      key = "vol:" <> volume_id <> ":/contended"

      {:ok, _claim} =
        NamespaceCoordinator.claim_path(
          NamespaceCoordinator,
          key,
          :exclusive
        )

      assert {:error, :busy} = Core.mkdir(vol_name, "/contended")
    end

    test "mkdir releases the claim on success", %{volume_name: vol_name, volume_id: volume_id} do
      assert {:ok, _} = Core.mkdir(vol_name, "/released")

      # The pre-claim succeeds only because mkdir's claim was released
      # in the `after` clause.
      key = "vol:" <> volume_id <> ":/released"

      assert {:ok, _claim} =
               NamespaceCoordinator.claim_path(
                 NamespaceCoordinator,
                 key,
                 :exclusive
               )
    end

    test "delete_file takes a subtree claim — refuses while a descendant is held",
         %{volume_name: vol_name, volume_id: volume_id} do
      {:ok, _} = Core.mkdir(vol_name, "/contended-dir")
      {:ok, _} = Core.write_file_streamed(vol_name, "/contended-dir/file.txt", ["data"])

      # `delete_file` will try `claim_subtree`, which conflicts with
      # any descendant path claim. Pre-take that descendant claim.
      key = "vol:" <> volume_id <> ":/contended-dir/file.txt"

      {:ok, _claim} =
        NamespaceCoordinator.claim_path(
          NamespaceCoordinator,
          key,
          :exclusive
        )

      assert {:error, :busy} = Core.delete_file(vol_name, "/contended-dir")
    end

    test "delete_file releases the subtree claim on success", %{volume_name: vol_name} do
      {:ok, _} = Core.write_file_streamed(vol_name, "/cleanup.txt", ["data"])

      assert :ok = Core.delete_file(vol_name, "/cleanup.txt")

      # The subtree claim on /cleanup.txt is released, so a fresh
      # mkdir on the same path can take its own path claim.
      assert {:ok, _} = Core.mkdir(vol_name, "/cleanup.txt")
    end

    # File delete (POSIX unlink-while-open, #643 of #638) takes a
    # `:shared :path` claim so a live `:pinned` claim on the same
    # path doesn't block delete — instead, delete tombstones the
    # FileMeta in-place so open handles keep working until they
    # close.
    test "file delete with no pins runs the full-delete path",
         %{volume_name: vol_name} do
      {:ok, %{id: id, volume_id: volume_id}} =
        Core.write_file_streamed(vol_name, "/full-delete.txt", ["data"])

      assert :ok = Core.delete_file(vol_name, "/full-delete.txt")

      assert {:error, :not_found} = Core.get_file_meta(vol_name, "/full-delete.txt")
      assert {:error, :not_found} = FileIndex.get(volume_id, id)
    end

    test "file delete with a live pin tombstones the FileMeta in place",
         %{volume_name: vol_name, volume_id: volume_id} do
      {:ok, %{id: file_id}} =
        Core.write_file_streamed(vol_name, "/pinned.txt", ["data"])

      key = "vol:" <> volume_id <> ":/pinned.txt"

      {:ok, holder} = Agent.start_link(fn -> nil end)

      {:ok, pin_id} =
        NamespaceCoordinator.claim_pinned_for(NamespaceCoordinator, key, holder)

      try do
        assert :ok = Core.delete_file(vol_name, "/pinned.txt")

        # Path-based access goes 404.
        assert {:error, :not_found} = Core.get_file_meta(vol_name, "/pinned.txt")

        # File-id-based access still works — the FileMeta is detached.
        assert {:ok, %{detached: true, pinned_claim_ids: pin_ids}} =
                 FileIndex.get(volume_id, file_id)

        assert pin_id in pin_ids
      after
        # Releasing the pin doesn't auto-GC yet (#644). Manual cleanup.
        :ok = NamespaceCoordinator.release(NamespaceCoordinator, pin_id)
        Agent.stop(holder, :normal, 1_000)
      end
    end

    test "file delete is idempotent on an already-detached file",
         %{volume_name: vol_name, volume_id: volume_id} do
      {:ok, _} = Core.write_file_streamed(vol_name, "/dup-delete.txt", ["data"])

      key = "vol:" <> volume_id <> ":/dup-delete.txt"
      {:ok, holder} = Agent.start_link(fn -> nil end)
      {:ok, pin_id} = NamespaceCoordinator.claim_pinned_for(NamespaceCoordinator, key, holder)

      try do
        assert :ok = Core.delete_file(vol_name, "/dup-delete.txt")
        assert {:error, :not_found} = Core.delete_file(vol_name, "/dup-delete.txt")
      after
        :ok = NamespaceCoordinator.release(NamespaceCoordinator, pin_id)
        Agent.stop(holder, :normal, 1_000)
      end
    end

    test "concurrent exclusive path claim still blocks file delete (rename guard)",
         %{volume_name: vol_name, volume_id: volume_id} do
      # `:shared :path` for delete coexists with `:pinned` but still
      # conflicts with a rename's `:exclusive :path` on the same
      # path — the same matrix protects against partial-rename races.
      {:ok, _} = Core.write_file_streamed(vol_name, "/guarded.txt", ["data"])

      key = "vol:" <> volume_id <> ":/guarded.txt"

      {:ok, _claim} =
        NamespaceCoordinator.claim_path(NamespaceCoordinator, key, :exclusive)

      assert {:error, :busy} = Core.delete_file(vol_name, "/guarded.txt")
    end
  end

  describe "rename_file/3" do
    test "renames file in same directory", %{volume_name: vol_name} do
      {:ok, _} = Core.write_file_streamed(vol_name, "/old-name.txt", ["data"])
      assert :ok = Core.rename_file(vol_name, "/old-name.txt", "/new-name.txt")
      assert {:error, :not_found} = Core.get_file_meta(vol_name, "/old-name.txt")
      assert {:ok, _} = Core.get_file_meta(vol_name, "/new-name.txt")
    end

    test "moves file to different directory", %{volume_name: vol_name} do
      {:ok, _} = Core.mkdir(vol_name, "/src")
      {:ok, _} = Core.mkdir(vol_name, "/dst")
      {:ok, _} = Core.write_file_streamed(vol_name, "/src/file.txt", ["data"])

      assert :ok = Core.rename_file(vol_name, "/src/file.txt", "/dst/file.txt")
      assert {:error, :not_found} = Core.get_file_meta(vol_name, "/src/file.txt")
      assert {:ok, _} = Core.get_file_meta(vol_name, "/dst/file.txt")
    end

    test "moves and renames file", %{volume_name: vol_name} do
      {:ok, _} = Core.mkdir(vol_name, "/from")
      {:ok, _} = Core.mkdir(vol_name, "/to")
      {:ok, _} = Core.write_file_streamed(vol_name, "/from/original.txt", ["data"])

      assert :ok = Core.rename_file(vol_name, "/from/original.txt", "/to/renamed.txt")
      assert {:error, :not_found} = Core.get_file_meta(vol_name, "/from/original.txt")
      assert {:ok, _} = Core.get_file_meta(vol_name, "/to/renamed.txt")
    end

    test "returns error for nonexistent volume" do
      assert {:error, :not_found} = Core.rename_file("no-volume", "/a.txt", "/b.txt")
    end
  end

  # --- S3 credential operations ---

  describe "lookup_s3_credential/1" do
    # S3CredentialManager reads via Ra since #347; the outer setup
    # stops Ra, so this describe re-enables it.
    setup do
      ensure_node_named()
      start_ra()
      :ok = RaServer.init_cluster()
      :ok
    end

    test "returns error for unknown credential" do
      assert {:error, :not_found} = Core.lookup_s3_credential("unknown-key")
    end
  end
end
