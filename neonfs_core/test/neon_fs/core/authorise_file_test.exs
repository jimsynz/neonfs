defmodule NeonFS.Core.AuthoriseFileTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{ACLManager, Authorise, FileIndex, FileMeta, VolumeACL}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    start_core_subsystems()
    start_acl_manager()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  defp setup_volume_acl(volume_id, owner_uid, owner_gid) do
    acl =
      VolumeACL.new(
        volume_id: volume_id,
        owner_uid: owner_uid,
        owner_gid: owner_gid
      )

    :ok = ACLManager.set_volume_acl(volume_id, acl)
  end

  defp create_file(volume_id, path, opts) do
    mode = Keyword.get(opts, :mode, 0o644)
    uid = Keyword.get(opts, :uid, 1000)
    gid = Keyword.get(opts, :gid, 1000)
    acl_entries = Keyword.get(opts, :acl_entries, [])
    default_acl = Keyword.get(opts, :default_acl)

    file_meta =
      FileMeta.new(volume_id, path,
        mode: mode,
        uid: uid,
        gid: gid,
        acl_entries: acl_entries,
        default_acl: default_acl
      )

    {:ok, _} = FileIndex.create(file_meta)
    file_meta
  end

  describe "check/4 with {:file, volume_id, path}" do
    test "root (UID 0) bypasses file-level checks" do
      create_file("vol-f1", "/secret.txt", mode: 0o000, uid: 1000, gid: 1000)

      assert :ok = Authorise.check(0, :read, {:file, "vol-f1", "/secret.txt"})
      assert :ok = Authorise.check(0, :write, {:file, "vol-f1", "/secret.txt"})
    end

    test "owner can read file with mode 0o644" do
      create_file("vol-f2", "/file.txt", mode: 0o644, uid: 1000, gid: 1000)

      assert :ok = Authorise.check(1000, :read, {:file, "vol-f2", "/file.txt"})
    end

    test "owner can write file with mode 0o644" do
      create_file("vol-f3", "/file.txt", mode: 0o644, uid: 1000, gid: 1000)

      assert :ok = Authorise.check(1000, :write, {:file, "vol-f3", "/file.txt"})
    end

    test "other user cannot write file with mode 0o644" do
      create_file("vol-f4", "/file.txt", mode: 0o644, uid: 1000, gid: 1000)

      assert {:error, :forbidden} = Authorise.check(2000, :write, {:file, "vol-f4", "/file.txt"})
    end

    test "other user can read file with mode 0o644" do
      create_file("vol-f5", "/file.txt", mode: 0o644, uid: 1000, gid: 1000)

      assert :ok = Authorise.check(2000, :read, {:file, "vol-f5", "/file.txt"})
    end

    test "mode 0o600 denies other users" do
      create_file("vol-f6", "/private.txt", mode: 0o600, uid: 1000, gid: 1000)

      assert {:error, :forbidden} =
               Authorise.check(2000, :read, {:file, "vol-f6", "/private.txt"})

      assert {:error, :forbidden} =
               Authorise.check(2000, :write, {:file, "vol-f6", "/private.txt"})
    end

    test "group member can read file with mode 0o640" do
      create_file("vol-f7", "/group.txt", mode: 0o640, uid: 1000, gid: 100)

      # UID 2000 with GID 100 should be able to read
      assert :ok = Authorise.check(2000, [100], :read, {:file, "vol-f7", "/group.txt"})
    end

    test "non-group member cannot read file with mode 0o640" do
      create_file("vol-f8", "/group.txt", mode: 0o640, uid: 1000, gid: 100)

      # UID 2000 with GID 200 should NOT be able to read
      assert {:error, :forbidden} =
               Authorise.check(2000, [200], :read, {:file, "vol-f8", "/group.txt"})
    end
  end

  describe "extended ACL entries" do
    test "named user ACL entry grants access beyond mode bits" do
      create_file("vol-ext1", "/shared.txt",
        mode: 0o600,
        uid: 1000,
        gid: 1000,
        acl_entries: [
          %{type: :user, id: 2000, permissions: MapSet.new([:r, :w])},
          %{type: :mask, id: nil, permissions: MapSet.new([:r, :w])}
        ]
      )

      assert :ok = Authorise.check(2000, :read, {:file, "vol-ext1", "/shared.txt"})
      assert :ok = Authorise.check(2000, :write, {:file, "vol-ext1", "/shared.txt"})
    end

    test "mask limits named user effective permissions" do
      create_file("vol-ext2", "/masked.txt",
        mode: 0o600,
        uid: 1000,
        gid: 1000,
        acl_entries: [
          %{type: :user, id: 2000, permissions: MapSet.new([:r, :w])},
          %{type: :mask, id: nil, permissions: MapSet.new([:r])}
        ]
      )

      # Mask limits to read-only
      assert :ok = Authorise.check(2000, :read, {:file, "vol-ext2", "/masked.txt"})

      assert {:error, :forbidden} =
               Authorise.check(2000, :write, {:file, "vol-ext2", "/masked.txt"})
    end

    test "mask does not limit owner permissions" do
      create_file("vol-ext3", "/owner-masked.txt",
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        acl_entries: [
          %{type: :mask, id: nil, permissions: MapSet.new([:r])}
        ]
      )

      # Owner permissions come from mode bits, not affected by mask
      assert :ok = Authorise.check(1000, :read, {:file, "vol-ext3", "/owner-masked.txt"})
      assert :ok = Authorise.check(1000, :write, {:file, "vol-ext3", "/owner-masked.txt"})
    end

    test "named group ACL entry grants access" do
      create_file("vol-ext4", "/group-ext.txt",
        mode: 0o600,
        uid: 1000,
        gid: 1000,
        acl_entries: [
          %{type: :group, id: 200, permissions: MapSet.new([:r])},
          %{type: :mask, id: nil, permissions: MapSet.new([:r, :w])}
        ]
      )

      assert :ok = Authorise.check(3000, [200], :read, {:file, "vol-ext4", "/group-ext.txt"})
    end
  end

  describe "directory ACL inheritance" do
    test "set_default_acl stores default ACL on directory" do
      create_file("vol-dir1", "/docs",
        mode: 0o40755,
        uid: 1000,
        gid: 1000
      )

      default_entries = [
        %{type: :user, id: 2000, permissions: MapSet.new([:r, :w])},
        %{type: :mask, id: nil, permissions: MapSet.new([:r, :w])}
      ]

      :ok = ACLManager.set_default_acl("vol-dir1", "/docs", default_entries)

      {:ok, acl_info} = ACLManager.get_file_acl("vol-dir1", "/docs")
      assert acl_info.default_acl == default_entries
    end

    test "set_file_acl stores extended ACL entries on file" do
      create_file("vol-facl1", "/test.txt",
        mode: 0o644,
        uid: 1000,
        gid: 1000
      )

      entries = [
        %{type: :user, id: 2000, permissions: MapSet.new([:r, :w])},
        %{type: :mask, id: nil, permissions: MapSet.new([:r, :w])}
      ]

      :ok = ACLManager.set_file_acl("vol-facl1", "/test.txt", entries)

      {:ok, acl_info} = ACLManager.get_file_acl("vol-facl1", "/test.txt")
      assert acl_info.acl_entries == entries
    end

    test "get_file_acl returns not_found for missing file" do
      assert {:error, :not_found} = ACLManager.get_file_acl("vol-nofile", "/missing.txt")
    end

    test "set_file_acl returns not_found for missing file" do
      assert {:error, :not_found} = ACLManager.set_file_acl("vol-nofile", "/missing.txt", [])
    end
  end

  describe "fallback to volume-level check" do
    test "falls back to volume ACL when file not found" do
      setup_volume_acl("vol-fallback", 1000, 1000)

      # No file exists at this path, should fall back to volume-level check
      # Owner UID 1000 should pass volume-level check
      assert :ok = Authorise.check(1000, :read, {:file, "vol-fallback", "/nonexistent.txt"})
    end

    test "falls back to volume ACL denial when file not found" do
      setup_volume_acl("vol-fallback2", 500, 500)

      # Non-owner, no ACL entry — should be denied at volume level
      assert {:error, :forbidden} =
               Authorise.check(1000, :read, {:file, "vol-fallback2", "/nonexistent.txt"})
    end
  end

  describe "defaults" do
    test "mode defaults to 0o644 for files and 0o755 for directories" do
      file = FileMeta.new("vol-defaults", "/file.txt")
      assert file.mode == 0o644
      assert file.acl_entries == []
      assert file.default_acl == nil
    end
  end
end
