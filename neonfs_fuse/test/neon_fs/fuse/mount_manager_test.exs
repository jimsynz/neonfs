defmodule NeonFS.FUSE.MountManagerTest do
  use ExUnit.Case, async: false

  alias NeonFS.FUSE.{MountInfo, MountManager}

  @moduletag :tmp_dir

  describe "build_mount_options/1 (issue #1574)" do
    test "defaults to auto_unmount only" do
      assert MountManager.build_mount_options([]) == ["auto_unmount"]
    end

    test "adds allow_other when requested" do
      assert "allow_other" in MountManager.build_mount_options(allow_other: true)
    end

    test "adds allow_root when requested" do
      assert "allow_root" in MountManager.build_mount_options(allow_root: true)
    end

    test "omits allow_other and allow_root by default" do
      opts = MountManager.build_mount_options([])
      refute "allow_other" in opts
      refute "allow_root" in opts
    end
  end

  describe "diagnose_fusermount_no_fd/2 (issue #756)" do
    test "reports a missing mount point", %{tmp_dir: tmp_dir} do
      missing = Path.join(tmp_dir, "does-not-exist")

      assert {:fusermount_no_fd, message} =
               MountManager.diagnose_fusermount_no_fd(missing, 1000)

      assert message =~ "does not exist"
      assert message =~ missing
    end

    test "reports a mount point that is a regular file", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "regular_file")
      File.write!(file_path, "")

      assert {:fusermount_no_fd, message} =
               MountManager.diagnose_fusermount_no_fd(file_path, 1000)

      assert message =~ "is not a directory"
      assert message =~ file_path
    end

    test "reports an ownership mismatch when daemon uid differs", %{tmp_dir: tmp_dir} do
      dir = Path.join(tmp_dir, "wrong_owner")
      File.mkdir_p!(dir)

      stat_uid = File.stat!(dir).uid
      # Pick a daemon_uid that is non-zero, non-root and not equal to the
      # owner uid — exactly the case the diagnostic targets.
      daemon_uid = stat_uid + 1

      assert {:fusermount_no_fd, message} =
               MountManager.diagnose_fusermount_no_fd(dir, daemon_uid)

      assert message =~ "Mount point must be owned by the daemon user"
      assert message =~ "uid=#{stat_uid}"
      assert message =~ "daemon uid=#{daemon_uid}"
      assert message =~ "chown neonfs:neonfs #{dir}"
    end

    test "falls back to the generic 'check daemon logs' message when ownership matches",
         %{tmp_dir: tmp_dir} do
      dir = Path.join(tmp_dir, "owned")
      File.mkdir_p!(dir)
      stat_uid = File.stat!(dir).uid

      assert {:fusermount_no_fd, message} =
               MountManager.diagnose_fusermount_no_fd(dir, stat_uid)

      assert message =~ "fusermount3 rejected the mount"
      assert message =~ dir
      assert message =~ "journalctl"
    end

    test "falls back to the generic message when daemon is root", %{tmp_dir: tmp_dir} do
      dir = Path.join(tmp_dir, "root_daemon")
      File.mkdir_p!(dir)

      # `daemon_uid == 0` should suppress the ownership check (root can
      # mount anywhere) and surface the generic kernel-rejection message.
      assert {:fusermount_no_fd, message} =
               MountManager.diagnose_fusermount_no_fd(dir, 0)

      assert message =~ "fusermount3 rejected the mount"
    end

    test "falls back to the generic message when daemon uid is unknown",
         %{tmp_dir: tmp_dir} do
      dir = Path.join(tmp_dir, "unknown_daemon")
      File.mkdir_p!(dir)

      assert {:fusermount_no_fd, message} =
               MountManager.diagnose_fusermount_no_fd(dir, nil)

      assert message =~ "fusermount3 rejected the mount"
    end
  end

  describe "mount/3 mount-point validation names the FUSE node (#1358)" do
    setup do
      start_supervised!(MountManager)
      :ok
    end

    test "missing mount point reports the node it was checked on", %{tmp_dir: tmp_dir} do
      missing = Path.join(tmp_dir, "does-not-exist")

      assert {:error, message} = MountManager.mount("vol-a", missing)
      assert message =~ "mount point #{Path.expand(missing)} not found"
      assert message =~ "on FUSE node #{Node.self()}"
    end

    test "non-directory mount point reports the node it was checked on", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "regular_file")
      File.write!(file_path, "")

      assert {:error, message} = MountManager.mount("vol-a", file_path)
      assert message =~ "is not a directory"
      assert message =~ "on FUSE node #{Node.self()}"
    end
  end

  describe "get_mount_by_volume_name/1 (issue #1016)" do
    # `MountManager` is a `:name`-registered singleton; the suite that
    # actually spins up the full FUSE stack lives in
    # `neonfs_integration`. These tests prod the `handle_call/3`
    # clauses directly so the unit suite covers the new lookup
    # without needing a kernel mount.

    setup do
      manager = start_supervised!(MountManager)

      :sys.replace_state(manager, fn _state ->
        %MountManager.State{
          mounts: %{
            "mount_abc123" =>
              MountInfo.new(
                id: "mount_abc123",
                volume_name: "vol-a",
                mount_point: "/tmp/mnt-a",
                started_at: DateTime.utc_now(),
                mount_session: nil,
                handler_pid: nil,
                session_pid: nil,
                cache_pid: nil
              ),
            "mount_def456" =>
              MountInfo.new(
                id: "mount_def456",
                volume_name: "vol-b",
                mount_point: "/tmp/mnt-b",
                started_at: DateTime.utc_now(),
                mount_session: nil,
                handler_pid: nil,
                session_pid: nil,
                cache_pid: nil
              )
          },
          mount_points: %{"/tmp/mnt-a" => "mount_abc123", "/tmp/mnt-b" => "mount_def456"}
        }
      end)

      {:ok, manager: manager}
    end

    test "returns the matching mount for an exact volume name" do
      assert {:ok, %{volume_name: "vol-a", id: "mount_abc123"}} =
               MountManager.get_mount_by_volume_name("vol-a")

      assert {:ok, %{volume_name: "vol-b"}} = MountManager.get_mount_by_volume_name("vol-b")
    end

    test "returns :not_found when no mount has the volume name" do
      assert {:error, :not_found} = MountManager.get_mount_by_volume_name("nope")
    end
  end
end
