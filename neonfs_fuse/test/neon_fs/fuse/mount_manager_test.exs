defmodule NeonFS.FUSE.MountManagerTest do
  use ExUnit.Case, async: true

  alias NeonFS.FUSE.MountManager

  @moduletag :tmp_dir

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
end
