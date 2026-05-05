defmodule NeonFS.Containerd.SupervisorTest do
  @moduledoc """
  Regression test for `NeonFS.Containerd.Supervisor` boot behaviour
  when the configured socket directory can't be prepared. The
  omnibus daemon used to crash hard if `/run/containerd/proxy-plugins`
  wasn't writable; the supervisor now logs a warning and starts with
  no children instead.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  setup do
    Application.put_env(:neonfs_containerd, :listener, :socket)
    Application.put_env(:neonfs_containerd, :register_service, false)

    on_exit(fn ->
      Application.delete_env(:neonfs_containerd, :listener)
      Application.delete_env(:neonfs_containerd, :socket_path)
      Application.delete_env(:neonfs_containerd, :register_service)
    end)

    :ok
  end

  test "starts with no listener when the socket directory can't be prepared" do
    # Pin the socket path's parent at a regular file so `mkdir_p` returns
    # `:enotdir`. This stands in for the real-world cases (containerd
    # not installed → ENOENT, ProtectSystem=strict → EROFS) that
    # triggered the omnibus boot crash.
    blocking_file =
      Path.join(
        System.tmp_dir!(),
        "neonfs_containerd_supervisor_blocker_#{System.unique_integer([:positive])}"
      )

    File.write!(blocking_file, "")
    socket_path = Path.join([blocking_file, "containerd.sock"])
    Application.put_env(:neonfs_containerd, :socket_path, socket_path)
    on_exit(fn -> File.rm(blocking_file) end)

    log =
      capture_log(fn ->
        start_supervised!({NeonFS.Containerd.Supervisor, []})
      end)

    assert log =~ "containerd content store plugin disabled"
    assert log =~ "enotdir"
    refute File.exists?(socket_path)

    children = Supervisor.which_children(NeonFS.Containerd.Supervisor)
    assert children == []
  end
end
