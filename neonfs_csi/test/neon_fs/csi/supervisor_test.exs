defmodule NeonFS.CSI.SupervisorTest do
  @moduledoc """
  Regression test for `NeonFS.CSI.Supervisor` boot behaviour when
  the configured socket directory can't be prepared. The kubelet
  hostPath the canonical CSI deployment binds (`/var/lib/csi/...` /
  `/var/lib/kubelet/plugins/...`) won't exist on a host that isn't
  running k8s, and the supervisor used to crash hard. It now logs
  a warning and starts with no children instead.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  setup do
    Application.put_env(:neonfs_csi, :listener, :socket)
    Application.put_env(:neonfs_csi, :register_service, false)

    on_exit(fn ->
      Application.delete_env(:neonfs_csi, :listener)
      Application.delete_env(:neonfs_csi, :socket_path)
      Application.delete_env(:neonfs_csi, :register_service)
    end)

    :ok
  end

  test "starts with no listener when the socket directory can't be prepared" do
    blocking_file =
      Path.join(
        System.tmp_dir!(),
        "neonfs_csi_supervisor_blocker_#{System.unique_integer([:positive])}"
      )

    File.write!(blocking_file, "")
    socket_path = Path.join([blocking_file, "csi.sock"])
    Application.put_env(:neonfs_csi, :socket_path, socket_path)
    on_exit(fn -> File.rm(blocking_file) end)

    log =
      capture_log(fn ->
        start_supervised!({NeonFS.CSI.Supervisor, []})
      end)

    assert log =~ "CSI plugin disabled"
    assert log =~ "enotdir"
    refute File.exists?(socket_path)

    children = Supervisor.which_children(NeonFS.CSI.Supervisor)
    assert children == []
  end
end
