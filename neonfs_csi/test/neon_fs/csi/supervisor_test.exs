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

  # The defect this closes: the child spec passed `ip:` at the top level,
  # which `GRPC.Server.Supervisor` raises on, and omitted `start_server:`,
  # which defaults to false and loads the supervisor with no listener at all.
  # Either way the socket never appeared, so every CSI sidecar sat in "Still
  # connecting" until it timed out. Asserting the socket exists is the only
  # form of this test that would have failed.
  test "opens the unix socket the CO's sidecars dial" do
    socket_path =
      Path.join(
        System.tmp_dir!(),
        "neonfs_csi_listener_#{System.unique_integer([:positive])}/csi.sock"
      )

    Application.put_env(:neonfs_csi, :socket_path, socket_path)
    on_exit(fn -> File.rm_rf(Path.dirname(socket_path)) end)

    start_supervised!({NeonFS.CSI.Supervisor, []})

    assert [_listener] = Supervisor.which_children(NeonFS.CSI.Supervisor)

    assert File.exists?(socket_path),
           "the gRPC listener did not create #{socket_path}"
  end

  describe "service registration metadata" do
    setup do
      on_exit(fn -> Application.delete_env(:neonfs_csi, :mode) end)
      :ok
    end

    # The controller resolves a CSI node_id to a BEAM node through this
    # metadata; without it there is no mapping in the cluster at all.
    test "a node-mode plugin advertises the node_id it reports to the CO" do
      Application.put_env(:neonfs_csi, :mode, :node)
      Application.put_env(:neonfs_csi, :node_id, "worker-7")
      on_exit(fn -> Application.delete_env(:neonfs_csi, :node_id) end)

      metadata = NeonFS.CSI.Supervisor.registration_metadata()

      assert metadata.mode == :node
      assert metadata.node_id == "worker-7"
    end

    test "a controller-mode plugin advertises no node_id, having none" do
      Application.put_env(:neonfs_csi, :mode, :controller)

      metadata = NeonFS.CSI.Supervisor.registration_metadata()

      assert metadata.mode == :controller
      refute Map.has_key?(metadata, :node_id)
    end
  end
end
