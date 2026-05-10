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

  describe "log_grpc_exception?/1 (#952)" do
    test "drops :not_found RPCError exceptions" do
      # `GRPC.RPCError.exception(:not_found)` resolves the atom to the
      # integer status code (5) via GRPC.Status — the struct stores
      # the int. Use the constructor so the test mirrors what the
      # gRPC adapter actually produces at runtime.
      report = %GRPC.Server.Adapters.ReportException{
        kind: :error,
        reason: GRPC.RPCError.exception(status: :not_found, message: "blob not found"),
        stack: [],
        adapter_extra: []
      }

      refute NeonFS.Containerd.Supervisor.log_grpc_exception?(report)
    end

    test "logs other RPCError statuses" do
      for status <- [:internal, :invalid_argument, :failed_precondition, :unavailable] do
        report = %GRPC.Server.Adapters.ReportException{
          kind: :error,
          reason: GRPC.RPCError.exception(status: status, message: "..."),
          stack: [],
          adapter_extra: []
        }

        assert NeonFS.Containerd.Supervisor.log_grpc_exception?(report),
               "expected #{status} to be logged"
      end
    end

    test "logs non-RPCError exceptions" do
      report = %GRPC.Server.Adapters.ReportException{
        kind: :error,
        reason: %RuntimeError{message: "something else exploded"},
        stack: [],
        adapter_extra: []
      }

      assert NeonFS.Containerd.Supervisor.log_grpc_exception?(report)
    end
  end
end
