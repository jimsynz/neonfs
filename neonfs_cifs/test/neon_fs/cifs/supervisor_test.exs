defmodule NeonFS.CIFS.SupervisorTest do
  @moduledoc """
  Regression test for `NeonFS.CIFS.Supervisor` boot behaviour when
  the configured socket directory can't be prepared. The supervisor
  used to crash hard via `File.mkdir_p!/1`; it now logs a warning
  and starts with no children instead.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  setup do
    Application.put_env(:neonfs_cifs, :listener, :socket)
    Application.put_env(:neonfs_cifs, :register_service, false)

    on_exit(fn ->
      Application.delete_env(:neonfs_cifs, :listener)
      Application.delete_env(:neonfs_cifs, :socket_path)
      Application.delete_env(:neonfs_cifs, :register_service)
    end)

    :ok
  end

  test "starts with no listener when the socket directory can't be prepared" do
    blocking_file =
      Path.join(
        System.tmp_dir!(),
        "neonfs_cifs_supervisor_blocker_#{System.unique_integer([:positive])}"
      )

    File.write!(blocking_file, "")
    socket_path = Path.join([blocking_file, "cifs.sock"])
    Application.put_env(:neonfs_cifs, :socket_path, socket_path)
    on_exit(fn -> File.rm(blocking_file) end)

    log =
      capture_log(fn ->
        start_supervised!({NeonFS.CIFS.Supervisor, []})
      end)

    assert log =~ "CIFS listener disabled"
    assert log =~ "enotdir"
    refute File.exists?(socket_path)

    children = Supervisor.which_children(NeonFS.CIFS.Supervisor)
    assert children == []
  end
end
