defmodule NeonFS.NFS.ExportManagerTest do
  use ExUnit.Case, async: false

  alias NeonFS.NFS.{ExportManager, ExportSupervisor, InodeTable}

  setup do
    start_supervised!(InodeTable)
    start_supervised!(ExportSupervisor)
    # Start ExportManager without auto-starting the NFS server
    # (NIF may not be available or port may be in use)
    {:ok, manager} = start_supervised({ExportManager, []})
    # Wait for the :start_server continue to complete
    :sys.get_state(manager)
    {:ok, manager: manager}
  end

  test "exports a volume" do
    assert {:ok, export_id} = ExportManager.export("photos")
    assert is_binary(export_id)
    assert String.starts_with?(export_id, "export_")
  end

  test "export is idempotent for same volume" do
    {:ok, id1} = ExportManager.export("data")
    {:ok, id2} = ExportManager.export("data")
    assert id1 == id2
  end

  test "lists exports" do
    ExportManager.export("vol1")
    ExportManager.export("vol2")

    exports = ExportManager.list_exports()
    names = Enum.map(exports, & &1.volume_name) |> Enum.sort()
    assert names == ["vol1", "vol2"]
  end

  test "unexports a volume" do
    {:ok, export_id} = ExportManager.export("temp")
    assert :ok = ExportManager.unexport(export_id)
    assert ExportManager.list_exports() == []
  end

  test "unexport returns error for unknown id" do
    assert {:error, :not_found} = ExportManager.unexport("nonexistent")
  end

  test "export info has correct fields" do
    {:ok, _id} = ExportManager.export("test_vol")
    [export] = ExportManager.list_exports()

    assert export.volume_name == "test_vol"
    assert %DateTime{} = export.exported_at
  end

  # POSIX cutover ramp-up (sub-issue #655 of #286). The legacy NIF
  # path is exercised by every other test in this file via the
  # NIF-not-loaded fallback; this describe block boots the BEAM
  # stack instead and verifies a real listener is bound.
  describe ":beam handler_stack dispatch" do
    setup do
      previous_stack = Application.get_env(:neonfs_nfs, :handler_stack)
      previous_port = Application.get_env(:neonfs_nfs, :port)
      previous_bind = Application.get_env(:neonfs_nfs, :bind_address)

      Application.put_env(:neonfs_nfs, :handler_stack, :beam)
      Application.put_env(:neonfs_nfs, :bind_address, "127.0.0.1")
      # Port 0 → kernel picks ephemeral; avoids collisions across
      # concurrent test runs.
      Application.put_env(:neonfs_nfs, :port, 0)

      on_exit(fn ->
        restore_env(:handler_stack, previous_stack)
        restore_env(:bind_address, previous_bind)
        restore_env(:port, previous_port)
      end)

      :ok
    end

    test "starts an NFSServer.RPC.Server listener instead of the NIF" do
      # The default `setup` stops `ExportManager` between cases via
      # `start_supervised`'s on_exit; we restart it here under the
      # `:beam` stack settings stamped by this describe's setup.
      stop_supervised(ExportManager)
      {:ok, manager} = start_supervised({ExportManager, []})
      :sys.get_state(manager)

      state = :sys.get_state(manager)
      # `:beam` path stores the listener pid in `nfs_server` and
      # leaves `handler_pid` nil (no per-handler GenServer; the
      # `NFSServer.RPC.Server` accept loop spawns one process per
      # connection).
      assert is_pid(state.nfs_server)
      assert Process.alive?(state.nfs_server)
      assert state.handler_pid == nil
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:neonfs_nfs, key)
  defp restore_env(key, value), do: Application.put_env(:neonfs_nfs, key, value)
end
