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
end
