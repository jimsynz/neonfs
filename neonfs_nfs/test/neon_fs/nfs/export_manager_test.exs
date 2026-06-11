defmodule NeonFS.NFS.ExportManagerTest do
  use ExUnit.Case, async: false

  use Mimic

  alias NeonFS.Events.{Envelope, VolumeUpdated}
  alias NeonFS.NFS.{ExportManager, InodeTable}

  @volume_id "01234567-89ab-7cde-bf01-23456789abcd"

  setup do
    start_supervised!(InodeTable)

    # Use port 0 so the kernel picks an ephemeral port — avoids
    # collisions across concurrent test runs and against any
    # already-bound 2049.
    Application.put_env(:neonfs_nfs, :bind_address, "127.0.0.1")
    Application.put_env(:neonfs_nfs, :port, 0)

    on_exit(fn ->
      Application.delete_env(:neonfs_nfs, :bind_address)
      Application.delete_env(:neonfs_nfs, :port)
    end)

    {:ok, manager} = start_supervised({ExportManager, []})
    # Wait for the :start_server continue to complete
    :sys.get_state(manager)
    Mimic.allow(NeonFS.Client.Router, self(), manager)
    {:ok, manager: manager}
  end

  defp volume(name, opts \\ []) do
    %{
      id: Keyword.get(opts, :id, @volume_id),
      name: name,
      nfs_export: Keyword.get(opts, :nfs_export, true),
      updated_at: DateTime.utc_now()
    }
  end

  defp sync_exports(manager, volumes) do
    stub(NeonFS.Client.Router, :call, fn NeonFS.Core, :list_volumes, [] ->
      {:ok, volumes}
    end)

    send(manager, :resync)
    :sys.get_state(manager)
  end

  test "mirrors exported volumes from cluster state", %{manager: manager} do
    sync_exports(manager, [volume("photos"), volume("docs", nfs_export: false)])

    assert [export] = ExportManager.list_exports()
    assert export.volume_name == "photos"
    assert export.volume_id == @volume_id
    assert %DateTime{} = export.exported_at
  end

  test "get_export finds exported volumes only", %{manager: manager} do
    sync_exports(manager, [volume("photos"), volume("docs", nfs_export: false)])

    assert {:ok, export} = ExportManager.get_export("photos")
    assert export.volume_id == @volume_id
    assert {:error, :not_found} = ExportManager.get_export("docs")
  end

  test "resync drops exports removed from cluster state", %{manager: manager} do
    sync_exports(manager, [volume("photos"), volume("media")])
    assert length(ExportManager.list_exports()) == 2

    sync_exports(manager, [volume("media")])
    assert [%{volume_name: "media"}] = ExportManager.list_exports()
  end

  test "volume lifecycle events trigger a resync", %{manager: manager} do
    sync_exports(manager, [])
    assert ExportManager.list_exports() == []

    stub(NeonFS.Client.Router, :call, fn NeonFS.Core, :list_volumes, [] ->
      {:ok, [volume("photos")]}
    end)

    envelope = %Envelope{
      event: %VolumeUpdated{volume_id: @volume_id},
      source_node: node(),
      sequence: 1,
      hlc_timestamp: {0, 0, node()}
    }

    send(manager, {:neonfs_event, envelope})
    :sys.get_state(manager)

    assert [%{volume_name: "photos"}] = ExportManager.list_exports()
  end

  test "failed resync keeps the current mirror", %{manager: manager} do
    sync_exports(manager, [volume("photos")])

    stub(NeonFS.Client.Router, :call, fn NeonFS.Core, :list_volumes, [] ->
      {:error, :all_nodes_unreachable}
    end)

    send(manager, :resync)
    :sys.get_state(manager)

    assert [%{volume_name: "photos"}] = ExportManager.list_exports()
  end

  # The native-BEAM stack starts an `NFSServer.RPC.Server` listener
  # under `ExportManager`. (NIF cutover landed in #657 of #286.)
  test "starts an NFSServer.RPC.Server listener" do
    state = :sys.get_state(ExportManager)
    assert is_pid(state.nfs_server)
    assert Process.alive?(state.nfs_server)
  end
end
