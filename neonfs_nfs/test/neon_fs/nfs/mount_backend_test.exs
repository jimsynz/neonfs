defmodule NeonFS.NFS.MountBackendTest do
  use ExUnit.Case, async: false

  use Mimic

  alias NeonFS.NFS.{ExportManager, Filehandle, InodeTable, MountBackend}
  alias NFSServer.Mount.Types.ExportNode

  setup :verify_on_exit!

  setup do
    start_supervised!(InodeTable)

    Application.put_env(:neonfs_nfs, :bind_address, "127.0.0.1")
    Application.put_env(:neonfs_nfs, :port, 0)

    on_exit(fn ->
      Application.delete_env(:neonfs_nfs, :bind_address)
      Application.delete_env(:neonfs_nfs, :port)
    end)

    {:ok, manager} = start_supervised({ExportManager, []})
    :sys.get_state(manager)
    Mimic.allow(NeonFS.Client.Router, self(), manager)
    :ok
  end

  describe "resolve/2" do
    test "synthetic root resolves to a fhandle with a null volume id and fileid 1" do
      assert {:ok, fhandle, [_ | _] = auth_flavors} = MountBackend.resolve("/", %{})
      assert {:ok, %{volume_id: <<0::128>>, fileid: 1}} = Filehandle.decode(fhandle)
      assert 1 in auth_flavors
    end

    test "volume root resolves through ExportManager + Core.get_volume" do
      {:ok, _id} = ExportManager.export("photos")
      vol_id = "01234567-89ab-7cde-bf01-23456789abcd"

      stub(NeonFS.Client.Router, :call, fn NeonFS.Core, :get_volume, ["photos"] ->
        {:ok, %{id: vol_id}}
      end)

      assert {:ok, fhandle, _flavors} = MountBackend.resolve("/photos", %{})
      assert {:ok, %{volume_id: vol_id_bin, fileid: 1}} = Filehandle.decode(fhandle)
      assert {:ok, ^vol_id_bin} = Filehandle.volume_uuid_to_binary(vol_id)
    end

    test "volume root resolution populates InodeTable.lookup_volume_name (issue #761)" do
      {:ok, _id} = ExportManager.export("photos")
      vol_id = "01234567-89ab-7cde-bf01-23456789abcd"

      stub(NeonFS.Client.Router, :call, fn NeonFS.Core, :get_volume, ["photos"] ->
        {:ok, %{id: vol_id}}
      end)

      assert {:ok, fhandle, _flavors} = MountBackend.resolve("/photos", %{})
      assert {:ok, %{volume_id: vol_id_bin}} = Filehandle.decode(fhandle)
      # NFSv3Backend.resolve_handle/1 reads back through this index
      # to recover the volume name from the filehandle's `volume_id`,
      # which is the actual #761 fix.
      assert {:ok, "photos"} = InodeTable.lookup_volume_name(vol_id_bin)
    end

    test "unknown volume returns :noent" do
      assert {:error, :noent} = MountBackend.resolve("/unknown-volume", %{})
    end

    test "non-rooted path returns :inval" do
      assert {:error, :inval} = MountBackend.resolve("not-a-path", %{})
    end
  end

  describe "list_exports/1" do
    test "maps ExportManager exports to ExportNode entries" do
      {:ok, _} = ExportManager.export("vol-a")
      {:ok, _} = ExportManager.export("vol-b")

      nodes = MountBackend.list_exports(%{})
      dirs = nodes |> Enum.map(& &1.dir) |> Enum.sort()
      assert dirs == ["/vol-a", "/vol-b"]
      assert Enum.all?(nodes, fn %ExportNode{groups: groups} -> groups == [] end)
    end

    test "returns [] when no volumes are exported" do
      assert [] = MountBackend.list_exports(%{})
    end
  end

  describe "list_mounts/1" do
    test "returns [] (NeonFS doesn't track mount state)" do
      assert [] = MountBackend.list_mounts(%{})
    end
  end

  describe "bookkeeping callbacks" do
    test "record_mount / forget_mount / forget_all_mounts are no-ops" do
      assert :ok = MountBackend.record_mount("client.example", "/vol", %{})
      assert :ok = MountBackend.forget_mount("client.example", "/vol", %{})
      assert :ok = MountBackend.forget_all_mounts("client.example", %{})
    end
  end
end
