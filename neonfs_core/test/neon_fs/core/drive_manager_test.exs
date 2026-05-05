defmodule NeonFS.Core.DriveManagerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Cluster.State
  alias NeonFS.Core.{BlobStore, DriveManager, DriveRegistry}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    meta_dir = Path.join(tmp_dir, "meta")
    File.mkdir_p!(meta_dir)

    # Set up meta_dir for cluster.json persistence
    Application.put_env(:neonfs_core, :meta_dir, meta_dir)

    # Configure a single default drive
    default_drive_path = Path.join(tmp_dir, "default")
    File.mkdir_p!(default_drive_path)
    drives = [%{id: "default", path: default_drive_path, tier: :hot, capacity: 0}]
    Application.put_env(:neonfs_core, :drives, drives)

    # Start the required infrastructure
    start_supervised!({Registry, keys: :unique, name: NeonFS.Core.DriveStateRegistry})
    start_supervised!({BlobStore, drives: drives, prefix_depth: 2})
    start_supervised!({DriveRegistry, drives: drives, sync_interval_ms: 0})

    start_supervised!(
      {DynamicSupervisor, name: NeonFS.Core.DriveStateSupervisor, strategy: :one_for_one}
    )

    # Create a cluster.json so update_drives can persist
    node_info = %{
      id: "node_test123",
      name: Node.self(),
      joined_at: DateTime.utc_now()
    }

    state = %State{
      cluster_id: "clust_test",
      cluster_name: "test-cluster",
      created_at: DateTime.utc_now(),
      master_key: "test_key",
      this_node: node_info,
      drives: []
    }

    :ok = State.save(state)

    # Start DriveManager
    start_supervised!(DriveManager)

    on_exit(fn ->
      File.rm_rf!(tmp_dir)
      Application.delete_env(:neonfs_core, :meta_dir)
      Application.delete_env(:neonfs_core, :drives)
    end)

    %{tmp_dir: tmp_dir, meta_dir: meta_dir}
  end

  describe "list_drives/0" do
    test "lists the default drive" do
      drives = DriveManager.list_drives()
      assert length(drives) == 1
      assert hd(drives).id == "default"
    end
  end

  describe "list_all_drives/1" do
    test "returns all drives with no filter" do
      drives = DriveManager.list_all_drives()
      assert length(drives) == 1
      assert hd(drives).id == "default"
      assert hd(drives).node == Atom.to_string(Node.self())
    end

    test "filters by node" do
      drives = DriveManager.list_all_drives(node: Node.self())
      assert length(drives) == 1
      assert hd(drives).id == "default"
    end

    test "returns empty list for unknown node" do
      drives = DriveManager.list_all_drives(node: :unknown@nowhere)
      assert drives == []
    end

    test "includes drives added at runtime", %{tmp_dir: tmp_dir} do
      new_path = Path.join(tmp_dir, "extra_drive")
      File.mkdir_p!(new_path)
      {:ok, _} = DriveManager.add_drive(%{id: "extra", path: new_path, tier: "warm"})

      drives = DriveManager.list_all_drives()
      assert length(drives) == 2
      ids = Enum.map(drives, & &1.id)
      assert "default" in ids
      assert "extra" in ids
    end
  end

  describe "add_drive/1" do
    test "adds a new drive", %{tmp_dir: tmp_dir} do
      new_path = Path.join(tmp_dir, "new_drive")
      File.mkdir_p!(new_path)

      assert {:ok, drive} =
               DriveManager.add_drive(%{id: "new1", path: new_path, tier: "hot", capacity: "0"})

      assert drive.id == "new1"
      assert drive.path == new_path
      assert drive.tier == "hot"

      # Verify it appears in list
      drives = DriveManager.list_drives()
      assert length(drives) == 2
      assert Enum.any?(drives, &(&1.id == "new1"))
    end

    test "rejects missing path" do
      assert {:error, "Path is required"} = DriveManager.add_drive(%{path: "", tier: "hot"})
    end

    test "rejects non-existent path" do
      assert {:error, "Path does not exist: /nonexistent/path"} =
               DriveManager.add_drive(%{path: "/nonexistent/path", tier: "hot"})
    end

    test "rejects path that is not writable by the daemon", %{tmp_dir: tmp_dir} do
      readonly_path = Path.join(tmp_dir, "readonly_drive")
      File.mkdir_p!(readonly_path)
      File.chmod!(readonly_path, 0o500)

      on_exit(fn ->
        File.chmod(readonly_path, 0o700)
        File.rm_rf(readonly_path)
      end)

      if write_probe_works?(readonly_path) do
        # Running as root or on a filesystem that ignores chmod — chmod cannot
        # produce an unwritable directory here, so the probe behaviour cannot
        # be exercised; treat as inconclusive rather than a false pass.
        :ok
      else
        assert {:error, message} =
                 DriveManager.add_drive(%{path: readonly_path, tier: "hot"})

        assert message =~ "is not writable by the daemon"
        assert message =~ readonly_path
        assert message =~ "chown neonfs:neonfs"

        refute Enum.any?(DriveManager.list_drives(), &(&1.path == readonly_path))

        leftover =
          File.ls!(readonly_path) |> Enum.filter(&String.starts_with?(&1, ".neonfs-probe-"))

        assert leftover == []
      end
    end

    test "rejects path that is not a directory but a file", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "regular_file")
      File.write!(file_path, "not a dir")

      assert {:error, message} = DriveManager.add_drive(%{path: file_path, tier: "hot"})
      assert message =~ "Path exists but is not a directory"
    end

    test "rejects invalid tier", %{tmp_dir: tmp_dir} do
      new_path = Path.join(tmp_dir, "invalid_tier")
      File.mkdir_p!(new_path)

      assert {:error, "Invalid tier. Must be hot, warm, or cold"} =
               DriveManager.add_drive(%{path: new_path, tier: "invalid"})
    end

    test "rejects duplicate drive ID", %{tmp_dir: tmp_dir} do
      new_path = Path.join(tmp_dir, "dup_drive")
      File.mkdir_p!(new_path)

      assert {:error, {:duplicate_drive_id, "default"}} =
               DriveManager.add_drive(%{id: "default", path: new_path, tier: "hot"})
    end

    test "persists to cluster.json", %{tmp_dir: tmp_dir} do
      new_path = Path.join(tmp_dir, "persisted")
      File.mkdir_p!(new_path)

      {:ok, _drive} =
        DriveManager.add_drive(%{id: "persisted", path: new_path, tier: "cold", capacity: "1T"})

      {:ok, loaded} = State.load()
      assert [_ | _] = loaded.drives
      assert Enum.any?(loaded.drives, fn d -> d["id"] == "persisted" end)
    end

    test "auto-generates ID from path", %{tmp_dir: tmp_dir} do
      new_path = Path.join(tmp_dir, "auto_id")
      File.mkdir_p!(new_path)

      assert {:ok, drive} = DriveManager.add_drive(%{path: new_path, tier: "warm"})
      assert drive.id == "auto_id"
    end
  end

  describe "remove_drive/2" do
    test "removes an empty drive", %{tmp_dir: tmp_dir} do
      new_path = Path.join(tmp_dir, "to_remove")
      File.mkdir_p!(new_path)

      {:ok, _drive} = DriveManager.add_drive(%{id: "removable", path: new_path, tier: "hot"})
      assert :ok = DriveManager.remove_drive("removable")

      drives = DriveManager.list_drives()
      refute Enum.any?(drives, &(&1.id == "removable"))
    end

    test "refuses to remove drive with data unless forced", %{tmp_dir: tmp_dir} do
      new_path = Path.join(tmp_dir, "has_data")
      File.mkdir_p!(new_path)

      {:ok, _drive} = DriveManager.add_drive(%{id: "has_data", path: new_path, tier: "hot"})

      # Write some data to the drive
      data = "test chunk data"
      {:ok, _hash, _info} = BlobStore.write_chunk(data, "has_data", "hot")

      # Should refuse without force
      assert {:error, :drive_has_data} = DriveManager.remove_drive("has_data")

      # Should succeed with force
      assert :ok = DriveManager.remove_drive("has_data", force: true)
    end

    test "returns error for unknown drive" do
      assert {:error, {:unknown_drive, "nonexistent"}} =
               DriveManager.remove_drive("nonexistent")
    end

    test "persists removal to cluster.json", %{tmp_dir: tmp_dir} do
      new_path = Path.join(tmp_dir, "persist_remove")
      File.mkdir_p!(new_path)

      {:ok, _} = DriveManager.add_drive(%{id: "to_persist_remove", path: new_path, tier: "hot"})

      {:ok, state_before} = State.load()
      assert Enum.any?(state_before.drives, fn d -> d["id"] == "to_persist_remove" end)

      :ok = DriveManager.remove_drive("to_persist_remove")

      {:ok, state_after} = State.load()
      refute Enum.any?(state_after.drives, fn d -> d["id"] == "to_persist_remove" end)
    end
  end

  defp write_probe_works?(path) do
    probe = Path.join(path, ".write-probe-#{System.unique_integer([:positive])}")

    case File.write(probe, "x") do
      :ok ->
        _ = File.rm(probe)
        true

      {:error, _} ->
        false
    end
  end
end
