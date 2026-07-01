defmodule NeonFS.Core.DriveManagerTest do
  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Cluster.State

  alias NeonFS.Core.{
    BlobStore,
    DriveManager,
    DriveRegistry,
    DriveTrust,
    NodeRegistry,
    RaSupervisor
  }

  alias NeonFS.Core.Drive.Identity

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

  describe "register_local_drives_in_bootstrap/0 retry (#1102)" do
    setup do
      Application.put_env(:neonfs_core, :bootstrap_register_backoff_ms, 0)
      on_exit(fn -> Application.delete_env(:neonfs_core, :bootstrap_register_backoff_ms) end)
      :ok
    end

    test "retries a transient Ra failure until the registration succeeds" do
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stub(RaSupervisor, :command, fn {:register_drive, _entry} ->
        attempt = Agent.get_and_update(counter, &{&1 + 1, &1 + 1})
        if attempt < 3, do: {:error, :noproc}, else: {:ok, :ok, :leader@node}
      end)

      assert :ok = DriveManager.register_local_drives_in_bootstrap()
      assert Agent.get(counter, & &1) == 3
    end

    test "gives up after the attempt budget and emits telemetry" do
      stub(RaSupervisor, :command, fn {:register_drive, _entry} -> {:error, :noproc} end)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :drive_manager, :bootstrap_register_failed]
        ])

      assert :ok = DriveManager.register_local_drives_in_bootstrap()

      assert_receive {[:neonfs, :drive_manager, :bootstrap_register_failed], ^ref, %{attempts: 5},
                      %{drive_id: "default"}},
                     1_000
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

    test "writes \`.neonfs-drive.json\` identity file (#778)", %{tmp_dir: tmp_dir} do
      new_path = Path.join(tmp_dir, "new_with_identity")
      File.mkdir_p!(new_path)

      assert {:ok, _drive} =
               DriveManager.add_drive(%{id: "new_with_id", path: new_path, tier: "hot"})

      identity_file = Path.join(new_path, ".neonfs-drive.json")
      assert File.exists?(identity_file)

      {:ok, identity} = Identity.read(new_path)
      assert identity.drive_id == "new_with_id"
      assert identity.cluster_id == "clust_test"
      assert identity.schema_version == 1
      assert identity.on_disk_format_version == 1
    end

    test "refuses to add a drive whose identity names a foreign cluster (#778)",
         %{tmp_dir: tmp_dir} do
      foreign_path = Path.join(tmp_dir, "foreign_drive")
      File.mkdir_p!(foreign_path)

      # Plant an identity file claiming the directory belongs to another cluster.
      :ok =
        Identity.write(
          foreign_path,
          Identity.new("foreign_disk", "clust_other")
        )

      assert {:error, message} =
               DriveManager.add_drive(%{id: "any_id", path: foreign_path, tier: "hot"})

      assert message =~ "belongs to a different cluster"
      assert message =~ "clust_other"
      assert message =~ "clust_test"
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

  describe "auto-uncordon (#1420)" do
    test "auto_uncordon?/2: a :maintenance node with no unverified drive resumes" do
      assert DriveManager.auto_uncordon?(:maintenance, false)
      refute DriveManager.auto_uncordon?(:maintenance, true)
      refute DriveManager.auto_uncordon?(:active, false)
      refute DriveManager.auto_uncordon?(:draining, false)
      refute DriveManager.auto_uncordon?(nil, false)
    end

    test "a cordoned node whose drives are all trusted auto-uncordons on a trust-clear" do
      dm = Process.whereis(DriveManager)
      Mimic.allow(NodeRegistry, self(), dm)
      Mimic.allow(DriveTrust, self(), dm)
      test = self()

      stub(NodeRegistry, :status, fn _node -> :maintenance end)
      stub(DriveTrust, :unverified, fn -> [] end)

      stub(NodeRegistry, :set_status, fn node, status ->
        send(test, {:set_status, node, status}) && :ok
      end)

      GenServer.cast(dm, :maybe_auto_uncordon)
      :sys.get_state(dm)

      assert_received {:set_status, _node, :active}
    end

    test "a cordoned node with an unverified drive does not auto-uncordon" do
      dm = Process.whereis(DriveManager)
      Mimic.allow(NodeRegistry, self(), dm)
      Mimic.allow(DriveTrust, self(), dm)
      test = self()

      stub(NodeRegistry, :status, fn _node -> :maintenance end)
      stub(DriveTrust, :unverified, fn -> [{Node.self(), "default"}] end)

      stub(NodeRegistry, :set_status, fn node, status ->
        send(test, {:set_status, node, status}) && :ok
      end)

      GenServer.cast(dm, :maybe_auto_uncordon)
      :sys.get_state(dm)

      refute_received {:set_status, _node, :active}
    end
  end

  describe "recover_drive/3 (#1426)" do
    test "a :dirty drive is marked :unverified and a drive-scoped scrub is queued" do
      test = self()
      mark_fn = fn node, drive_id -> send(test, {:marked, node, drive_id}) && :ok end
      scrub_fn = fn drive_id -> send(test, {:scrubbed, drive_id}) end

      assert :ok =
               DriveManager.recover_drive("d-dirty", :dirty, mark_fn: mark_fn, scrub_fn: scrub_fn)

      assert_received {:marked, _node, "d-dirty"}
      assert_received {:scrubbed, "d-dirty"}
    end

    test ":clean, :fresh and unknown drives are left untouched" do
      test = self()
      mark_fn = fn _node, _drive_id -> send(test, :marked) && :ok end
      scrub_fn = fn _drive_id -> send(test, :scrubbed) end

      for open_state <- [:clean, :fresh, nil] do
        assert :ok =
                 DriveManager.recover_drive("d", open_state, mark_fn: mark_fn, scrub_fn: scrub_fn)
      end

      refute_received :marked
      refute_received :scrubbed
    end

    test "a failed mark skips the scrub (left for retry)" do
      test = self()
      mark_fn = fn _node, _drive_id -> {:error, :no_leader} end
      scrub_fn = fn _drive_id -> send(test, :scrubbed) end

      assert :ok = DriveManager.recover_drive("d", :dirty, mark_fn: mark_fn, scrub_fn: scrub_fn)
      refute_received :scrubbed
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
