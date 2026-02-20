defmodule NeonFS.Core.DriveRegistryTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.{Drive, DriveRegistry}

  setup do
    # Ensure a DriveRegistry is running with our test drives.
    # The app supervisor may have already started one, so handle both cases.
    ensure_registry_with_drives([
      %{id: "nvme0", path: "/tmp/test_nvme0", tier: :hot, capacity: 1_000_000_000},
      %{id: "sata0", path: "/tmp/test_sata0", tier: :cold, capacity: 4_000_000_000}
    ])

    :ok
  end

  defp ensure_registry_with_drives(drives_config) do
    case GenServer.whereis(DriveRegistry) do
      nil ->
        # No registry running — start one
        {:ok, _pid} = DriveRegistry.start_link(drives: drives_config, sync_interval_ms: 0)

      _pid ->
        # Registry already running (from app supervisor or previous test).
        # Clear ETS and re-register our test drives.
        :ets.delete_all_objects(:drive_registry)

        drives_config
        |> Enum.map(&Drive.from_config(&1, Node.self()))
        |> Enum.each(&DriveRegistry.register_drive/1)
    end
  end

  describe "startup" do
    test "registers local drives from config on startup" do
      drives = DriveRegistry.list_drives()
      assert length(drives) == 2

      ids = Enum.map(drives, & &1.id)
      assert "nvme0" in ids
      assert "sata0" in ids
    end

    test "drives have correct fields from config" do
      drives = DriveRegistry.list_drives()
      nvme = Enum.find(drives, &(&1.id == "nvme0"))

      assert nvme.node == Node.self()
      assert nvme.path == "/tmp/test_nvme0"
      assert nvme.tier == :hot
      assert nvme.capacity_bytes == 1_000_000_000
      assert nvme.used_bytes == 0
      assert nvme.state == :active
    end
  end

  describe "register_drive/1" do
    test "registers a new drive" do
      drive = %Drive{
        id: "sata1",
        node: Node.self(),
        path: "/tmp/test_sata1",
        tier: :warm,
        capacity_bytes: 2_000_000_000
      }

      assert :ok = DriveRegistry.register_drive(drive)

      drives = DriveRegistry.list_drives()
      assert length(drives) == 3
      assert Enum.any?(drives, &(&1.id == "sata1"))
    end

    test "overwrites existing drive with same node+id" do
      drive = %Drive{
        id: "nvme0",
        node: Node.self(),
        path: "/tmp/test_nvme0_updated",
        tier: :hot,
        capacity_bytes: 2_000_000_000
      }

      assert :ok = DriveRegistry.register_drive(drive)

      drives = DriveRegistry.list_drives()
      nvme = Enum.find(drives, &(&1.id == "nvme0"))
      assert nvme.capacity_bytes == 2_000_000_000
    end

    test "emits telemetry on registration" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :drive_registry, :register]
        ])

      drive = %Drive{
        id: "sata2",
        node: Node.self(),
        path: "/tmp/test_sata2",
        tier: :warm,
        capacity_bytes: 500_000_000
      }

      DriveRegistry.register_drive(drive)

      assert_receive {[:neonfs, :drive_registry, :register], ^ref, %{capacity_bytes: 500_000_000},
                      %{drive_id: "sata2", node: _, tier: :warm}}
    end
  end

  describe "drives_for_tier/1" do
    test "returns drives in the specified tier" do
      hot_drives = DriveRegistry.drives_for_tier(:hot)
      assert length(hot_drives) == 1
      assert hd(hot_drives).id == "nvme0"
    end

    test "returns empty list for tier with no drives" do
      assert DriveRegistry.drives_for_tier(:warm) == []
    end

    test "returns all drives in a tier" do
      drive = %Drive{
        id: "nvme1",
        node: Node.self(),
        path: "/tmp/test_nvme1",
        tier: :hot,
        capacity_bytes: 1_000_000_000
      }

      DriveRegistry.register_drive(drive)

      hot_drives = DriveRegistry.drives_for_tier(:hot)
      assert length(hot_drives) == 2
    end
  end

  describe "drives_for_node/1" do
    test "returns drives on the specified node" do
      drives = DriveRegistry.drives_for_node(Node.self())
      assert length(drives) == 2
    end

    test "returns empty for unknown node" do
      assert DriveRegistry.drives_for_node(:unknown@host) == []
    end
  end

  describe "select_drive/1" do
    test "selects active drive in tier" do
      assert {:ok, drive} = DriveRegistry.select_drive(:hot)
      assert drive.id == "nvme0"
      assert drive.tier == :hot
    end

    test "returns error for tier with no drives" do
      assert {:error, :no_drives_in_tier} = DriveRegistry.select_drive(:warm)
    end

    test "selects least-used drive" do
      # Add a second hot drive with lower usage ratio
      drive = %Drive{
        id: "nvme1",
        node: Node.self(),
        path: "/tmp/test_nvme1",
        tier: :hot,
        capacity_bytes: 1_000_000_000,
        used_bytes: 0
      }

      DriveRegistry.register_drive(drive)

      # Make nvme0 partially used
      DriveRegistry.update_usage("nvme0", 500_000_000)

      assert {:ok, selected} = DriveRegistry.select_drive(:hot)
      assert selected.id == "nvme1"
    end

    test "falls back to standby drives when no active drives" do
      DriveRegistry.update_state("nvme0", :standby)

      assert {:ok, selected} = DriveRegistry.select_drive(:hot)
      assert selected.id == "nvme0"
      assert selected.state == :standby
    end

    test "emits telemetry on selection" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :drive_registry, :select_drive]
        ])

      DriveRegistry.select_drive(:hot)

      assert_receive {[:neonfs, :drive_registry, :select_drive], ^ref, %{candidates: 1},
                      %{tier: :hot, node: _}}
    end
  end

  describe "update_usage/2" do
    test "updates used_bytes for a drive" do
      assert :ok = DriveRegistry.update_usage("nvme0", 100_000)

      drives = DriveRegistry.list_drives()
      nvme = Enum.find(drives, &(&1.id == "nvme0"))
      assert nvme.used_bytes == 100_000
    end

    test "returns error for unknown drive" do
      assert {:error, :not_found} = DriveRegistry.update_usage("nonexistent", 100)
    end
  end

  describe "update_state/2" do
    test "updates drive state to standby" do
      assert :ok = DriveRegistry.update_state("nvme0", :standby)

      drives = DriveRegistry.list_drives()
      nvme = Enum.find(drives, &(&1.id == "nvme0"))
      assert nvme.state == :standby
    end

    test "updates drive state to active" do
      DriveRegistry.update_state("nvme0", :standby)
      assert :ok = DriveRegistry.update_state("nvme0", :active)

      drives = DriveRegistry.list_drives()
      nvme = Enum.find(drives, &(&1.id == "nvme0"))
      assert nvme.state == :active
    end

    test "returns error for unknown drive" do
      assert {:error, :not_found} = DriveRegistry.update_state("nonexistent", :standby)
    end
  end
end
