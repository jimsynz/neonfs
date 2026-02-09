defmodule NeonFS.Core.TopologyTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.{Drive, DriveRegistry, Topology}

  setup do
    # Ensure a DriveRegistry is running with our test drives.
    ensure_registry_with_drives([
      %{id: "nvme0", path: "/tmp/test_nvme0", tier: :hot, capacity: 1_000_000_000},
      %{id: "sata0", path: "/tmp/test_sata0", tier: :cold, capacity: 4_000_000_000}
    ])

    :ok
  end

  defp ensure_registry_with_drives(drives_config) do
    case GenServer.whereis(DriveRegistry) do
      nil ->
        {:ok, _pid} = DriveRegistry.start_link(drives: drives_config, sync_interval_ms: 0)

      _pid ->
        :ets.delete_all_objects(:drive_registry)

        drives_config
        |> Enum.map(&Drive.from_config(&1, Node.self()))
        |> Enum.each(&DriveRegistry.register_drive/1)
    end
  end

  describe "available_tiers/0" do
    test "returns tiers from local active drives" do
      tiers = Topology.available_tiers()
      assert :cold in tiers
      assert :hot in tiers
    end

    test "excludes tiers from standby drives" do
      DriveRegistry.update_state("nvme0", :standby)

      tiers = Topology.available_tiers()
      refute :hot in tiers
      assert :cold in tiers
    end

    test "returns unique tiers" do
      # Add another hot drive
      drive = %Drive{
        id: "nvme1",
        node: Node.self(),
        path: "/tmp/test_nvme1",
        tier: :hot,
        capacity_bytes: 1_000_000_000
      }

      DriveRegistry.register_drive(drive)

      tiers = Topology.available_tiers()
      assert Enum.count(tiers, &(&1 == :hot)) == 1
    end

    test "returns empty when all drives standby" do
      DriveRegistry.update_state("nvme0", :standby)
      DriveRegistry.update_state("sata0", :standby)

      assert Topology.available_tiers() == []
    end
  end

  describe "cluster_tiers/0" do
    test "returns tiers from all drives across cluster" do
      tiers = Topology.cluster_tiers()
      assert :cold in tiers
      assert :hot in tiers
    end

    test "includes tiers from remote drives" do
      remote_drive = %Drive{
        id: "remote_ssd0",
        node: :remote@host,
        path: "/data/ssd0",
        tier: :warm,
        capacity_bytes: 2_000_000_000
      }

      DriveRegistry.register_drive(remote_drive)

      tiers = Topology.cluster_tiers()
      assert :warm in tiers
      assert :hot in tiers
      assert :cold in tiers
    end
  end

  describe "validate_tier_available/1" do
    test "returns :ok for available tier" do
      assert :ok = Topology.validate_tier_available(:hot)
      assert :ok = Topology.validate_tier_available(:cold)
    end

    test "returns error for unavailable tier" do
      assert {:error, :tier_unavailable} = Topology.validate_tier_available(:warm)
    end

    test "returns error when tier drive is in standby" do
      DriveRegistry.update_state("nvme0", :standby)

      assert {:error, :tier_unavailable} = Topology.validate_tier_available(:hot)
    end
  end
end
