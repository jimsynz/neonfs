defmodule NeonFS.Core.StripePlacementTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{Drive, DriveRegistry, StripePlacement}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_empty_drive_registry()

    on_exit(fn -> cleanup_test_dirs() end)
    :ok
  end

  # Start DriveRegistry with no default drives so we control topology
  defp start_empty_drive_registry do
    if pid = Process.whereis(NeonFS.Core.DriveRegistry) do
      GenServer.stop(pid, :normal, 5_000)
    end

    if :ets.info(:drive_registry) != :undefined do
      :ets.delete(:drive_registry)
    end

    start_supervised!(
      {NeonFS.Core.DriveRegistry, drives: [], sync_interval_ms: 0},
      restart: :temporary
    )
  end

  describe "select_targets/2 with multiple nodes" do
    setup do
      register_drives([
        %{id: "n1-d1", node: :node1@host, tier: :hot, capacity: 100_000, used: 10_000},
        %{id: "n2-d1", node: :node2@host, tier: :hot, capacity: 100_000, used: 20_000},
        %{id: "n3-d1", node: :node3@host, tier: :hot, capacity: 100_000, used: 30_000}
      ])

      :ok
    end

    test "distributes chunks across 3 nodes for a 2+1 stripe" do
      config = %{data_chunks: 2, parity_chunks: 1}
      assert {:ok, targets} = StripePlacement.select_targets(config, tier: :hot)
      assert length(targets) == 3

      nodes = Enum.map(targets, & &1.node)
      assert length(Enum.uniq(nodes)) == 3
    end

    test "distributes 14 chunks across 3 nodes for a 10+4 stripe" do
      config = %{data_chunks: 10, parity_chunks: 4}
      assert {:ok, targets} = StripePlacement.select_targets(config, tier: :hot)
      assert length(targets) == 14

      nodes = Enum.map(targets, & &1.node)
      assert length(Enum.uniq(nodes)) == 3

      # Each node should have roughly equal chunks (14/3 ≈ 4-5 each)
      by_node = Enum.frequencies(nodes)
      counts = Map.values(by_node)
      assert Enum.min(counts) >= 4
      assert Enum.max(counts) <= 5
    end

    test "assigns each chunk on a different node when enough nodes" do
      # Add more nodes to have 6 total
      register_drives([
        %{id: "n4-d1", node: :node4@host, tier: :hot, capacity: 100_000, used: 5_000},
        %{id: "n5-d1", node: :node5@host, tier: :hot, capacity: 100_000, used: 5_000},
        %{id: "n6-d1", node: :node6@host, tier: :hot, capacity: 100_000, used: 5_000}
      ])

      config = %{data_chunks: 4, parity_chunks: 2}
      assert {:ok, targets} = StripePlacement.select_targets(config, tier: :hot)
      assert length(targets) == 6

      nodes = Enum.map(targets, & &1.node)
      assert length(Enum.uniq(nodes)) == 6
    end
  end

  describe "select_targets/2 with single node, multiple drives" do
    setup do
      register_drives([
        %{id: "d1", node: node(), tier: :hot, capacity: 100_000, used: 10_000},
        %{id: "d2", node: node(), tier: :hot, capacity: 100_000, used: 20_000},
        %{id: "d3", node: node(), tier: :hot, capacity: 100_000, used: 30_000}
      ])

      :ok
    end

    test "spreads chunks across 3 drives" do
      config = %{data_chunks: 2, parity_chunks: 1}
      assert {:ok, targets} = StripePlacement.select_targets(config, tier: :hot)
      assert length(targets) == 3

      drives = Enum.map(targets, & &1.drive_id)
      assert length(Enum.uniq(drives)) == 3
    end

    test "cycles drives when more chunks than drives" do
      config = %{data_chunks: 4, parity_chunks: 2}
      assert {:ok, targets} = StripePlacement.select_targets(config, tier: :hot)
      assert length(targets) == 6

      # All should be on current node
      assert Enum.all?(targets, &(&1.node == node()))

      # Should use all 3 drives
      drives = Enum.map(targets, & &1.drive_id) |> Enum.uniq()
      assert length(drives) == 3
    end
  end

  describe "select_targets/2 with single node, single drive" do
    setup do
      register_drives([
        %{id: "only-drive", node: node(), tier: :hot, capacity: 100_000, used: 10_000}
      ])

      :ok
    end

    test "places all chunks on single drive" do
      config = %{data_chunks: 4, parity_chunks: 2}
      assert {:ok, targets} = StripePlacement.select_targets(config, tier: :hot)
      assert length(targets) == 6
      assert Enum.all?(targets, &(&1.drive_id == "only-drive"))
    end
  end

  describe "select_targets/2 capacity filtering" do
    test "excludes drives above 90% utilization" do
      register_drives([
        %{id: "full", node: :node1@host, tier: :hot, capacity: 100, used: 95},
        %{id: "ok", node: :node2@host, tier: :hot, capacity: 100, used: 50}
      ])

      config = %{data_chunks: 2, parity_chunks: 1}
      assert {:ok, targets} = StripePlacement.select_targets(config, tier: :hot)

      # Full drive excluded, all go to "ok" drive
      assert Enum.all?(targets, &(&1.drive_id == "ok"))
    end

    test "returns error when no drives available" do
      config = %{data_chunks: 2, parity_chunks: 1}
      assert {:error, :no_drives_available} = StripePlacement.select_targets(config, tier: :hot)
    end

    test "returns error when all drives are full" do
      register_drives([
        %{id: "full1", node: :node1@host, tier: :hot, capacity: 100, used: 95},
        %{id: "full2", node: :node2@host, tier: :hot, capacity: 100, used: 92}
      ])

      config = %{data_chunks: 2, parity_chunks: 1}
      assert {:error, :no_drives_available} = StripePlacement.select_targets(config, tier: :hot)
    end
  end

  describe "select_targets/2 tier filtering" do
    test "only selects drives from the requested tier" do
      register_drives([
        %{id: "hot-d1", node: :node1@host, tier: :hot, capacity: 100_000, used: 10_000},
        %{id: "warm-d1", node: :node2@host, tier: :warm, capacity: 100_000, used: 10_000}
      ])

      config = %{data_chunks: 2, parity_chunks: 1}
      assert {:ok, targets} = StripePlacement.select_targets(config, tier: :hot)

      # All targets should be on the hot drive's node
      assert Enum.all?(targets, &(&1.drive_id == "hot-d1"))
    end
  end

  describe "select_targets/2 standby drive filtering" do
    test "excludes standby drives" do
      register_drives([
        %{id: "active", node: :node1@host, tier: :hot, capacity: 100_000, used: 10_000},
        %{
          id: "standby",
          node: :node2@host,
          tier: :hot,
          capacity: 100_000,
          used: 10_000,
          state: :standby
        }
      ])

      config = %{data_chunks: 2, parity_chunks: 1}
      assert {:ok, targets} = StripePlacement.select_targets(config, tier: :hot)
      assert Enum.all?(targets, &(&1.drive_id == "active"))
    end
  end

  describe "validate_placement/2" do
    test "returns :ok when enough distinct nodes" do
      targets = [
        %{node: :n1, drive_id: "d1"},
        %{node: :n2, drive_id: "d1"},
        %{node: :n3, drive_id: "d1"}
      ]

      config = %{data_chunks: 2, parity_chunks: 1}
      assert :ok = StripePlacement.validate_placement(targets, config)
    end

    test "returns warning when insufficient failure domains" do
      targets = [
        %{node: :n1, drive_id: "d1"},
        %{node: :n1, drive_id: "d2"},
        %{node: :n1, drive_id: "d3"}
      ]

      config = %{data_chunks: 2, parity_chunks: 1}
      assert {:warning, msg} = StripePlacement.validate_placement(targets, config)
      assert msg =~ "1 distinct node"
      assert msg =~ "Need 2+ nodes"
    end

    test "returns :ok for erasure 10+4 with 5 nodes" do
      targets =
        for i <- 0..13 do
          node = :"n#{rem(i, 5)}@host"
          %{node: node, drive_id: "d1"}
        end

      config = %{data_chunks: 10, parity_chunks: 4}
      assert :ok = StripePlacement.validate_placement(targets, config)
    end

    test "warns for erasure 10+4 with only 3 nodes" do
      targets =
        for i <- 0..13 do
          node = :"n#{rem(i, 3)}@host"
          %{node: node, drive_id: "d1"}
        end

      config = %{data_chunks: 10, parity_chunks: 4}
      assert {:warning, _msg} = StripePlacement.validate_placement(targets, config)
    end
  end

  describe "select_targets/2 with mixed node/drive topology" do
    test "2 nodes with 2 drives each distributes evenly" do
      register_drives([
        %{id: "n1-d1", node: :node1@host, tier: :hot, capacity: 100_000, used: 10_000},
        %{id: "n1-d2", node: :node1@host, tier: :hot, capacity: 100_000, used: 20_000},
        %{id: "n2-d1", node: :node2@host, tier: :hot, capacity: 100_000, used: 10_000},
        %{id: "n2-d2", node: :node2@host, tier: :hot, capacity: 100_000, used: 20_000}
      ])

      config = %{data_chunks: 4, parity_chunks: 2}
      assert {:ok, targets} = StripePlacement.select_targets(config, tier: :hot)
      assert length(targets) == 6

      by_node = Enum.frequencies_by(targets, & &1.node)
      assert by_node[:node1@host] == 3
      assert by_node[:node2@host] == 3
    end
  end

  # ─── Helpers ──────────────────────────────────────────────────────────

  defp register_drives(drive_specs) do
    Enum.each(drive_specs, fn spec ->
      drive = %Drive{
        id: spec.id,
        node: spec.node,
        path: "/data/#{spec.id}",
        tier: spec.tier,
        capacity_bytes: spec[:capacity] || 100_000,
        used_bytes: spec[:used] || 0,
        state: spec[:state] || :active
      }

      DriveRegistry.register_drive(drive)
    end)
  end
end
