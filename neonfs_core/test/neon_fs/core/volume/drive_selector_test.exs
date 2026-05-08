defmodule NeonFS.Core.Volume.DriveSelectorTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Core.Volume.DriveSelector

  describe "select_replicas/2 — replicate" do
    test "picks `factor` drives spread across distinct nodes when possible" do
      drives =
        for node <- [:n1@h, :n2@h, :n3@h, :n4@h, :n5@h] do
          drive(node, "drv-#{node}-1")
        end

      assert {:ok, picked} =
               DriveSelector.select_replicas(replicate(3, 2), drives)

      assert length(picked) == 3
      assert length(Enum.uniq(picked)) == 3
    end

    test "rolls over to a second drive on the same node only after exhausting other nodes" do
      drives = [
        drive(:n1@h, "n1-a"),
        drive(:n1@h, "n1-b"),
        drive(:n2@h, "n2-a"),
        drive(:n2@h, "n2-b")
      ]

      assert {:ok, picked} = DriveSelector.select_replicas(replicate(3, 2), drives)

      # First two picks should hit different nodes; third pick fills in.
      [d1, d2, _d3] = Enum.map(picked, &drive_lookup(drives, &1))
      assert d1.node != d2.node
    end

    test "is deterministic" do
      drives =
        Enum.shuffle(
          for n <- 1..5 do
            drive(:"n#{n}@h", "drv-#{n}")
          end
        )

      assert {:ok, picked_a} = DriveSelector.select_replicas(replicate(3, 2), drives)

      assert {:ok, picked_b} =
               DriveSelector.select_replicas(replicate(3, 2), Enum.reverse(drives))

      assert picked_a == picked_b
    end

    test "returns insufficient_drives when fewer than min_copies are available" do
      drives = [drive(:n1@h, "drv-1")]

      assert {:error, :insufficient_drives, %{available: 1, needed: 2}} =
               DriveSelector.select_replicas(replicate(3, 2), drives)
    end

    test "returns the drives it has when target > available but minimum is met" do
      drives = [drive(:n1@h, "drv-1"), drive(:n2@h, "drv-2")]

      # factor=3, min_copies=2 → 2 drives available, meets minimum.
      assert {:ok, picked} = DriveSelector.select_replicas(replicate(3, 2), drives)
      assert length(picked) == 2
    end

    test "deduplicates by drive_id when the input has duplicates" do
      d = drive(:n1@h, "drv-dupe")
      drives = [d, d, drive(:n2@h, "drv-other")]

      assert {:ok, picked} = DriveSelector.select_replicas(replicate(2, 1), drives)
      assert length(picked) == 2
      assert length(Enum.uniq(picked)) == 2
    end
  end

  describe "select_replicas/2 — erasure" do
    test "picks data + parity drives" do
      drives = for n <- 1..6, do: drive(:"n#{n}@h", "drv-#{n}")

      assert {:ok, picked} = DriveSelector.select_replicas(erasure(4, 2), drives)
      assert length(picked) == 6
    end

    test "minimum is data_chunks (parity is best-effort)" do
      drives = [
        drive(:n1@h, "drv-1"),
        drive(:n2@h, "drv-2"),
        drive(:n3@h, "drv-3"),
        drive(:n4@h, "drv-4")
      ]

      # data=3 parity=2 → minimum is 3, available is 4, target is 5.
      assert {:ok, picked} = DriveSelector.select_replicas(erasure(3, 2), drives)
      assert length(picked) == 4

      # data=5 parity=2 → minimum is 5, available is 4 → fails.
      assert {:error, :insufficient_drives, %{available: 4, needed: 5}} =
               DriveSelector.select_replicas(erasure(5, 2), drives)
    end
  end

  describe "select_replicas/2 — input shape" do
    test "accepts the map shape returned by MetadataStateMachine.get_drives/1" do
      drives_map = %{
        "drv-1" => drive(:n1@h, "drv-1"),
        "drv-2" => drive(:n2@h, "drv-2")
      }

      assert {:ok, picked} = DriveSelector.select_replicas(replicate(2, 1), drives_map)
      assert length(picked) == 2
    end
  end

  describe "select_replicas/2 — properties" do
    property "result length is min(target, available) when minimum is met" do
      check all(
              factor <- integer(1..6),
              min_copies <- integer(1..factor),
              drive_count <- integer(0..10)
            ) do
        drives = for n <- 1..drive_count//1, do: drive(:"n#{n}@h", "drv-#{n}")

        case DriveSelector.select_replicas(replicate(factor, min_copies), drives) do
          {:ok, picked} ->
            assert length(picked) == min(factor, drive_count)
            assert length(picked) >= min_copies
            # No duplicate drives in the output.
            assert length(Enum.uniq(picked)) == length(picked)

          {:error, :insufficient_drives, %{available: a, needed: n}} ->
            assert a == drive_count
            assert n == min_copies
            assert a < n
        end
      end
    end

    property "deterministic across input order permutations" do
      check all(drive_count <- integer(1..8), seed <- integer(0..1_000_000)) do
        # Build a deterministic drive set then shuffle it twice with
        # different seeds. Both shuffles must produce the same result.
        drives = for n <- 1..drive_count, do: drive(:"n#{rem(n, 4)}@h", "drv-#{n}")

        a = drives |> Enum.shuffle() |> shuffle_with(seed)
        b = drives |> Enum.shuffle() |> shuffle_with(seed + 1)

        result_a = DriveSelector.select_replicas(replicate(3, 1), a)
        result_b = DriveSelector.select_replicas(replicate(3, 1), b)

        assert result_a == result_b
      end
    end
  end

  ## Helpers

  defp drive(node, drive_id) do
    %{
      drive_id: drive_id,
      node: node,
      cluster_id: "clust",
      on_disk_format_version: 1,
      registered_at: DateTime.utc_now()
    }
  end

  defp drive_lookup(drives, {node, drive_id}),
    do: Enum.find(drives, &(&1.node == node and &1.drive_id == drive_id))

  defp replicate(factor, min_copies),
    do: %{type: :replicate, factor: factor, min_copies: min_copies}

  defp erasure(data_chunks, parity_chunks),
    do: %{type: :erasure, data_chunks: data_chunks, parity_chunks: parity_chunks}

  defp shuffle_with(list, seed) do
    :rand.seed(:exsss, {seed, seed * 31, seed * 71})
    Enum.shuffle(list)
  end
end
