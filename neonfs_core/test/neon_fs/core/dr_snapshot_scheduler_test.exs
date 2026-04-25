defmodule NeonFS.Core.DRSnapshotSchedulerTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.DRSnapshotScheduler

  ## Pure-function retention math

  describe "compute_retention/2" do
    @now ~U[2026-04-25 12:00:00Z]

    defp snap(id, days_ago) when is_integer(days_ago) do
      %{id: id, created_at: DateTime.add(@now, -days_ago * 86_400, :second)}
    end

    test "keeps every snapshot in the daily window up to `daily` count" do
      # 6 snapshots, all distinct days, well within the 7-day window
      # → all 6 kept as :daily.
      snaps = for i <- 0..5, do: snap("snap-d#{i}", i)
      classes = DRSnapshotScheduler.compute_retention(snaps, now: @now)

      assert Map.values(classes) |> Enum.uniq() == [:daily]
    end

    test "older-than-daily-window falls into weekly bucket" do
      # newest 7 days take :daily slots; 8-day-old falls into weekly.
      dailies = for i <- 0..6, do: snap("d#{i}", i)
      snaps = dailies ++ [snap("w1", 8)]

      classes = DRSnapshotScheduler.compute_retention(snaps, now: @now)
      assert classes["w1"] == :weekly
    end

    test "monthlies promoted from snapshots outside the weekly window" do
      # 0..6 → daily, 8..14 → maybe weekly, 35 days → monthly.
      snaps = [
        snap("d0", 0),
        snap("d1", 1),
        snap("w1", 10),
        snap("m1", 35)
      ]

      classes = DRSnapshotScheduler.compute_retention(snaps, now: @now)
      assert classes["m1"] == :monthly
    end

    test "snapshots older than every window are pruned" do
      ancient = snap("ancient", 365 * 2)

      classes = DRSnapshotScheduler.compute_retention([ancient], now: @now)
      assert classes["ancient"] == :prune
    end

    test "respects custom bucket sizes" do
      snaps =
        for i <- 0..9 do
          # 0 hours, 24h, 48h, …, 9*24h ago — each in its own UTC day.
          %{id: "s#{i}", created_at: DateTime.add(@now, -i * 86_400, :second)}
        end

      classes = DRSnapshotScheduler.compute_retention(snaps, now: @now, daily: 3)

      kept_dailies = for {id, :daily} <- classes, do: id
      assert length(kept_dailies) == 3
    end

    test "two snapshots in the same UTC day collapse to one daily slot" do
      snaps = [
        %{id: "morning", created_at: ~U[2026-04-25 06:00:00Z]},
        %{id: "evening", created_at: ~U[2026-04-25 22:00:00Z]}
      ]

      classes = DRSnapshotScheduler.compute_retention(snaps, now: @now, daily: 7)

      # The newer one (sorted desc) wins the daily slot; the older
      # one falls through to the next bucket (or :prune).
      assert classes["evening"] == :daily
      refute classes["morning"] == :daily
    end
  end

  ## GenServer wiring

  describe "GenServer" do
    test "tick on a non-leader node skips snapshot creation" do
      test_pid = self()
      name = :"sched_#{System.unique_integer([:positive])}"

      {:ok, _pid} =
        DRSnapshotScheduler.start_link(
          name: name,
          enabled: false,
          leader_check_fn: fn -> false end,
          create_fn: fn _ ->
            send(test_pid, :create_called)
            {:ok, %{path: "/dr/foo", manifest: %{}}}
          end
        )

      assert {:ok, %{snapshot: :skipped}} = DRSnapshotScheduler.run_now(name)
      refute_received :create_called, 100
    end

    test "tick on a leader calls create_fn and emits the tick telemetry event" do
      test_pid = self()
      ref = make_ref()
      name = :"sched_#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach(
          {__MODULE__, ref},
          [:neonfs, :dr_snapshot_scheduler, :tick],
          fn _, m, md, _ -> send(test_pid, {:tick, ref, m, md}) end,
          nil
        )

      try do
        {:ok, _pid} =
          DRSnapshotScheduler.start_link(
            name: name,
            enabled: false,
            leader_check_fn: fn -> true end,
            create_fn: fn _ ->
              {:ok, %{path: "/dr/20260425T000000Z", manifest: %{}}}
            end
          )

        assert {:ok, %{snapshot: %{path: "/dr/" <> _}, pruned: pruned}} =
                 DRSnapshotScheduler.run_now(name)

        # Pruning runs even when there are no snapshots — when
        # `_system` doesn't exist (no Ra cluster up in this unit
        # test) `list_snapshots/0` short-circuits and returns no ids
        # to prune.
        assert is_list(pruned)

        assert_received {:tick, ^ref, %{pruned_count: _}, _}
      after
        :telemetry.detach({__MODULE__, ref})
      end
    end

    test "create_fn returning :error is logged and doesn't crash" do
      name = :"sched_#{System.unique_integer([:positive])}"

      {:ok, _pid} =
        DRSnapshotScheduler.start_link(
          name: name,
          enabled: false,
          leader_check_fn: fn -> true end,
          create_fn: fn _ -> {:error, :nope} end
        )

      assert {:ok, %{snapshot: {:error, :nope}, pruned: []}} =
               DRSnapshotScheduler.run_now(name)
    end

    test "enabled: false starts without scheduling a timer" do
      name = :"sched_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        DRSnapshotScheduler.start_link(
          name: name,
          enabled: false,
          leader_check_fn: fn -> true end,
          create_fn: fn _ -> {:ok, %{path: "/dr/x", manifest: %{}}} end
        )

      assert :sys.get_state(pid).timer_ref == nil
    end
  end
end
