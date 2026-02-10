defmodule NeonFS.Core.ReadRepairTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.ReadRepair

  setup do
    # Start BackgroundWorker dependencies
    start_supervised!({Task.Supervisor, name: NeonFS.Core.BackgroundTaskSupervisor})
    start_supervised!(NeonFS.Core.BackgroundWorker)
    start_supervised!({ReadRepair, coalesce_window_ms: 200})
    :ok
  end

  describe "submit/3" do
    test "submits repair jobs for stale replicas" do
      test_pid = self()

      repair_context = %{
        segment_id: :crypto.strong_rand_bytes(32),
        latest_value: %{test: true},
        opts: [
          local_node: node()
        ]
      }

      # We can't easily verify the actual repair write without a real MetadataStore,
      # but we can verify the telemetry events fire
      :telemetry.attach(
        "test-repair-submitted",
        [:neonfs, :read_repair, :submitted],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:repair_submitted, metadata})
        end,
        nil
      )

      ReadRepair.submit("test:key1", repair_context, [:fake_node@host])

      assert_receive {:repair_submitted, %{key: "test:key1"}}, 1_000

      :telemetry.detach("test-repair-submitted")
    end

    test "coalesces multiple repairs for the same key" do
      test_pid = self()
      submitted_count = :counters.new(1, [])

      :telemetry.attach(
        "test-coalesce",
        [:neonfs, :read_repair, :submitted],
        fn _event, _measurements, _metadata, _config ->
          :counters.add(submitted_count, 1, 1)
          send(test_pid, :submitted)
        end,
        nil
      )

      repair_context = %{
        segment_id: :crypto.strong_rand_bytes(32),
        latest_value: %{test: true},
        opts: [local_node: node()]
      }

      # Submit multiple repairs for the same key within the coalescing window
      ReadRepair.submit("test:coalesce_key", repair_context, [:fake_node@host])
      ReadRepair.submit("test:coalesce_key", repair_context, [:fake_node@host])
      ReadRepair.submit("test:coalesce_key", repair_context, [:fake_node@host])

      # Wait a bit for the casts to process
      Process.sleep(100)

      # Only one should have been submitted (the first one)
      assert :counters.get(submitted_count, 1) == 1

      :telemetry.detach("test-coalesce")
    end

    test "allows repairs after coalescing window expires" do
      test_pid = self()
      submitted_count = :counters.new(1, [])

      :telemetry.attach(
        "test-coalesce-expiry",
        [:neonfs, :read_repair, :submitted],
        fn _event, _measurements, _metadata, _config ->
          :counters.add(submitted_count, 1, 1)
          send(test_pid, :submitted)
        end,
        nil
      )

      repair_context = %{
        segment_id: :crypto.strong_rand_bytes(32),
        latest_value: %{test: true},
        opts: [local_node: node()]
      }

      ReadRepair.submit("test:expiry_key", repair_context, [:fake_node@host])
      assert_receive :submitted, 1_000

      # Wait for coalescing window to expire (200ms configured)
      Process.sleep(250)

      ReadRepair.submit("test:expiry_key", repair_context, [:fake_node@host])
      assert_receive :submitted, 1_000

      assert :counters.get(submitted_count, 1) == 2

      :telemetry.detach("test-coalesce-expiry")
    end

    test "different keys are not coalesced" do
      test_pid = self()
      submitted_count = :counters.new(1, [])

      :telemetry.attach(
        "test-different-keys",
        [:neonfs, :read_repair, :submitted],
        fn _event, _measurements, _metadata, _config ->
          :counters.add(submitted_count, 1, 1)
          send(test_pid, :submitted)
        end,
        nil
      )

      repair_context = %{
        segment_id: :crypto.strong_rand_bytes(32),
        latest_value: %{test: true},
        opts: [local_node: node()]
      }

      ReadRepair.submit("test:key_a", repair_context, [:fake_node@host])
      ReadRepair.submit("test:key_b", repair_context, [:fake_node@host])

      # Both should be submitted
      assert_receive :submitted, 1_000
      assert_receive :submitted, 1_000

      assert :counters.get(submitted_count, 1) == 2

      :telemetry.detach("test-different-keys")
    end
  end
end
