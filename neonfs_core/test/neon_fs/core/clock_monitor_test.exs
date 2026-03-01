defmodule NeonFS.Core.ClockMonitorTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.ClockMonitor

  setup do
    ensure_monitor_stopped()

    on_exit(fn ->
      ensure_monitor_stopped()
      cleanup_ets()
    end)

    :ok
  end

  defp ensure_monitor_stopped do
    case GenServer.whereis(ClockMonitor) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal, 5_000)
    end

    cleanup_ets()
  end

  defp cleanup_ets do
    if :ets.whereis(:clock_quarantine) != :undefined do
      :ets.delete(:clock_quarantine)
    end
  rescue
    ArgumentError -> :ok
  end

  defp start_monitor(opts \\ []) do
    defaults = [check_interval_ms: 0, node_lister: fn -> [] end]
    merged = Keyword.merge(defaults, opts)
    start_supervised!({ClockMonitor, merged})
  end

  describe "start_link/1" do
    test "starts the GenServer and creates ETS table" do
      start_monitor()

      assert :ets.whereis(:clock_quarantine) != :undefined
    end
  end

  describe "quarantined?/1" do
    test "returns false for unknown node" do
      start_monitor()

      refute ClockMonitor.quarantined?(:some_node@host)
    end

    test "returns true for quarantined node" do
      start_monitor()

      # Directly insert quarantine entry for testing
      :ets.insert(:clock_quarantine, {:bad_node@host, 1500})

      assert ClockMonitor.quarantined?(:bad_node@host)
    end
  end

  describe "quarantined_nodes/0" do
    test "returns empty list when no nodes quarantined" do
      start_monitor()

      assert ClockMonitor.quarantined_nodes() == []
    end

    test "returns list of quarantined nodes" do
      start_monitor()

      :ets.insert(:clock_quarantine, {:node_a@host, 1500})
      :ets.insert(:clock_quarantine, {:node_b@host, 2000})

      nodes = ClockMonitor.quarantined_nodes()
      assert length(nodes) == 2
      assert :node_a@host in nodes
      assert :node_b@host in nodes
    end
  end

  describe "check_clocks (via :check_clocks message)" do
    test "classifies node with low skew as ok" do
      test_pid = self()

      :telemetry.attach(
        "test-ok-skew",
        [:neonfs, :clock, :skew],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:skew, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-ok-skew") end)

      start_monitor(
        node_lister: fn -> [:peer@host] end,
        time_fetcher: fn :peer@host ->
          # Simulate 10ms skew (within bounds)
          {:ok, System.system_time(:millisecond) + 10}
        end
      )

      send(GenServer.whereis(ClockMonitor), :check_clocks)
      :sys.get_state(ClockMonitor)

      assert_receive {:skew, %{skew_ms: skew}, %{node: :peer@host}}
      assert skew < 200
      refute ClockMonitor.quarantined?(:peer@host)
    end

    test "classifies node with moderate skew as warning" do
      test_pid = self()

      :telemetry.attach(
        "test-warning-skew",
        [:neonfs, :clock, :skew],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:skew, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-warning-skew") end)

      start_monitor(
        warning_threshold_ms: 100,
        node_lister: fn -> [:peer@host] end,
        time_fetcher: fn :peer@host ->
          # Simulate 250ms skew (warning level)
          {:ok, System.system_time(:millisecond) + 250}
        end
      )

      send(GenServer.whereis(ClockMonitor), :check_clocks)
      :sys.get_state(ClockMonitor)

      assert_receive {:skew, %{skew_ms: skew}, %{node: :peer@host}}
      assert skew >= 100
      refute ClockMonitor.quarantined?(:peer@host)
    end

    test "quarantines node with excessive skew" do
      test_pid = self()

      :telemetry.attach(
        "test-quarantine",
        [:neonfs, :clock, :quarantine],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:quarantined, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-quarantine") end)

      start_monitor(
        quarantine_threshold_ms: 500,
        node_lister: fn -> [:bad_node@host] end,
        time_fetcher: fn :bad_node@host ->
          # Simulate 1500ms skew (quarantine level)
          {:ok, System.system_time(:millisecond) + 1500}
        end
      )

      send(GenServer.whereis(ClockMonitor), :check_clocks)
      :sys.get_state(ClockMonitor)

      assert ClockMonitor.quarantined?(:bad_node@host)
      assert_receive {:quarantined, %{skew_ms: _skew}, %{node: :bad_node@host}}
    end

    test "unquarantines node when skew returns to normal" do
      test_pid = self()

      :telemetry.attach(
        "test-unquarantine",
        [:neonfs, :clock, :unquarantine],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:unquarantined, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-unquarantine") end)

      start_monitor(
        quarantine_threshold_ms: 500,
        node_lister: fn -> [:recovering@host] end,
        time_fetcher: fn :recovering@host ->
          # Now within bounds
          {:ok, System.system_time(:millisecond) + 10}
        end
      )

      # Manually quarantine the node first
      :ets.insert(:clock_quarantine, {:recovering@host, 1500})
      assert ClockMonitor.quarantined?(:recovering@host)

      # Trigger check — should unquarantine since skew is now low
      send(GenServer.whereis(ClockMonitor), :check_clocks)
      :sys.get_state(ClockMonitor)

      refute ClockMonitor.quarantined?(:recovering@host)
      assert_receive {:unquarantined, %{node: :recovering@host}}
    end

    test "handles probe failure gracefully" do
      start_monitor(
        node_lister: fn -> [:unreachable@host] end,
        time_fetcher: fn :unreachable@host -> {:error, :nodedown} end
      )

      # Should not crash
      send(GenServer.whereis(ClockMonitor), :check_clocks)
      :sys.get_state(ClockMonitor)

      assert Process.alive?(GenServer.whereis(ClockMonitor))
    end

    test "does not emit duplicate quarantine events" do
      test_pid = self()

      :telemetry.attach(
        "test-no-dup-quarantine",
        [:neonfs, :clock, :quarantine],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:quarantined, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-no-dup-quarantine") end)

      start_monitor(
        quarantine_threshold_ms: 500,
        node_lister: fn -> [:bad@host] end,
        time_fetcher: fn :bad@host ->
          {:ok, System.system_time(:millisecond) + 2000}
        end
      )

      # First check — should quarantine and emit event
      send(GenServer.whereis(ClockMonitor), :check_clocks)
      :sys.get_state(ClockMonitor)
      assert_receive {:quarantined, %{node: :bad@host}}

      # Second check — already quarantined, should NOT emit again
      send(GenServer.whereis(ClockMonitor), :check_clocks)
      :sys.get_state(ClockMonitor)
      refute_receive {:quarantined, _}
    end

    test "probes multiple nodes" do
      test_pid = self()

      :telemetry.attach(
        "test-multi-node",
        [:neonfs, :clock, :skew],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:skew, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-multi-node") end)

      start_monitor(
        node_lister: fn -> [:node_a@host, :node_b@host] end,
        time_fetcher: fn
          :node_a@host -> {:ok, System.system_time(:millisecond) + 5}
          :node_b@host -> {:ok, System.system_time(:millisecond) + 10}
        end
      )

      send(GenServer.whereis(ClockMonitor), :check_clocks)
      :sys.get_state(ClockMonitor)

      assert_receive {:skew, _measurements, %{node: :node_a@host}}
      assert_receive {:skew, _measurements, %{node: :node_b@host}}
    end
  end

  describe "threshold classification" do
    test "classifies skew below warning as ok" do
      test_pid = self()

      :telemetry.attach(
        "test-classify-ok",
        [:neonfs, :clock, :skew],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:skew, measurements.skew_ms, metadata.node})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-classify-ok") end)

      start_monitor(
        warning_threshold_ms: 200,
        critical_threshold_ms: 500,
        quarantine_threshold_ms: 1000,
        node_lister: fn -> [:n@h] end,
        time_fetcher: fn :n@h -> {:ok, System.system_time(:millisecond) + 50} end
      )

      send(GenServer.whereis(ClockMonitor), :check_clocks)
      :sys.get_state(ClockMonitor)

      assert_receive {:skew, skew, :n@h}
      assert skew < 200
    end

    test "classifies skew at critical level" do
      test_pid = self()

      :telemetry.attach(
        "test-classify-critical",
        [:neonfs, :clock, :skew],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:skew, measurements.skew_ms, metadata.node})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-classify-critical") end)

      start_monitor(
        warning_threshold_ms: 200,
        critical_threshold_ms: 500,
        quarantine_threshold_ms: 1000,
        node_lister: fn -> [:n@h] end,
        time_fetcher: fn :n@h -> {:ok, System.system_time(:millisecond) + 600} end
      )

      send(GenServer.whereis(ClockMonitor), :check_clocks)
      :sys.get_state(ClockMonitor)

      assert_receive {:skew, skew, :n@h}
      # Should be at critical level (>=500, <1000) — not quarantined
      assert skew >= 500
      refute ClockMonitor.quarantined?(:n@h)
    end
  end
end
