defmodule NeonFS.Core.ClusterRecoveryMonitorTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.ClusterRecoveryMonitor

  # Agent-backed mock of the ClusterMode facade: records set_mode/2 calls
  # and answers recovering?/0 and entry/0 from a seedable entry.
  defmodule MockClusterMode do
    @moduledoc false
    use Agent

    def start_link(entry),
      do: Agent.start_link(fn -> %{entry: entry, calls: []} end, name: __MODULE__)

    def recovering?, do: Agent.get(__MODULE__, fn %{entry: e} -> e[:mode] == :recovering end)

    def entry, do: Agent.get(__MODULE__, fn %{entry: e} -> e end)

    def set_mode(mode, reason) do
      Agent.update(__MODULE__, fn st ->
        %{
          st
          | entry: %{mode: mode, updated_at: DateTime.utc_now(), reason: reason},
            calls: st.calls ++ [{mode, reason}]
        }
      end)

      :ok
    end

    def calls, do: Agent.get(__MODULE__, & &1.calls)

    def put_entry(entry), do: Agent.update(__MODULE__, &%{&1 | entry: entry})
  end

  defmodule MockDriveTrust do
    @moduledoc false
    use Agent

    def start_link(unverified), do: Agent.start_link(fn -> unverified end, name: __MODULE__)
    def unverified, do: Agent.get(__MODULE__, & &1)
  end

  defp attach(events) do
    ref = make_ref()

    :telemetry.attach_many(
      "crm-test-#{inspect(ref)}",
      events,
      fn event, meas, meta, parent -> send(parent, {event, ref, meas, meta}) end,
      self()
    )

    on_exit(fn -> :telemetry.detach("crm-test-#{inspect(ref)}") end)
    ref
  end

  # Starts the monitor with the given injected deps. `settle_ms` and
  # `check_interval_ms` are huge so the only tick is the one the test sends
  # manually, making assertions deterministic.
  defp start_monitor(opts) do
    defaults = [
      name: :"crm_#{System.unique_integer([:positive])}",
      settle_ms: 3_600_000,
      check_interval_ms: 3_600_000,
      cluster_mode_mod: MockClusterMode,
      drive_trust_mod: MockDriveTrust,
      members_online_fn: fn -> true end,
      now_fn: fn -> DateTime.utc_now() end
    ]

    start_supervised!({ClusterRecoveryMonitor, Keyword.merge(defaults, opts)})
  end

  defp tick(pid) do
    send(pid, :tick)
    :sys.get_state(pid)
  end

  describe "entry detection (#1437)" do
    test "a fresh cluster (not auto-restarted) never enters :recovering" do
      MockClusterMode.start_link(%{mode: :normal})
      ref = attach([[:neonfs, :cluster_recovery, :entered]])

      pid =
        start_monitor(auto_restarted_fn: fn -> false end, leader_status_fn: fn -> :leader end)

      state = tick(pid)

      assert state.entry_decided
      assert MockClusterMode.calls() == []
      refute_received {[:neonfs, :cluster_recovery, :entered], ^ref, _, _}
    end

    test "an auto-restarted leader enters :recovering" do
      MockClusterMode.start_link(%{mode: :normal})
      ref = attach([[:neonfs, :cluster_recovery, :entered]])

      pid = start_monitor(auto_restarted_fn: fn -> true end, leader_status_fn: fn -> :leader end)

      tick(pid)

      assert [{:recovering, "cold quorum reform"}] = MockClusterMode.calls()
      assert MockClusterMode.recovering?()
      assert_received {[:neonfs, :cluster_recovery, :entered], ^ref, %{}, %{node: _}}
    end

    test "an auto-restarted follower does not enter (its leader will)" do
      MockClusterMode.start_link(%{mode: :normal})

      pid =
        start_monitor(auto_restarted_fn: fn -> true end, leader_status_fn: fn -> :follower end)

      state = tick(pid)

      assert state.entry_decided
      assert MockClusterMode.calls() == []
    end

    test "waits across ticks while no leader, then gives up when the window closes" do
      MockClusterMode.start_link(%{mode: :normal})

      pid =
        start_monitor(
          auto_restarted_fn: fn -> true end,
          leader_status_fn: fn -> :no_leader end,
          entry_window_ticks: 2
        )

      state = tick(pid)
      refute state.entry_decided
      assert state.ticks == 1

      state = tick(pid)
      assert state.entry_decided
      assert state.ticks == 2
      assert MockClusterMode.calls() == []
    end
  end

  describe "exit (#1437)" do
    test "leader exits :recovering once reassembled (members back, no unverified drives)" do
      MockClusterMode.start_link(%{
        mode: :recovering,
        updated_at: DateTime.utc_now(),
        reason: "x"
      })

      MockDriveTrust.start_link([])
      ref = attach([[:neonfs, :cluster_recovery, :exited]])

      pid =
        start_monitor(
          leader_status_fn: fn -> :leader end,
          members_online_fn: fn -> true end
        )

      tick(pid)

      assert {:normal, "reassembly complete"} in MockClusterMode.calls()
      refute MockClusterMode.recovering?()
      assert_received {[:neonfs, :cluster_recovery, :exited], ^ref, %{}, %{reason: _}}
    end

    test "stays :recovering while drives are still unverified" do
      MockClusterMode.start_link(%{
        mode: :recovering,
        updated_at: DateTime.utc_now(),
        reason: "x"
      })

      MockDriveTrust.start_link([{:node@host, "drv-1"}])

      pid =
        start_monitor(
          leader_status_fn: fn -> :leader end,
          members_online_fn: fn -> true end
        )

      tick(pid)

      assert MockClusterMode.calls() == []
      assert MockClusterMode.recovering?()
    end

    test "exits on the timeout backstop even when not reassembled" do
      old = DateTime.add(DateTime.utc_now(), -3600, :second)
      MockClusterMode.start_link(%{mode: :recovering, updated_at: old, reason: "x"})
      MockDriveTrust.start_link([{:node@host, "drv-1"}])

      pid =
        start_monitor(
          leader_status_fn: fn -> :leader end,
          members_online_fn: fn -> false end,
          recovery_timeout_ms: 1_800_000
        )

      tick(pid)

      assert {:normal, "recovery timeout"} in MockClusterMode.calls()
    end

    test "a follower does not drive exit even when reassembled" do
      MockClusterMode.start_link(%{
        mode: :recovering,
        updated_at: DateTime.utc_now(),
        reason: "x"
      })

      MockDriveTrust.start_link([])

      pid =
        start_monitor(
          leader_status_fn: fn -> :follower end,
          members_online_fn: fn -> true end
        )

      tick(pid)

      assert MockClusterMode.calls() == []
      assert MockClusterMode.recovering?()
    end
  end
end
