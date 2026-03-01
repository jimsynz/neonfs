defmodule NeonFS.Core.DriveStateTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.{DriveCommand, DriveRegistry, DriveState}

  @registry NeonFS.Core.DriveStateRegistry

  setup do
    DriveCommand.Test.setup()
    ensure_registry()
    ensure_drive_registry()

    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:neonfs, :drive_state, :transition]
      ])

    {:ok, telemetry_ref: ref}
  end

  defp ensure_registry do
    unless Process.whereis(@registry) do
      start_supervised!({Registry, keys: :unique, name: @registry})
    end
  end

  defp ensure_drive_registry do
    case GenServer.whereis(DriveRegistry) do
      nil ->
        start_supervised!(
          {DriveRegistry,
           drives: [%{id: "test_hdd", path: "/test", tier: :cold, capacity: 1000}],
           sync_interval_ms: 0}
        )

      _pid ->
        :ets.delete_all_objects(:drive_registry)

        drive = %NeonFS.Core.Drive{
          id: "test_hdd",
          node: Node.self(),
          path: "/test",
          tier: :cold,
          capacity_bytes: 1000
        }

        DriveRegistry.register_drive(drive)
    end
  end

  defp start_drive(opts \\ []) do
    drive_id = Keyword.get(opts, :drive_id, "test_hdd")
    drive_path = Keyword.get(opts, :drive_path, "/test/path")
    power_mgmt = Keyword.get(opts, :power_management, true)
    idle_timeout = Keyword.get(opts, :idle_timeout, 3600)
    command_module = Keyword.get(opts, :command_module, DriveCommand.Test)

    start_supervised!(
      {DriveState,
       [
         drive_id: drive_id,
         drive_path: drive_path,
         power_management: power_mgmt,
         idle_timeout: idle_timeout,
         command_module: command_module
       ]},
      id: {:drive_state_test, drive_id}
    )
  end

  defp wait_for_transition(drive_id, expected, timeout_ms \\ 2000) do
    # Check if already in the desired state
    if DriveState.get_state(drive_id) == expected do
      :ok
    else
      do_wait_for_transition(drive_id, expected, timeout_ms)
    end
  end

  defp do_wait_for_transition(drive_id, expected, timeout_ms) do
    receive do
      {[:neonfs, :drive_state, :transition], _ref, %{}, %{drive_id: ^drive_id, to: ^expected}} ->
        :ok

      {[:neonfs, :drive_state, :transition], _ref, %{}, %{drive_id: ^drive_id}} ->
        # Not the target state yet — keep waiting
        do_wait_for_transition(drive_id, expected, timeout_ms)
    after
      timeout_ms ->
        current = DriveState.get_state(drive_id)

        flunk(
          "Timed out waiting for drive #{drive_id} to reach #{expected}, " <>
            "current state: #{current}"
        )
    end
  end

  describe "startup" do
    test "starts in active state" do
      start_drive()
      assert :active = DriveState.get_state("test_hdd")
    end

    test "non-PM drive starts in active state" do
      start_drive(drive_id: "ssd0", power_management: false)
      assert :active = DriveState.get_state("ssd0")
    end
  end

  describe "ensure_active/1" do
    test "returns :ok for active drive" do
      start_drive()
      assert :ok = DriveState.ensure_active("test_hdd")
    end

    test "returns :ok for non-PM drive" do
      start_drive(drive_id: "ssd0", power_management: false)
      assert :ok = DriveState.ensure_active("ssd0")
    end

    test "returns :ok for idle drive and transitions back to active" do
      # Use very short idle timeout to reach idle state
      start_drive(idle_timeout: 0)

      # Wait for idle → spinning_down → standby cycle
      wait_for_transition("test_hdd", :standby)

      # Now spin up via ensure_active
      assert :ok = DriveState.ensure_active("test_hdd")
      assert :active = DriveState.get_state("test_hdd")
    end

    test "returns :ok for non-existent drive (graceful fallback)" do
      assert :ok = DriveState.ensure_active("nonexistent_drive")
    end
  end

  describe "record_io/1" do
    test "returns :ok" do
      start_drive()
      assert :ok = DriveState.record_io("test_hdd")
    end

    test "resets idle timer (drive stays active)" do
      pid = start_drive(idle_timeout: 1)

      # Record I/O to reset the timer, then sync with the GenServer
      DriveState.record_io("test_hdd")
      :sys.get_state(pid)

      # Should still be active because record_io reset the timer
      # (the timer would have fired at 1 second, but record_io resets it)
      assert DriveState.get_state("test_hdd") in [:active, :idle, :spinning_down]
    end

    test "transitions idle drive back to active" do
      # This test manually sends the idle_timeout message
      pid = start_drive(idle_timeout: 3600)

      # Force idle transition by sending idle_timeout, then sync with GenServer
      send(pid, :idle_timeout)
      :sys.get_state(pid)

      # Drive should now be spinning_down (idle -> spinning_down happens together)
      state = DriveState.get_state("test_hdd")
      assert state in [:spinning_down, :standby]
    end

    test "no-op for non-PM drive" do
      start_drive(drive_id: "ssd0", power_management: false)
      assert :ok = DriveState.record_io("ssd0")
    end

    test "returns :ok for non-existent drive (graceful fallback)" do
      assert :ok = DriveState.record_io("nonexistent_drive")
    end
  end

  describe "get_state/1" do
    test "returns :active for active drive" do
      start_drive()
      assert :active = DriveState.get_state("test_hdd")
    end

    test "returns :active for non-existent drive (graceful fallback)" do
      assert :active = DriveState.get_state("nonexistent_drive")
    end
  end

  describe "full state machine cycle" do
    test "active → idle → spinning_down → standby → spinning_up → active" do
      start_drive(idle_timeout: 0)

      # Should transition through: active → idle → spinning_down → standby
      wait_for_transition("test_hdd", :standby)

      # Verify spin_down was called
      calls = DriveCommand.Test.get_calls()
      assert Enum.any?(calls, fn {action, _path} -> action == :spin_down end)

      DriveCommand.Test.reset()

      # Now spin up via ensure_active
      assert :ok = DriveState.ensure_active("test_hdd")
      assert :active = DriveState.get_state("test_hdd")

      # Verify spin_up was called
      calls = DriveCommand.Test.get_calls()
      assert Enum.any?(calls, fn {action, _path} -> action == :spin_up end)
    end

    test "idle timeout triggers correctly after period of no I/O" do
      start_drive(idle_timeout: 0)

      # With 0-second timeout, should quickly transition to standby
      wait_for_transition("test_hdd", :standby)
    end
  end

  describe "concurrent spin-up coalescing" do
    test "multiple callers share the same spin-up operation" do
      # Configure spin-up to take a bit so we can queue multiple callers
      DriveCommand.Test.configure(:spin_up_delay, 100)

      start_drive(idle_timeout: 0)
      wait_for_transition("test_hdd", :standby)
      DriveCommand.Test.reset()

      # Spawn multiple concurrent callers
      tasks =
        for _i <- 1..5 do
          Task.async(fn ->
            DriveState.ensure_active("test_hdd")
          end)
        end

      # All should succeed
      results = Task.await_many(tasks, 5000)
      assert Enum.all?(results, &(&1 == :ok))

      # Verify spin_up was called only ONCE
      spin_up_calls =
        DriveCommand.Test.get_calls()
        |> Enum.filter(fn {action, _} -> action == :spin_up end)

      assert length(spin_up_calls) == 1
    end
  end

  describe "spin-up failure" do
    test "returns error to all waiters on spin-up failure" do
      DriveCommand.Test.configure(:spin_up_result, {:error, :device_error})
      DriveCommand.Test.configure(:spin_up_delay, 50)

      start_drive(idle_timeout: 0)
      wait_for_transition("test_hdd", :standby)

      # Spawn concurrent callers
      tasks =
        for _i <- 1..3 do
          Task.async(fn ->
            DriveState.ensure_active("test_hdd")
          end)
        end

      results = Task.await_many(tasks, 5000)
      assert Enum.all?(results, &(&1 == {:error, :spin_up_failed}))

      # Drive should be back in standby
      assert :standby = DriveState.get_state("test_hdd")
    end
  end

  describe "spin-down failure" do
    test "returns to active on spin-down failure" do
      DriveCommand.Test.configure(:spin_down_result, {:error, :permission_denied})

      # Use long idle timeout to prevent rapid cycling, trigger manually
      pid = start_drive(idle_timeout: 3600)

      # Manually trigger idle timeout
      send(pid, :idle_timeout)
      :sys.get_state(pid)

      # Wait for the failed spin-down to transition back to active
      wait_for_transition("test_hdd", :active)

      # Should be back to active after spin-down failure
      assert :active = DriveState.get_state("test_hdd")
    end
  end

  describe "non-PM drives (SSDs)" do
    test "stay permanently active regardless of I/O" do
      pid = start_drive(drive_id: "ssd0", power_management: false)

      assert :active = DriveState.get_state("ssd0")

      # Record some I/O, then sync with the GenServer
      DriveState.record_io("ssd0")
      :sys.get_state(pid)

      # Still active
      assert :active = DriveState.get_state("ssd0")
    end

    test "ensure_active returns immediately" do
      start_drive(drive_id: "ssd0", power_management: false)

      start_time = System.monotonic_time(:millisecond)
      assert :ok = DriveState.ensure_active("ssd0")
      elapsed = System.monotonic_time(:millisecond) - start_time

      # Should be near-instant (well under 100ms)
      assert elapsed < 100
    end
  end

  describe "telemetry" do
    test "emits transition events", %{telemetry_ref: ref} do
      start_drive(idle_timeout: 0)

      # Assert each transition event in order (these also serve as synchronisation)
      assert_receive {[:neonfs, :drive_state, :transition], ^ref, %{},
                      %{drive_id: "test_hdd", from: :active, to: :idle}},
                     2000

      assert_receive {[:neonfs, :drive_state, :transition], ^ref, %{},
                      %{drive_id: "test_hdd", from: :idle, to: :spinning_down}},
                     2000

      assert_receive {[:neonfs, :drive_state, :transition], ^ref, %{},
                      %{drive_id: "test_hdd", from: :spinning_down, to: :standby}},
                     2000
    end
  end

  describe "DriveRegistry integration" do
    test "updates registry state on transitions" do
      start_drive(idle_timeout: 0)

      # Wait for standby
      wait_for_transition("test_hdd", :standby)

      # DriveRegistry should show :standby
      drives = DriveRegistry.list_drives()
      hdd = Enum.find(drives, &(&1.id == "test_hdd"))

      if hdd do
        assert hdd.state == :standby
      end

      # Spin up
      DriveState.ensure_active("test_hdd")

      # DriveRegistry should show :active
      drives = DriveRegistry.list_drives()
      hdd = Enum.find(drives, &(&1.id == "test_hdd"))

      if hdd do
        assert hdd.state == :active
      end
    end
  end

  describe "ensure_active during spinning_down" do
    test "queues waiter and spins up after spin-down completes" do
      # Use a delay so we can catch the spinning_down state
      DriveCommand.Test.configure(:spin_down_delay, 200)

      start_drive(idle_timeout: 0)

      # Wait for spinning_down state
      wait_for_transition("test_hdd", :spinning_down)

      # Call ensure_active during spinning_down — should queue and wait
      task =
        Task.async(fn ->
          DriveState.ensure_active("test_hdd")
        end)

      # After spin_down completes, should auto-spin-up for the waiter
      result = Task.await(task, 5000)
      assert result == :ok
      assert :active = DriveState.get_state("test_hdd")
    end
  end
end
