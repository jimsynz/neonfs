defmodule NeonFS.IO.SupervisorTest do
  use ExUnit.Case, async: false

  alias NeonFS.IO.WorkerSupervisor

  # Mock DriveRegistry that returns configurable drives
  defmodule MockDriveRegistry do
    use Agent

    def start_link(drives) do
      Agent.start_link(fn -> drives end, name: __MODULE__)
    end

    def list_drives do
      Agent.get(__MODULE__, & &1)
    end
  end

  # Mock VolumeRegistry for Producer
  defmodule MockVolumeRegistry do
    def get(_volume_id), do: {:ok, %{io_weight: 100}}
  end

  defp unique_names do
    suffix = System.unique_integer([:positive])

    %{
      supervisor: :"io_sup_#{suffix}",
      producer: :"io_prod_#{suffix}",
      adjuster: :"io_adj_#{suffix}",
      worker_sup: :"io_wsup_#{suffix}"
    }
  end

  defp start_tree(names, opts \\ []) do
    drives = Keyword.get(opts, :drives, [])
    drive_registry_mod = Keyword.get(opts, :drive_registry_mod, MockDriveRegistry)

    # Start mock drive registry with the given drives
    start_supervised!({MockDriveRegistry, drives}, id: :"mock_dr_#{names.supervisor}")

    # Start the global worker registry (used by WorkerSupervisor)
    start_supervised!(
      {Registry, keys: :unique, name: NeonFS.IO.WorkerRegistry},
      id: NeonFS.IO.WorkerRegistry
    )

    sup_opts = [
      name: names.supervisor,
      drive_registry_mod: drive_registry_mod,
      producer_opts: [name: names.producer, volume_registry_mod: MockVolumeRegistry],
      adjuster_opts: [name: names.adjuster, producer: names.producer, check_interval: 86_400_000],
      worker_supervisor_opts: [name: names.worker_sup]
    ]

    start_supervised!({NeonFS.IO.Supervisor, sup_opts}, id: names.supervisor)

    names
  end

  # Waits for a registered name to appear with a new PID (after a restart).
  defp await_name_registered(name, timeout \\ 1000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_await_name(name, deadline)
  end

  defp do_await_name(name, deadline) do
    case Process.whereis(name) do
      pid when is_pid(pid) ->
        pid

      nil ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(5)
          do_await_name(name, deadline)
        else
          raise "Timed out waiting for #{inspect(name)} to be registered"
        end
    end
  end

  describe "supervision tree structure" do
    test "starts with expected children" do
      names = unique_names()
      start_tree(names)

      # Producer is running
      assert Process.whereis(names.producer) |> is_pid()

      # PriorityAdjuster is running
      assert Process.whereis(names.adjuster) |> is_pid()

      # WorkerSupervisor is running
      assert Process.whereis(names.worker_sup) |> is_pid()
    end

    test "producer crash does not kill the priority adjuster" do
      names = unique_names()
      start_tree(names)

      adjuster_pid = Process.whereis(names.adjuster)
      producer_pid = Process.whereis(names.producer)
      assert is_pid(adjuster_pid)
      assert is_pid(producer_pid)

      # Monitor to detect restart
      ref = Process.monitor(producer_pid)

      # Kill the producer
      Process.exit(producer_pid, :kill)

      # Wait for the producer to be restarted
      assert_receive {:DOWN, ^ref, :process, ^producer_pid, :killed}, 1000

      # Wait for the supervisor to restart the producer under a new PID
      new_producer = await_name_registered(names.producer)

      # Adjuster should still be the same process (not restarted)
      assert Process.alive?(adjuster_pid)
      assert Process.whereis(names.adjuster) == adjuster_pid

      # Producer should be restarted (new PID)
      assert is_pid(new_producer)
      assert new_producer != producer_pid
    end
  end

  describe "drive worker lifecycle" do
    test "starts workers for local drives on init" do
      names = unique_names()

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :io, :supervisor, :discovery_complete]
        ])

      drives = [
        %NeonFS.Core.Drive{
          id: "nvme0",
          node: Node.self(),
          path: "/data/nvme0",
          tier: :hot
        },
        %NeonFS.Core.Drive{
          id: "sda1",
          node: Node.self(),
          path: "/data/sda1",
          tier: :cold
        }
      ]

      start_tree(names, drives: drives)

      # Wait for discovery to complete via telemetry
      assert_receive {[:neonfs, :io, :supervisor, :discovery_complete], ^ref, %{count: 2}, %{}},
                     1_000

      # Both workers should be running
      assert {:ok, _pid} = WorkerSupervisor.lookup_worker("nvme0")
      assert {:ok, _pid} = WorkerSupervisor.lookup_worker("sda1")
    end

    test "does not start workers for remote drives" do
      names = unique_names()

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :io, :supervisor, :discovery_complete]
        ])

      drives = [
        %NeonFS.Core.Drive{
          id: "remote-nvme",
          node: :remote@elsewhere,
          path: "/data/nvme0",
          tier: :hot
        }
      ]

      start_tree(names, drives: drives)

      # Wait for discovery to complete — count should be 0 (remote drives filtered out)
      assert_receive {[:neonfs, :io, :supervisor, :discovery_complete], ^ref, %{count: 0}, %{}},
                     1_000

      assert :error = WorkerSupervisor.lookup_worker("remote-nvme")
    end

    test "adding a drive starts a new worker" do
      names = unique_names()
      start_tree(names)

      # Start a worker manually (simulating what telemetry handler does)
      assert {:ok, pid} =
               WorkerSupervisor.start_worker(
                 drive_id: "new-drive",
                 drive_type: :ssd,
                 producer: names.producer,
                 supervisor: names.worker_sup
               )

      assert is_pid(pid)
      assert {:ok, ^pid} = WorkerSupervisor.lookup_worker("new-drive")
    end

    test "removing a drive stops its worker" do
      names = unique_names()
      start_tree(names)

      # Start a worker first
      {:ok, pid} =
        WorkerSupervisor.start_worker(
          drive_id: "doomed-drive",
          drive_type: :ssd,
          producer: names.producer,
          supervisor: names.worker_sup
        )

      assert Process.alive?(pid)

      # Monitor before stopping so we can wait for the DOWN message,
      # which guarantees the Registry has unregistered the process.
      ref = Process.monitor(pid)

      # Stop it
      assert :ok = WorkerSupervisor.stop_worker("doomed-drive", supervisor: names.worker_sup)

      # Wait for the process to fully terminate
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000

      # Worker should be gone
      assert :error = WorkerSupervisor.lookup_worker("doomed-drive")
    end

    test "stopping a non-existent worker returns error" do
      names = unique_names()
      start_tree(names)

      assert {:error, :not_found} =
               WorkerSupervisor.stop_worker("ghost-drive", supervisor: names.worker_sup)
    end

    test "worker crash is isolated to that drive only" do
      names = unique_names()
      start_tree(names)

      # Start two workers
      {:ok, pid_a} =
        WorkerSupervisor.start_worker(
          drive_id: "drive-a",
          drive_type: :ssd,
          producer: names.producer,
          supervisor: names.worker_sup
        )

      {:ok, pid_b} =
        WorkerSupervisor.start_worker(
          drive_id: "drive-b",
          drive_type: :ssd,
          producer: names.producer,
          supervisor: names.worker_sup
        )

      # Kill drive-a's worker
      ref = Process.monitor(pid_a)
      Process.exit(pid_a, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid_a, :killed}, 1000

      # Wait for the registry to reflect the restarted worker
      # The DynamicSupervisor restarts the child and re-registers it
      assert {:ok, new_pid_a} =
               wait_until(fn ->
                 case WorkerSupervisor.lookup_worker("drive-a") do
                   {:ok, pid} when pid != pid_a -> {:ok, pid}
                   _ -> :retry
                 end
               end)

      # drive-b should still be alive (same PID)
      assert Process.alive?(pid_b)

      # drive-a should have been restarted with a new PID
      assert new_pid_a != pid_a
      assert Process.alive?(new_pid_a)
    end
  end

  describe "telemetry-driven lifecycle" do
    test "drive_manager:add event starts a worker" do
      names = unique_names()
      start_tree(names)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :io, :supervisor, :worker_started]
        ])

      # Simulate the telemetry event that DriveManager emits
      :telemetry.execute(
        [:neonfs, :drive_manager, :add],
        %{},
        %{drive_id: "telem-drive", tier: :hot}
      )

      # Wait for the worker_started telemetry confirming the handler acted
      assert_receive {[:neonfs, :io, :supervisor, :worker_started], ^ref, %{},
                      %{drive_id: "telem-drive", drive_type: :ssd}},
                     1_000

      assert {:ok, pid} = WorkerSupervisor.lookup_worker("telem-drive")
      assert is_pid(pid)
    end

    test "drive_manager:remove event stops a worker" do
      names = unique_names()
      start_tree(names)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :io, :supervisor, :worker_stopped]
        ])

      # Start a worker first
      {:ok, pid} =
        WorkerSupervisor.start_worker(
          drive_id: "telem-remove",
          drive_type: :ssd,
          producer: names.producer,
          supervisor: names.worker_sup
        )

      assert Process.alive?(pid)

      # Simulate the telemetry event that DriveManager emits
      :telemetry.execute(
        [:neonfs, :drive_manager, :remove],
        %{},
        %{drive_id: "telem-remove"}
      )

      # Wait for the worker_stopped telemetry confirming the handler acted
      assert_receive {[:neonfs, :io, :supervisor, :worker_stopped], ^ref, %{},
                      %{drive_id: "telem-remove"}},
                     1_000

      refute Process.alive?(pid)
    end
  end

  # Polls a function until it returns a non-:retry value or times out.
  defp wait_until(fun, timeout \\ 1000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    case fun.() do
      :retry ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(5)
          do_wait_until(fun, deadline)
        else
          flunk("wait_until timed out")
        end

      result ->
        result
    end
  end
end
