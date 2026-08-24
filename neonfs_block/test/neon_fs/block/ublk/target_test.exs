defmodule NeonFS.Block.Ublk.TargetTest do
  @moduledoc """
  Everything a ublk attachment decides on the BEAM side.

  The helper is stood in for by a process that connects to nothing, because
  none of what is under test here is IO: it is that an attachment takes the
  cluster-wide claim, that losing the device releases it, and that a fence
  takes the device rather than being logged and ignored. Serving actual bytes
  is `NeonFS.Block.Ublk.QueueTest`, and needing a kernel with ublk in it is
  the rig's.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Block.{DeviceRegistry, StubCore}
  alias NeonFS.Block.Ublk.{Capability, Supervisor}

  @export "stub:/dev.img"

  setup do
    StubCore.report_to(self())
    Application.put_env(:neonfs_block, :io_core, StubCore)

    Application.put_env(:neonfs_block, :coordinator_call_fn, fn
      :claim_path_for, _args -> {:ok, "claim"}
      :release, _args -> :ok
    end)

    Application.put_env(:neonfs_block, :ublk_helper, fake_helper())

    on_exit(fn ->
      for key <- [:io_core, :coordinator_call_fn, :ublk_helper, :ublk_control_path] do
        Application.delete_env(:neonfs_block, key)
      end

      Capability.refresh()

      Application.delete_env(:neonfs_block, :stub_core_test_pid)
    end)

    start_supervised!(DeviceRegistry)
    start_supervised!({Registry, keys: :unique, name: Supervisor.registry()})
    start_supervised!(Supervisor)

    :ok
  end

  describe "a host without the driver" do
    test "refuses the attach, naming the path it looked for" do
      without_driver()

      assert {:error, {:ublk_driver_absent, "/dev/does-not-exist"}} = Supervisor.attach(@export)
      assert Supervisor.attached() == []
    end

    # It has to be decided before the device is opened, or a refused attach
    # leaves the cluster-wide claim taken by nothing.
    test "does not take the attachment claim" do
      without_driver()

      assert {:error, _reason} = Supervisor.attach(@export)
      assert DeviceRegistry.attached() == %{}
    end
  end

  describe "an attachment" do
    setup do
      {:ok, control} = with_control_device()
      {:ok, control: control}
    end

    test "holds the device through the registry, as a connection does" do
      assert {:ok, pid} = Supervisor.attach(@export)

      assert Supervisor.attached() == [@export]
      assert DeviceRegistry.attached() == %{@export => 1}
      assert Process.alive?(pid)
    end

    test "is idempotent: a second attach is the same target" do
      assert {:ok, pid} = Supervisor.attach(@export)
      assert {:ok, ^pid} = Supervisor.attach(@export)

      assert Supervisor.attached() == [@export]
    end

    test "releases the device when it goes" do
      assert {:ok, _pid} = Supervisor.attach(@export)
      assert :ok = Supervisor.detach(@export)

      assert Supervisor.attached() == []
      assert DeviceRegistry.attached() == %{}
    end

    test "detaching what is not attached is not an error" do
      assert :ok = Supervisor.detach("stub:/never.img")
    end

    # A fenced device belongs to a newer epoch elsewhere. Logging it and
    # carrying on would leave this node serving a guest from an attachment
    # the cluster has already given away.
    test "a fence takes the device down" do
      assert {:ok, pid} = Supervisor.attach(@export)
      monitor = Process.monitor(pid)

      send(pid, {:fenced, @export, 7})

      assert_receive {:DOWN, ^monitor, :process, ^pid, {:shutdown, {:fenced, 7}}}, 5_000
      assert Supervisor.attached() == []
    end

    # Before `USER_RECOVERY` this took the device down. The kernel now holds
    # it quiesced instead, so the device — and its path, which is the part a
    # guest cannot survive changing — outlives the process serving it.
    test "the helper dying is recovered in place" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :block, :ublk, :recovery_started],
          [:neonfs, :block, :ublk, :recovery_completed]
        ])

      on_exit(fn -> :telemetry.detach(ref) end)

      assert {:ok, pid} = Supervisor.attach(@export)
      %{helper: helper, device_path: path} = :sys.get_state(pid)

      Port.close(helper)

      assert_receive {[:neonfs, :block, :ublk, :recovery_started], ^ref, %{attempt: 1}, _meta},
                     5_000

      assert_receive {[:neonfs, :block, :ublk, :recovery_completed], ^ref, _measurements, _meta},
                     5_000

      assert Process.alive?(pid)
      assert Supervisor.attached() == [@export]
      assert DeviceRegistry.attached() == %{@export => 1}

      # The path is the whole point: a guest holding `/dev/ublkbN` across the
      # restart is what recovery is for.
      assert %{device_path: ^path, helper: replacement} = :sys.get_state(pid)
      assert replacement != helper
    end

    test "the replacement helper is told it is recovering, not creating" do
      assert {:ok, pid} = Supervisor.attach(@export)
      %{helper: helper} = :sys.get_state(pid)

      Port.close(helper)
      wait_for_recovery()

      # The fake helper writes its environment out, which is the only way to
      # see from here what the real one would be told.
      assert File.read!(recovery_witness()) =~ "recover=1"
    end

    # A helper that cannot serve this device would otherwise hold the
    # attachment claim against a device nothing can use.
    test "a helper that keeps dying exhausts the budget and takes the device" do
      Application.put_env(:neonfs_block, :ublk_helper, suicidal_helper())

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :block, :ublk, :recovery_exhausted]
        ])

      on_exit(fn -> :telemetry.detach(ref) end)

      assert {:ok, pid} = Supervisor.attach(@export)
      monitor = Process.monitor(pid)

      assert_receive {[:neonfs, :block, :ublk, :recovery_exhausted], ^ref, %{attempts: 5}, _meta},
                     15_000

      assert_receive {:DOWN, ^monitor, :process, ^pid, {:ublk_recovery_exhausted, _cause}},
                     5_000

      assert Supervisor.attached() == []
      assert DeviceRegistry.attached() == %{}
    end

    # Resuming a fenced device is the one thing fencing exists to prevent:
    # another node owns it, and every IO in flight belongs to a dead epoch.
    test "a fence is not recovered" do
      assert {:ok, pid} = Supervisor.attach(@export)
      monitor = Process.monitor(pid)

      send(pid, {:fenced, @export, 11})

      assert_receive {:DOWN, ^monitor, :process, ^pid, {:shutdown, {:fenced, 11}}}, 5_000
      assert Supervisor.attached() == []
    end

    test "its sockets are named per queue and cleaned up" do
      assert {:ok, pid} = Supervisor.attach(@export, queues: 2)

      %{listeners: listeners} = :sys.get_state(pid)
      assert map_size(listeners) == 2
      paths = Enum.map(listeners, fn {queue, %{path: path}} -> {queue, path} end) |> Enum.sort()
      assert [{0, zero}, {1, one}] = paths
      assert String.ends_with?(zero, ".0")
      assert String.ends_with?(one, ".1")
      assert File.exists?(zero) and File.exists?(one)

      assert :ok = Supervisor.detach(@export)
      refute File.exists?(zero)
      refute File.exists?(one)
    end
  end

  defp without_driver do
    Application.put_env(:neonfs_block, :ublk_control_path, "/dev/does-not-exist")
    Capability.refresh()
  end

  defp with_control_device do
    path = Path.join(System.tmp_dir!(), "ublk-control-#{System.unique_integer([:positive])}")
    File.write!(path, "")
    Application.put_env(:neonfs_block, :ublk_control_path, path)
    Capability.refresh()
    on_exit(fn -> File.rm(path) end)
    {:ok, path}
  end

  # Stands in for the real helper at the only two points the target reads it:
  # it announces the device the target asked for, then holds its stdin open so
  # the port stays live until something closes it. It creates no device, which
  # nothing here opens. It also records whether it was told to recover, since
  # that instruction is otherwise invisible from this side.
  defp fake_helper do
    witness = recovery_witness()

    write_helper("""
    #!/bin/sh
    echo "recover=${NEONFS_UBLK_RECOVER:-0}" > #{witness}
    echo "neonfs_ublk: ready ${NEONFS_UBLK_ID}"
    exec cat
    """)
  end

  # Announces the device and then dies, which is a helper that cannot serve
  # it however many times it is restarted.
  defp suicidal_helper do
    write_helper("""
    #!/bin/sh
    echo "neonfs_ublk: ready ${NEONFS_UBLK_ID}"
    exit 1
    """)
  end

  defp write_helper(script) do
    path = Path.join(System.tmp_dir!(), "fake-ublk-#{System.unique_integer([:positive])}")
    File.write!(path, script)
    File.chmod!(path, 0o755)
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp recovery_witness do
    Process.get(:recovery_witness) ||
      tap(
        Path.join(System.tmp_dir!(), "ublk-recover-#{System.unique_integer([:positive])}"),
        fn path ->
          Process.put(:recovery_witness, path)
          on_exit(fn -> File.rm(path) end)
        end
      )
  end

  defp wait_for_recovery do
    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:neonfs, :block, :ublk, :recovery_completed]
      ])

    receive do
      {[:neonfs, :block, :ublk, :recovery_completed], ^ref, _measurements, _meta} -> :ok
    after
      5_000 -> flunk("the device never recovered")
    end
  after
    :ok
  end
end
