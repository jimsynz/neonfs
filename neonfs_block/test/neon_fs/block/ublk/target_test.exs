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

    test "the helper dying takes the device down" do
      assert {:ok, pid} = Supervisor.attach(@export)
      monitor = Process.monitor(pid)

      %{helper: helper} = :sys.get_state(pid)
      Port.close(helper)

      assert_receive {:DOWN, ^monitor, :process, ^pid, {:helper_exited, _reason}}, 5_000
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
  # nothing here opens.
  defp fake_helper do
    path = Path.join(System.tmp_dir!(), "fake-ublk-#{System.unique_integer([:positive])}")

    File.write!(path, """
    #!/bin/sh
    echo "neonfs_ublk: ready ${NEONFS_UBLK_ID}"
    exec cat
    """)

    File.chmod!(path, 0o755)
    on_exit(fn -> File.rm(path) end)
    path
  end
end
