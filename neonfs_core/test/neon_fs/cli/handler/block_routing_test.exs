defmodule NeonFS.CLI.Handler.BlockRoutingTest do
  @moduledoc """
  What the CLI's block commands route where, and what they say when the
  frontend they resolved cannot be attached from a block node.

  The block nodes are stubbed at the RPC boundary rather than run, because
  what is under test is the routing and the shape of the answer — a real
  ublk attach needs a kernel with the driver, which this suite does not have
  and which is the rig's to prove.
  """

  use NeonFS.TestCase, async: false

  alias NeonFS.CLI.Handler
  alias NeonFS.Client.ServiceInfo
  alias NeonFS.Core.RaServer

  @moduletag :tmp_dir

  defmodule BlockRPCStub do
    @moduledoc false

    @doc false
    def call(node, module, function, args, _timeout) do
      Agent.update(:block_rpc_calls, &[{function, node} | &1])
      answer(node, module, function, args)
    end

    defp answer(node, NeonFS.Block, :select, [preference]) do
      case {capability(node), preference} do
        {:ok, :nbd} -> {:ok, :nbd}
        {:ok, _any} -> {:ok, :ublk}
        {{:error, reason}, :ublk} -> {:error, {:frontend_forced_unavailable, :ublk, reason}}
        {{:error, _reason}, _auto_or_nbd} -> {:ok, :nbd}
      end
    end

    defp answer(_node, NeonFS.Block, :attach_ublk, _args), do: {:ok, "/dev/ublkb0"}
    defp answer(_node, NeonFS.Block, :detach_ublk, _args), do: :ok

    defp answer(node, NeonFS.Block, :frontends, _args) do
      case capability(node) do
        :ok -> [:nbd, :ublk]
        {:error, _reason} -> [:nbd]
      end
    end

    defp answer(node, NeonFS.Block.Ublk.Capability, :check, _args), do: capability(node)

    defp answer(_node, NeonFS.Block.Ublk.Supervisor, :attached, _args) do
      Application.get_env(:neonfs_core, :block_stub_attached, [])
    end

    defp answer(_node, NeonFS.Block.DeviceRegistry, :attached, _args) do
      Application.get_env(:neonfs_core, :block_stub_nbd_attached, %{})
    end

    defp capability(_node) do
      Application.get_env(
        :neonfs_core,
        :block_stub_capability,
        {:error, {:ublk_driver_absent, "/dev/ublk-control"}}
      )
    end
  end

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    ensure_cluster_state()
    stop_ra()
    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()
    start_service_registry()

    register_service!(
      ServiceInfo.new(Node.self(), :block,
        metadata: %{capabilities: [:nbd], nbd_endpoint: {"10.0.0.4", 10_809}}
      )
    )

    Application.put_env(:neonfs_core, :block_rpc_mod, BlockRPCStub)

    start_supervised!(%{
      id: :block_rpc_calls,
      start: {Agent, :start_link, [fn -> [] end, [name: :block_rpc_calls]]}
    })

    on_exit(fn ->
      for key <- [
            :block_rpc_mod,
            :block_stub_capability,
            :block_stub_attached,
            :block_stub_nbd_attached
          ] do
        Application.delete_env(:neonfs_core, key)
      end

      stop_ra()
      cleanup_test_dirs()
    end)

    :ok
  end

  describe "attach/2 over ublk" do
    setup do
      with_ublk()
      :ok
    end

    test "attaches on a block node and answers with the device path there" do
      assert {:ok, attachment} = Handler.block_attach("vol:/dev.img", "auto")

      assert attachment.frontend == :ublk
      assert attachment.device_path == "/dev/ublkb0"
      assert attachment.node == Node.self()
      assert {:attach_ublk, _node} = Enum.find(calls(), &match?({:attach_ublk, _}, &1))
    end

    test "the frontend is resolved on the node that will serve it" do
      assert {:ok, _attachment} = Handler.block_attach("vol", "auto")

      assert {:select, Node.self()} in calls()
    end
  end

  # Nothing was attached, and the answer has to be unmistakable about it —
  # an operator told "attached over nbd" would go looking for a device that
  # exists on no machine.
  describe "attach/2 when the frontend resolves to NBD" do
    test "reports the endpoint instead of claiming an attach" do
      assert {:ok, attachment} = Handler.block_attach("vol", "auto")

      assert attachment.frontend == :nbd
      assert attachment.attached == false
      assert attachment.endpoint == %{host: "10.0.0.4", port: 10_809}
      refute Map.has_key?(attachment, :device_path)
    end

    test "says why ublk was not used, naming the check that failed" do
      assert {:ok, attachment} = Handler.block_attach("vol", "auto")

      # The remedy, not the term name: an operator reading this needs to know
      # to load the module, and `{:ublk_driver_absent, _}` does not say that.
      assert attachment.reason =~ "/dev/ublk-control is absent"
      assert attachment.reason =~ "modprobe ublk_drv"
    end

    test "nothing is attached, so no attach is attempted" do
      assert {:ok, _attachment} = Handler.block_attach("vol", "auto")

      refute Enum.any?(calls(), &match?({:attach_ublk, _}, &1))
    end
  end

  describe "attach/2 with a forced frontend" do
    # This is the scriptable form: a benchmark that asked for ublk and got
    # NBD would measure the wrong thing and report the wrong name.
    test "forcing ublk where it is unavailable fails rather than falling back" do
      assert {:error, error} = Handler.block_attach("vol", "ublk")
      assert inspect(error) =~ "ublk"
      refute Enum.any?(calls(), &match?({:attach_ublk, _}, &1))
    end

    test "forcing nbd on a ublk-capable node is honoured" do
      with_ublk()

      assert {:ok, attachment} = Handler.block_attach("vol", "nbd")
      assert attachment.frontend == :nbd
    end

    test "a frontend nothing implements is refused before any node is asked" do
      assert {:error, error} = Handler.block_attach("vol", "scsi")
      assert inspect(error) =~ "unknown frontend"
      assert calls() == []
    end
  end

  describe "detach/1" do
    test "detaches only where the export is actually attached" do
      Application.put_env(:neonfs_core, :block_stub_attached, ["vol"])

      assert {:ok, %{export: "vol", detached: [entry]}} = Handler.block_detach("vol")
      assert entry.detached
      assert entry.node == Node.self()
    end

    # The kubelet-shaped problem: a detach that fails on something already
    # gone makes a caller retry forever.
    test "an export nothing has attached is not an error" do
      assert {:ok, %{detached: []}} = Handler.block_detach("vol")
      refute Enum.any?(calls(), &match?({:detach_ublk, _}, &1))
    end
  end

  describe "list_devices/0" do
    test "reports both routes, since either can be holding a volume" do
      Application.put_env(:neonfs_core, :block_stub_attached, ["ublk-vol"])
      Application.put_env(:neonfs_core, :block_stub_nbd_attached, %{"nbd-vol" => 2})

      assert {:ok, devices} = Handler.list_block_devices()

      assert %{export: "ublk-vol", frontend: :ublk} = Enum.find(devices, &(&1.frontend == :ublk))
      nbd = Enum.find(devices, &(&1.frontend == :nbd))
      assert nbd.export == "nbd-vol"
      assert nbd.holders == 2
    end

    test "nothing attached anywhere is an empty list, not an error" do
      assert {:ok, []} = Handler.list_block_devices()
    end
  end

  describe "frontends/0" do
    test "names the failed check when ublk is not on offer" do
      assert {:ok, [node]} = Handler.block_frontends()

      assert node.node == Node.self()
      assert node.frontends == [:nbd]
      assert node.ublk_unavailable =~ "/dev/ublk-control is absent"
      assert node.ublk_unavailable =~ "modprobe ublk_drv"
    end

    # The rig's actual failure: the driver is loaded and the control device is
    # there, but the daemon may not open it. "Unavailable" would send an
    # operator to `modprobe` for a permission problem.
    test "a control device it cannot open names the permission, not the driver" do
      Application.put_env(
        :neonfs_core,
        :block_stub_capability,
        {:error, {:ublk_control_inaccessible, "/dev/ublk-control", :eacces}}
      )

      assert {:ok, [node]} = Handler.block_frontends()

      assert node.frontends == [:nbd]
      assert node.ublk_unavailable =~ "may not open it"
      assert node.ublk_unavailable =~ "grant the daemon's user access"
      refute node.ublk_unavailable =~ "modprobe"
    end

    test "reports both frontends and no reason when ublk works" do
      with_ublk()

      assert {:ok, [node]} = Handler.block_frontends()
      assert node.frontends == [:nbd, :ublk]
      assert node.ublk_unavailable == nil
    end
  end

  defp with_ublk, do: Application.put_env(:neonfs_core, :block_stub_capability, :ok)

  defp calls, do: Agent.get(:block_rpc_calls, & &1)
end
