defmodule NeonFS.CSI.BlockFrontendTest do
  @moduledoc """
  Which frontend a block volume gets, and what is said when the forced one
  is not on offer.

  Discovery is stubbed rather than run, because what is under test is the
  decision a registration leads to — a real cluster would let a topology
  decide the answer instead of the test.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Client.{Discovery, ServiceInfo}
  alias NeonFS.CSI.BlockFrontend

  @volume "vol-1"

  setup :verify_on_exit!

  setup do
    on_exit(fn ->
      Application.delete_env(:neonfs_csi, :block_frontend)
      Application.delete_env(:neonfs_csi, :block_call_fn)
    end)

    :ok
  end

  describe "auto" do
    test "takes ublk when a block service on this host offers it" do
      discovers([block_service(Node.self(), [:nbd, :ublk])])

      assert {:ok, :ublk, node} = BlockFrontend.select(:auto)
      assert node == Node.self()
    end

    # The device would be created on the far host's kernel, where nothing
    # here can open it.
    test "falls back to NBD when the only ublk-capable service is elsewhere" do
      discovers([block_service(:"neonfs_block@somewhere-else", [:nbd, :ublk])])

      assert {:ok, :nbd} = BlockFrontend.select(:auto)
    end

    test "falls back to NBD when the local service offers only NBD" do
      discovers([block_service(Node.self(), [:nbd])])

      assert {:ok, :nbd} = BlockFrontend.select(:auto)
    end

    test "falls back to NBD when nothing is discovered at all" do
      discovers([])

      assert {:ok, :nbd} = BlockFrontend.select(:auto)
    end
  end

  describe "forcing" do
    test "NBD is honoured even where ublk would work" do
      discovers([block_service(Node.self(), [:nbd, :ublk])])

      assert {:ok, :nbd} = BlockFrontend.select(:nbd)
    end

    # Each of these has a different fix, so each is a different error: one
    # says co-locate a block target, the other says fix ublk on the target
    # that is already here.
    test "ublk with no local service names this host" do
      discovers([block_service(:neonfs_block@elsewhere, [:nbd, :ublk])])

      assert {:error, {:frontend_forced_unavailable, :ublk, {:no_local_block_service, host}}} =
               BlockFrontend.select(:ublk)

      assert is_binary(host)
    end

    test "ublk with a local service that lacks it names that service" do
      discovers([block_service(Node.self(), [:nbd])])

      assert {:error,
              {:frontend_forced_unavailable, :ublk, {:local_block_service_lacks_ublk, nodes}}} =
               BlockFrontend.select(:ublk)

      assert nodes == [Node.self()]
    end

    test "a frontend nothing implements is refused rather than defaulted" do
      assert {:error, {:unknown_frontend, :scsi}} = BlockFrontend.select(:scsi)
    end
  end

  describe "the preference" do
    test "is auto unless configured" do
      assert BlockFrontend.preference() == :auto
    end

    test "is read from config by select/0" do
      discovers([block_service(Node.self(), [:nbd, :ublk])])
      Application.put_env(:neonfs_csi, :block_frontend, :nbd)

      assert {:ok, :nbd} = BlockFrontend.select()
    end
  end

  describe "attaching" do
    test "over NBD, through the caller's own attach" do
      discovers([])

      assert {:ok, attachment} = BlockFrontend.attach(@volume, fn _id -> {:ok, "/dev/nbd3"} end)
      assert attachment == %{device_path: "/dev/nbd3", frontend: :nbd, node: Node.self()}
    end

    test "an NBD attach that fails is reported, not retried as ublk" do
      discovers([])

      assert {:error, :no_free_device} =
               BlockFrontend.attach(@volume, fn _id -> {:error, :no_free_device} end)
    end

    test "over ublk, by asking the local block node for a device" do
      discovers([block_service(Node.self(), [:nbd, :ublk])])
      test = self()

      block_calls(fn node, :attach_ublk, [volume, _opts] ->
        send(test, {:attach_ublk, node, volume})
        {:ok, "/dev/ublkb2"}
      end)

      assert {:ok, attachment} = BlockFrontend.attach(@volume, fn _id -> flunk("not NBD") end)
      assert attachment == %{device_path: "/dev/ublkb2", frontend: :ublk, node: Node.self()}
      assert_received {:attach_ublk, _node, @volume}
    end

    # Which node refused matters: the answer is on that host, not this one.
    test "a ublk attach that fails names the node that refused" do
      discovers([block_service(Node.self(), [:nbd, :ublk])])
      block_calls(fn _node, :attach_ublk, _args -> {:error, {:ublk_helper_absent, "/x"}} end)

      assert {:error, {:ublk_attach_failed, node, {:ublk_helper_absent, "/x"}}} =
               BlockFrontend.attach(@volume, fn _id -> flunk("not NBD") end)

      assert node == Node.self()
    end

    test "a selection failure never reaches the attach" do
      discovers([block_service(Node.self(), [:nbd])])
      Application.put_env(:neonfs_csi, :block_frontend, :ublk)

      assert {:error, {:frontend_forced_unavailable, :ublk, _reason}} =
               BlockFrontend.attach(@volume, fn _id -> flunk("must not attach") end)
    end
  end

  describe "detaching" do
    test "an NBD attachment goes through the caller's detach" do
      test = self()

      record = %{device_path: "/dev/nbd3", frontend: :nbd, node: Node.self()}

      assert :ok =
               BlockFrontend.detach(record, fn path ->
                 send(test, {:detached, path})
                 :ok
               end)

      assert_received {:detached, "/dev/nbd3"}
    end

    test "a ublk attachment is undone by the node that owns it" do
      test = self()

      block_calls(fn node, :detach_ublk, [export] ->
        send(test, {:detach_ublk, node, export})
        :ok
      end)

      record = %{
        device_path: "/dev/ublkb2",
        frontend: :ublk,
        node: Node.self(),
        volume_id: @volume
      }

      assert :ok = BlockFrontend.detach(record, fn _path -> flunk("not NBD") end)
      assert_received {:detach_ublk, _node, @volume}
    end

    test "a ublk detach that fails names the node that refused" do
      block_calls(fn _node, :detach_ublk, _args -> {:error, :not_attached} end)

      record = %{frontend: :ublk, node: Node.self(), volume_id: @volume}

      assert {:error, {:ublk_detach_failed, _node, :not_attached}} =
               BlockFrontend.detach(record, fn _path -> flunk("not NBD") end)
    end

    # A record from before the frontend was recorded is an NBD one, because
    # NBD is all there was — and it still has a device path to hand back.
    test "a record with no frontend detaches as NBD" do
      test = self()

      assert :ok =
               BlockFrontend.detach(%{device_path: "/dev/nbd7"}, fn path ->
                 send(test, {:detached, path})
                 :ok
               end)

      assert_received {:detached, "/dev/nbd7"}
    end
  end

  defp discovers(services) do
    stub(Discovery, :list_by_type, fn :block -> services end)
  end

  defp block_calls(fun) do
    Application.put_env(:neonfs_csi, :block_call_fn, fn node, _module, function, args ->
      fun.(node, function, args)
    end)
  end

  defp block_service(node, capabilities) do
    ServiceInfo.new(node, :block, metadata: %{capabilities: capabilities})
  end
end
