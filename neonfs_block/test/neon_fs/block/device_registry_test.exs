defmodule NeonFS.Block.DeviceRegistryTest do
  use ExUnit.Case, async: true

  alias NeonFS.Block.DeviceRegistry
  alias NeonFS.Core.BlockAttachment
  alias NeonFS.Error.Conflict

  setup do
    registry = start_supervised!({DeviceRegistry, name: :"registry_#{:erlang.unique_integer()}"})
    {:ok, registry: registry}
  end

  defp stub_core do
    test = self()

    Application.put_env(:neonfs_block, :core_call_fn, fn _module, function, args ->
      send(test, {:core_call, function, args})

      {:ok, %{file_id: "file", size: 4096, logical_block_bytes: 512, physical_block_bytes: 512}}
    end)

    on_exit(fn -> Application.delete_env(:neonfs_block, :core_call_fn) end)
  end

  defp stub_coordinator(claim_result) do
    test = self()

    Application.put_env(:neonfs_block, :coordinator_call_fn, fn function, args ->
      send(test, {:coordinator_call, function, args})

      case function do
        :claim_path_for -> claim_result
        :release -> :ok
      end
    end)

    on_exit(fn -> Application.delete_env(:neonfs_block, :coordinator_call_fn) end)
  end

  describe "attach/3" do
    test "refuses an export that names neither a volume nor a path", %{registry: registry} do
      assert {:error, {:malformed_export_name, ""}} =
               DeviceRegistry.attach("", self(), registry)

      assert {:error, {:malformed_export_name, ":/dev.img"}} =
               DeviceRegistry.attach(":/dev.img", self(), registry)

      assert {:error, {:malformed_export_name, "vol:"}} =
               DeviceRegistry.attach("vol:", self(), registry)
    end

    # Resolved from the shared constant rather than asked of core: the value
    # is cluster-wide and `neonfs_client` carries it, so a bare export costs
    # no round trip of its own.
    test "a bare volume names that volume's own device", %{registry: registry} do
      test = self()

      Application.put_env(:neonfs_block, :core_call_fn, fn _module, function, args ->
        send(test, {:core_call, function, args})
        {:error, :not_found}
      end)

      on_exit(fn -> Application.delete_env(:neonfs_block, :core_call_fn) end)

      DeviceRegistry.attach("blockvol", self(), registry)

      expected_path = BlockAttachment.default_device_path()
      assert_receive {:core_call, :open_device, ["blockvol", ^expected_path]}
      refute_received {:core_call, :device_path, []}
    end

    test "a failed attach leaves nothing attached", %{registry: registry} do
      DeviceRegistry.attach(":/dev.img", self(), registry)

      assert DeviceRegistry.attached(registry) == %{}
    end
  end

  describe "detach/3" do
    test "is idempotent for an export this holder never attached", %{registry: registry} do
      assert :ok = DeviceRegistry.detach("vol:/dev.img", self(), registry)
    end
  end

  describe "the attachment claim" do
    test "is taken on the resolved device, so a bare export cannot alias past it", %{
      registry: registry
    } do
      stub_core()
      stub_coordinator({:ok, "claim-1"})

      assert {:ok, _device} = DeviceRegistry.attach("blockvol", self(), registry)

      expected = BlockAttachment.path("blockvol", BlockAttachment.default_device_path())
      assert_receive {:coordinator_call, :claim_path_for, [^expected, :exclusive, _holder]}
    end

    test "is held by the registry, not the connection", %{registry: registry} do
      stub_core()
      stub_coordinator({:ok, "claim-1"})

      assert {:ok, _device} = DeviceRegistry.attach("blockvol:/dev.img", self(), registry)

      assert_receive {:coordinator_call, :claim_path_for, [_path, :exclusive, holder]}
      assert holder == registry
    end

    test "is taken once for an export several connections hold", %{registry: registry} do
      stub_core()
      stub_coordinator({:ok, "claim-1"})

      second = spawn(fn -> receive do: (:stop -> :ok) end)
      on_exit(fn -> Process.exit(second, :kill) end)

      assert {:ok, _device} = DeviceRegistry.attach("blockvol:/dev.img", self(), registry)
      assert {:ok, _device} = DeviceRegistry.attach("blockvol:/dev.img", second, registry)

      assert_receive {:coordinator_call, :claim_path_for, _args}
      refute_receive {:coordinator_call, :claim_path_for, _args}
    end

    test "is released when the last connection detaches", %{registry: registry} do
      stub_core()
      stub_coordinator({:ok, "claim-1"})

      assert {:ok, _device} = DeviceRegistry.attach("blockvol:/dev.img", self(), registry)
      assert :ok = DeviceRegistry.detach("blockvol:/dev.img", self(), registry)

      assert_receive {:coordinator_call, :release, ["claim-1"]}
      assert DeviceRegistry.attached(registry) == %{}
    end

    test "refuses an export whose device is attached elsewhere", %{registry: registry} do
      stub_core()
      stub_coordinator({:error, Conflict.from_reason(:conflict, "claim-9")})

      assert {:error, {:attached_elsewhere, "blockvol:/dev.img"}} =
               DeviceRegistry.attach("blockvol:/dev.img", self(), registry)

      assert DeviceRegistry.attached(registry) == %{}
    end

    test "refuses an export when exclusivity cannot be established at all", %{registry: registry} do
      stub_core()
      stub_coordinator({:error, :all_nodes_unreachable})

      assert {:error, {:attachment_claim_unavailable, {:error, :all_nodes_unreachable}}} =
               DeviceRegistry.attach("blockvol:/dev.img", self(), registry)

      assert DeviceRegistry.attached(registry) == %{}
    end
  end
end
