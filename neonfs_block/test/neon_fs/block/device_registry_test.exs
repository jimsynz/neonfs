defmodule NeonFS.Block.DeviceRegistryTest do
  use ExUnit.Case, async: true

  alias NeonFS.Block.DeviceRegistry

  setup do
    registry = start_supervised!({DeviceRegistry, name: :"registry_#{:erlang.unique_integer()}"})
    {:ok, registry: registry}
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

    test "a bare volume names that volume's own device", %{registry: registry} do
      test = self()

      Application.put_env(:neonfs_block, :core_call_fn, fn _module, function, args ->
        send(test, {:core_call, function, args})

        case function do
          :device_path -> "/dev.img"
          :open_device -> {:error, :not_found}
        end
      end)

      on_exit(fn -> Application.delete_env(:neonfs_block, :core_call_fn) end)

      DeviceRegistry.attach("blockvol", self(), registry)

      assert_receive {:core_call, :device_path, []}
      assert_receive {:core_call, :open_device, ["blockvol", "/dev.img"]}
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
end
