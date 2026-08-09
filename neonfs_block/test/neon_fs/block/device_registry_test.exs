defmodule NeonFS.Block.DeviceRegistryTest do
  use ExUnit.Case, async: true

  alias NeonFS.Block.DeviceRegistry

  setup do
    registry = start_supervised!({DeviceRegistry, name: :"registry_#{:erlang.unique_integer()}"})
    {:ok, registry: registry}
  end

  describe "attach/3" do
    test "refuses an export that does not name a volume and a path", %{registry: registry} do
      assert {:error, {:malformed_export_name, "nocolon"}} =
               DeviceRegistry.attach("nocolon", self(), registry)

      assert {:error, {:malformed_export_name, ":/dev.img"}} =
               DeviceRegistry.attach(":/dev.img", self(), registry)

      assert {:error, {:malformed_export_name, "vol:"}} =
               DeviceRegistry.attach("vol:", self(), registry)
    end

    test "a failed attach leaves nothing attached", %{registry: registry} do
      DeviceRegistry.attach("nocolon", self(), registry)

      assert DeviceRegistry.attached(registry) == %{}
    end
  end

  describe "detach/3" do
    test "is idempotent for an export this holder never attached", %{registry: registry} do
      assert :ok = DeviceRegistry.detach("vol:/dev.img", self(), registry)
    end
  end
end
