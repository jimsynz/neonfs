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

      {:ok,
       %{
         id: "device",
         size: 4096,
         chunk_bytes: 4096,
         epoch: 0,
         logical_block_bytes: 512,
         physical_block_bytes: 512
       }}
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

    # `NBD_FLAG_CAN_MULTI_CONN` promises a client that a read on one socket
    # sees a write on another and that a flush covers both. What makes that
    # true is this: the window is the *device's*, so every connection buffers
    # into and reads out of the same one. A window started per attach would
    # let two sockets hold different views of one extent and lose whichever
    # drained first.
    test "several connections to one export share one write window", %{registry: registry} do
      stub_core()
      stub_coordinator({:ok, "claim-1"})

      second = spawn(fn -> receive do: (:stop -> :ok) end)
      on_exit(fn -> Process.exit(second, :kill) end)

      assert {:ok, first_device} = DeviceRegistry.attach("blockvol:/dev.img", self(), registry)
      assert {:ok, second_device} = DeviceRegistry.attach("blockvol:/dev.img", second, registry)

      assert is_pid(first_device.window)
      assert first_device.window == second_device.window
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

  # A fence is per device, so it takes every holder of it — including the
  # ones that have only been reading, which are the dangerous ones because
  # they look healthy.
  describe "fenced/3" do
    test "tells every holder and drops the device", %{registry: registry} do
      stub_core()
      stub_coordinator({:ok, "claim-1"})

      first = spawn_holder()
      second = spawn_holder()

      {:ok, _device} = DeviceRegistry.attach("blockvol", first, registry)
      {:ok, _device} = DeviceRegistry.attach("blockvol", second, registry)
      assert DeviceRegistry.attached(registry) == %{"blockvol" => 2}

      :ok = DeviceRegistry.fenced("blockvol", 7, registry)

      assert_receive {:holder_notified, ^first, "blockvol", 7}, 1_000
      assert_receive {:holder_notified, ^second, "blockvol", 7}, 1_000
      assert DeviceRegistry.attached(registry) == %{}
    end

    # Released immediately rather than when the last holder notices, because
    # the point of preempting a device is that it can be attached elsewhere
    # without racing the losers' teardown.
    test "releases the attachment claim without waiting for the holders", %{registry: registry} do
      stub_core()
      stub_coordinator({:ok, "claim-1"})

      {:ok, _device} = DeviceRegistry.attach("blockvol", spawn_holder(), registry)

      :ok = DeviceRegistry.fenced("blockvol", 3, registry)

      assert_receive {:coordinator_call, :release, ["claim-1"]}, 1_000
    end

    test "emits what was fenced and how much went with it", %{registry: registry} do
      stub_core()
      stub_coordinator({:ok, "claim-1"})
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :fenced]])

      {:ok, _device} = DeviceRegistry.attach("blockvol", spawn_holder(), registry)
      :ok = DeviceRegistry.fenced("blockvol", 11, registry)

      assert_receive {[:neonfs, :block, :fenced], ^ref, measurements, %{export: "blockvol"}},
                     1_000

      assert measurements.holders == 1
      assert measurements.current_epoch == 11

      :telemetry.detach(ref)
    end

    # Two connections can discover the same fence at once, and the second
    # must not fight the first.
    test "a device already gone is not an error", %{registry: registry} do
      assert :ok = DeviceRegistry.fenced("never-attached", 1, registry)
    end

    test "the export can be attached again afterwards", %{registry: registry} do
      stub_core()
      stub_coordinator({:ok, "claim-1"})

      {:ok, _device} = DeviceRegistry.attach("blockvol", spawn_holder(), registry)
      :ok = DeviceRegistry.fenced("blockvol", 2, registry)

      assert {:ok, _device} = DeviceRegistry.attach("blockvol", spawn_holder(), registry)
      assert DeviceRegistry.attached(registry) == %{"blockvol" => 1}
    end
  end

  # A holder that reports what it was told, so the notification is asserted
  # where it lands rather than inferred from the registry's state.
  defp spawn_holder do
    test = self()

    spawn(fn ->
      receive do
        {:fenced, export, epoch} -> send(test, {:holder_notified, self(), export, epoch})
      after
        5_000 -> :timeout
      end
    end)
  end
end
