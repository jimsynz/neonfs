defmodule NeonFS.Block.SupervisorTest do
  @moduledoc """
  The supervision property the attachment claim depends on.

  `NeonFS.Block.DeviceRegistry` holds each attached device's exclusive claim,
  and the coordinator releases every claim whose holder dies. So a registry
  that dies while its connections keep serving would leave live NBD traffic
  against a device the cluster believes nobody has attached — which is the
  state the claim exists to make impossible.
  """

  use ExUnit.Case, async: false

  # Killing a supervised listener brutally is the point of these tests, and
  # ThousandIsland's own children report their deaths at :error level.
  @moduletag :capture_log

  alias NeonFS.Block.DeviceRegistry

  @ihaveopt 0x49_48_41_56_45_4F_50_54
  @export "vol:/dev.img"

  setup do
    Application.put_env(:neonfs_block, :core_call_fn, fn _module, function, _args ->
      case function do
        :open_device ->
          {:ok,
           %{
             id: "device",
             chunk_bytes: 4096,
             epoch: 0,
             size: 65_536,
             logical_block_bytes: 4096,
             physical_block_bytes: 4096
           }}

        _other ->
          :ok
      end
    end)

    Application.put_env(:neonfs_block, :coordinator_call_fn, fn
      :claim_path_for, _args -> {:ok, "claim"}
      :release, _args -> :ok
    end)

    on_exit(fn ->
      Application.delete_env(:neonfs_block, :core_call_fn)
      Application.delete_env(:neonfs_block, :coordinator_call_fn)
    end)

    supervisor = start_supervised!({NeonFS.Block.Supervisor, port: 0})
    {:ok, supervisor: supervisor}
  end

  test "a registry that dies takes the attached connections with it", %{supervisor: supervisor} do
    socket = connect(listener_port(supervisor))
    assert {:ok, _export_info} = handshake(socket, @export)
    assert DeviceRegistry.attached() == %{@export => 1}

    registry = Process.whereis(DeviceRegistry)
    ref = Process.monitor(registry)
    Process.exit(registry, :kill)
    assert_receive {:DOWN, ^ref, :process, ^registry, :killed}, 1_000

    # The claim went with the registry, so the connection must not outlive it.
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2_000)
  end

  test "the listener comes back, so the next attach is served", %{supervisor: supervisor} do
    first = connect(listener_port(supervisor))
    assert {:ok, _export_info} = handshake(first, @export)

    registry = Process.whereis(DeviceRegistry)
    ref = Process.monitor(registry)
    Process.exit(registry, :kill)
    assert_receive {:DOWN, ^ref, :process, ^registry, :killed}, 1_000

    wait_for_restart(supervisor)

    second = connect(listener_port(supervisor))
    assert {:ok, _export_info} = handshake(second, @export)
    assert DeviceRegistry.attached() == %{@export => 1}
  end

  # A listener that dies needs no help from the strategy: its connections die
  # with it and the registry's own monitors release their claims.
  test "a listener that dies releases its holders", %{supervisor: supervisor} do
    socket = connect(listener_port(supervisor))
    assert {:ok, _export_info} = handshake(socket, @export)

    # Subscribe before the kill: the release is what is being waited for, so
    # attaching afterwards is a race against the thing under test.
    telemetry =
      :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :detached]])

    listener = listener_pid(supervisor)
    ref = Process.monitor(listener)
    Process.exit(listener, :kill)
    assert_receive {:DOWN, ^ref, :process, ^listener, :killed}, 1_000

    # Waiting for the listener to come back was only ever a proxy for the
    # release, and a poor one: the registry releases when its own monitor
    # fires, which is ordered against the listener's death but not against the
    # restart. So wait for the release itself. `holders: 0` is the registry
    # reporting the device released rather than merely one holder dropped.
    assert_receive {[:neonfs, :block, :detached], ^telemetry, %{holders: 0}, %{export: @export}},
                   5_000

    assert DeviceRegistry.attached() == %{}
  end

  # The one wait that has to poll: a `Supervisor` restarting a child emits no
  # event to subscribe to, and `:sys.get_state/1` on the supervisor does not
  # help — the EXIT it will act on and this test's `:DOWN` are separate signals
  # from the same death, so a state call can be served before the EXIT arrives.
  #
  # What was wrong before was the budget, not the polling: 100 attempts of a
  # `GenServer.call` is a timeout of under a millisecond, while the teardown of
  # a listener with 100 acceptors takes milliseconds. This bounds by a deadline
  # and sleeps between attempts so the budget means what it says. The sleep is a
  # poll interval, not a synchronisation delay — nothing here assumes the
  # restart finishes within one.
  @restart_timeout_ms 5_000
  @restart_poll_ms 10

  defp wait_for_restart(supervisor) do
    wait_for_restart(supervisor, System.monotonic_time(:millisecond) + @restart_timeout_ms)
  end

  defp wait_for_restart(supervisor, deadline) do
    case listener_port_if_up(supervisor) do
      {:ok, _port} ->
        :ok

      :not_yet ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("listener did not come back within #{@restart_timeout_ms}ms")
        end

        Process.sleep(@restart_poll_ms)
        wait_for_restart(supervisor, deadline)
    end
  end

  defp listener_port_if_up(supervisor) do
    case ThousandIsland.listener_info(listener_pid(supervisor)) do
      {:ok, {_ip, port}} when port > 0 -> {:ok, port}
      _not_yet -> :not_yet
    end
  catch
    :exit, _reason -> :not_yet
  end

  defp listener_pid(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {_id, pid, _type, [ThousandIsland]} when is_pid(pid) -> pid
      _other -> nil
    end)
  end

  defp listener_port(supervisor) do
    {:ok, {_ip, port}} = ThousandIsland.listener_info(listener_pid(supervisor))
    port
  end

  defp connect(port) do
    {:ok, socket} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw], 2_000)

    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp handshake(socket, export) do
    {:ok, _greeting} = :gen_tcp.recv(socket, 18, 2_000)
    :ok = :gen_tcp.send(socket, <<1::32>>)
    :ok = :gen_tcp.send(socket, <<@ihaveopt::64, 1::32, byte_size(export)::32, export::binary>>)

    with {:ok, <<head::binary-size(10), _zeroes::binary>>} <- :gen_tcp.recv(socket, 134, 2_000) do
      {:ok, head}
    end
  end
end
