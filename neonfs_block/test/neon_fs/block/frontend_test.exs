defmodule NeonFS.Block.FrontendTest do
  @moduledoc """
  The seam itself: a frontend talks to whatever `Frontend.impl/0` names, so a
  second frontend (ublk) can answer the same callbacks against the same core.

  Driven through the NBD frontend against a stub core rather than through
  `Device`, because what is under test is that the protocol layer names no
  implementation of its own — which a test calling `Device` directly could
  not tell.
  """
  use ExUnit.Case, async: false

  alias NeonFS.Block.{DeviceRegistry, Frontend, Listener}

  @export "stub:/dev.img"
  @size 1_048_576
  @block 4096

  @request_magic 0x25609513
  @simple_reply_magic 0x67446698
  @ihaveopt 0x49_48_41_56_45_4F_50_54
  @read 0
  @write 1

  defmodule StubCore do
    @moduledoc false
    @behaviour NeonFS.Block.Frontend

    @impl true
    def open(export) do
      send(test_pid(), {:core, :open, export})

      {:ok,
       %{
         export: export,
         volume: "stub",
         path: "/dev.img",
         file_id: "stub-id",
         size: 1_048_576,
         logical_block_size: 4096,
         physical_block_size: 4096,
         read_only: false
       }}
    end

    @impl true
    def export_info(device) do
      %{
        size: device.size,
        logical_block_size: device.logical_block_size,
        physical_block_size: device.physical_block_size,
        read_only: device.read_only
      }
    end

    @impl true
    def read_stream(_device, offset, length) do
      send(test_pid(), {:core, :read_stream, offset, length})
      {:ok, [:binary.copy(<<0xC3>>, length)]}
    end

    @impl true
    def write(_device, offset, data) do
      send(test_pid(), {:core, :write, offset, byte_size(data)})
      :ok
    end

    @impl true
    def flush(_device) do
      send(test_pid(), {:core, :flush})
      :ok
    end

    @impl true
    def write_zeroes(_device, offset, length) do
      send(test_pid(), {:core, :write_zeroes, offset, length})
      :ok
    end

    @impl true
    def measure_read(_device, bytes, _start_time, status) do
      send(test_pid(), {:core, :measure_read, bytes, status})
      :ok
    end

    defp test_pid, do: Application.fetch_env!(:neonfs_block, :frontend_test_pid)
  end

  setup do
    Application.put_env(:neonfs_block, :frontend_test_pid, self())
    Application.put_env(:neonfs_block, :io_core, StubCore)

    Application.put_env(:neonfs_block, :coordinator_call_fn, fn
      :claim_path_for, _args -> {:ok, "claim"}
      :release, _args -> :ok
    end)

    on_exit(fn ->
      Application.delete_env(:neonfs_block, :io_core)
      Application.delete_env(:neonfs_block, :frontend_test_pid)
      Application.delete_env(:neonfs_block, :coordinator_call_fn)
    end)

    start_supervised!(DeviceRegistry)
    pid = start_supervised!(Listener.child_spec(port: 0))
    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)

    {:ok, port: port}
  end

  test "the NBD frontend serves a device from whatever core is configured",
       %{port: port} do
    socket = connect(port)
    assert {:ok, <<size::64, _flags::16>>} = handshake(socket)
    assert size == @size
    assert_receive {:core, :open, @export}, 2_000

    payload = :binary.copy(<<0x5A>>, @block)
    :ok = :gen_tcp.send(socket, request(@write, 1, @block, @block) <> payload)
    assert {:ok, <<@simple_reply_magic::32, 0::32, 1::64>>} = :gen_tcp.recv(socket, 16, 2_000)
    assert_receive {:core, :write, @block, @block}, 2_000

    :ok = :gen_tcp.send(socket, request(@read, 2, 0, @block))
    assert {:ok, <<@simple_reply_magic::32, 0::32, 2::64>>} = :gen_tcp.recv(socket, 16, 2_000)
    assert {:ok, data} = :gen_tcp.recv(socket, @block, 2_000)
    assert byte_size(data) == @block
    assert_receive {:core, :read_stream, 0, @block}, 2_000

    # The frontend counts what it drained, because only it knows.
    assert_receive {:core, :measure_read, @block, :ok}, 2_000
  end

  defp connect(port) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    socket
  end

  defp handshake(socket) do
    {:ok, _greeting} = :gen_tcp.recv(socket, 18, 2_000)
    :ok = :gen_tcp.send(socket, <<1::32>>)
    :ok = :gen_tcp.send(socket, <<@ihaveopt::64, 1::32, byte_size(@export)::32, @export::binary>>)

    with {:ok, <<head::binary-size(10), _zeroes::binary>>} <- :gen_tcp.recv(socket, 134, 2_000) do
      {:ok, head}
    end
  end

  defp request(type, cookie, offset, length) do
    <<@request_magic::32, 0::16, type::16, cookie::64, offset::64, length::32>>
  end
end
