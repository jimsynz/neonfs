defmodule NeonFS.Block.Ublk.QueueTest do
  @moduledoc """
  The ublk queue loop against a real Unix socket and a stub core.

  No ublk driver is involved: what the helper contributes is bytes on a
  socket, and a test can produce those. That is the point of the split — the
  half that needs a kernel feature is the half with no logic in it, so
  everything decided on the BEAM side is testable on a host with no ublk at
  all, which is every host this suite runs on.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Block.StubCore
  alias NeonFS.Block.Ublk.{Protocol, Queue}

  @version 1
  @block 4096

  defmodule FailingCore do
    @moduledoc false
    @behaviour NeonFS.Block.Frontend

    defdelegate open(export), to: StubCore
    defdelegate export_info(device), to: StubCore
    defdelegate measure_read(device, bytes, start_time, status), to: StubCore

    @impl true
    def read_stream(_device, _offset, _length), do: {:error, :stale_chunks}
    @impl true
    def write(_device, _offset, _data), do: {:error, :stale_chunks}
    @impl true
    def flush(_device), do: {:error, :stale_chunks}
    @impl true
    def write_zeroes(_device, _offset, _length), do: {:error, :stale_chunks}
  end

  setup do
    StubCore.report_to(self())
    Application.put_env(:neonfs_block, :io_core, StubCore)

    on_exit(fn ->
      Application.delete_env(:neonfs_block, :io_core)
      Application.delete_env(:neonfs_block, :stub_core_test_pid)
      Application.delete_env(:neonfs_block, :stub_core_read_only)
    end)

    :ok
  end

  test "a read is answered with the core's bytes, and measured" do
    socket = connected_queue()

    assert %{status: 0, tag: 5, data: data} = exchange(socket, :read, 5, 8192, @block)
    assert data == :binary.copy(<<0xC3>>, @block)

    assert_receive {:core, :read_stream, 8192, @block}
    assert_receive {:core, :measure_read, @block, :ok}
  end

  test "a write hands its payload to the core" do
    socket = connected_queue()
    payload = :binary.copy(<<0xD4>>, @block)

    assert %{status: 0, tag: 6, data: <<>>} = exchange(socket, :write, 6, 0, @block, payload)

    assert_receive {:core, :write, 0, @block}
  end

  test "a flush is a barrier the core is asked for" do
    socket = connected_queue()

    assert %{status: 0, tag: 7} = exchange(socket, :flush, 7, 0, 0)

    assert_receive {:core, :flush}
  end

  test "discard and write zeroes both land as a zero fill" do
    socket = connected_queue()

    assert %{status: 0} = exchange(socket, :discard, 8, 0, @block)
    assert_receive {:core, :write_zeroes, 0, @block}

    assert %{status: 0} = exchange(socket, :write_zeroes, 9, @block, @block)
    assert_receive {:core, :write_zeroes, @block, @block}
  end

  test "a core failure becomes the errno the kernel will report" do
    socket = connected_queue()

    Application.put_env(:neonfs_block, :io_core, FailingCore)

    assert %{status: 11, tag: 10, data: <<>>} =
             exchange(socket, :write, 10, 0, @block, :binary.copy(<<0>>, @block))
  end

  # ublk has no negotiation in which a guest agrees to a restriction, so a
  # read-only export that merely advertises itself would still land writes.
  test "a read-only export refuses writes without reaching the core" do
    Application.put_env(:neonfs_block, :stub_core_read_only, true)
    socket = connected_queue()

    assert %{status: 1, tag: 11} =
             exchange(socket, :write, 11, 0, @block, :binary.copy(<<0>>, @block))

    refute_receive {:core, :write, _offset, _bytes}

    assert %{status: 1} = exchange(socket, :discard, 12, 0, @block)
    refute_receive {:core, :write_zeroes, _offset, _length}
  end

  test "a frame it cannot read is refused rather than guessed at" do
    socket = connected_queue()

    send_frame(socket, <<@version + 1::8, 0::8, 3::16, 0::64, 512::32>>)

    assert %{status: 5, tag: 0} = recv_reply(socket)
    refute_receive {:core, :read_stream, _offset, _length}
  end

  test "the queue stops when the helper's socket closes" do
    {socket, server} = connected_queue(:with_server)
    monitor = Process.monitor(server)

    :ok = :gen_tcp.close(socket)

    assert_receive {:DOWN, ^monitor, :process, ^server, :normal}, 5_000
  end

  # ─── The helper's side of the socket ───────────────────────────────────

  defp connected_queue(shape \\ :socket) do
    {:ok, device} = StubCore.open("stub:/dev.img")
    info = StubCore.export_info(device)

    path = Path.join(System.tmp_dir!(), "ublk-test-#{System.unique_integer([:positive])}")

    {:ok, listener} =
      :gen_tcp.listen(0, [{:ifaddr, {:local, path}}, :binary, packet: :raw, active: false])

    test = self()
    server = spawn(fn -> Queue.serve(device, info, 0, listener) end)

    {:ok, socket} =
      :gen_tcp.connect({:local, path}, 0, [:binary, packet: :raw, active: false], 5_000)

    on_exit(fn ->
      :gen_tcp.close(listener)
      File.rm(path)
      send(test, :cleaned)
    end)

    case shape do
      :socket -> socket
      :with_server -> {socket, server}
    end
  end

  defp exchange(socket, op, tag, offset, length, payload \\ <<>>) do
    frame =
      <<@version::8, Protocol.op_code(op)::8, tag::16, offset::64, length::32>> <> payload

    send_frame(socket, frame)
    recv_reply(socket)
  end

  defp send_frame(socket, frame) do
    :ok = :gen_tcp.send(socket, <<byte_size(frame)::32>>)
    :ok = :gen_tcp.send(socket, frame)
  end

  # Reads exactly what the prefix names, which is what proves a streamed read
  # announced its own length correctly: a frame that overstates it would hang
  # here rather than arriving short.
  defp recv_reply(socket) do
    {:ok, <<length::32>>} = :gen_tcp.recv(socket, 4, 5_000)

    {:ok, <<@version::8, status::8, tag::16, declared::32, data::binary>>} =
      :gen_tcp.recv(socket, length, 5_000)

    assert byte_size(data) == declared
    %{status: status, tag: tag, data: data}
  end
end
