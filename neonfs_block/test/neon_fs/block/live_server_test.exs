defmodule NeonFS.Block.LiveServerTest do
  @moduledoc """
  Drives a live `NeonFS.Block.Listener` with an in-Elixir NBD client.

  The client hand-encodes its requests rather than reusing a helper, because
  the codec only decodes them — so what the server parses here is the same
  bytes `nbd-client` puts on the wire, not a round trip through one encoder.

  The cluster behind it is stubbed, deliberately rather than as a shortcut:
  the promise most worth asserting is that a flush is not acknowledged before
  the backing store's flush returns, and proving an ordering needs a callee
  the test can hold open. Kernel attachment against a real cluster is the
  rig's job.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Block.{DeviceRegistry, Listener}

  @block 4096
  @chunk 131_072
  @size 16 * @block
  @export "vol:/dev.img"

  @request_magic 0x25609513
  @simple_reply_magic 0x67446698
  @ihaveopt 0x49_48_41_56_45_4F_50_54

  @read 0
  @write 1
  @disconnect 2
  @flush 3
  @trim 4
  @write_zeroes 6

  setup do
    test = self()

    stub_core(fn _module, function, _args ->
      case function do
        :open_device -> open_device_reply()
        :flush -> hold_flush(test)
        :write -> write_reply()
        :write_zeroes -> write_zeroes_reply()
        _other -> :ok
      end
    end)

    Application.put_env(:neonfs_block, :read_stream_fn, fn _device, opts ->
      send(test, {:read_opts, opts})
      length = Keyword.fetch!(opts, :length)

      # Two elements, so a read that forwards only its first chunk fails here.
      half = div(length, 2)
      {:ok, %{stream: [:binary.copy(<<0xA5>>, half), :binary.copy(<<0x5A>>, length - half)]}}
    end)

    on_exit(fn ->
      Application.delete_env(:neonfs_block, :core_call_fn)
      Application.delete_env(:neonfs_block, :read_stream_fn)
    end)

    start_supervised!(DeviceRegistry)
    pid = start_supervised!(Listener.child_spec(port: 0))

    {:ok, port: listener_port(pid)}
  end

  describe "handshake" do
    test "completes through NBD_OPT_EXPORT_NAME and advertises the size", %{port: port} do
      socket = connect(port)

      assert {:ok, <<size::64, _flags::16>>} = handshake(socket, @export)
      assert size == @size
    end

    test "refuses a client that does not speak fixed newstyle", %{port: port} do
      socket = connect(port)
      assert {:ok, _greeting} = :gen_tcp.recv(socket, 18, 2_000)

      :ok = :gen_tcp.send(socket, <<0::32>>)

      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2_000)
    end

    test "declines structured replies and keeps the connection", %{port: port} do
      socket = connect(port)
      assert {:ok, _greeting} = :gen_tcp.recv(socket, 18, 2_000)
      :ok = :gen_tcp.send(socket, <<1::32>>)

      # NBD_OPT_STRUCTURED_REPLY, which this server declines by design — the
      # protocol requires a client to tolerate the refusal.
      :ok = :gen_tcp.send(socket, <<@ihaveopt::64, 8::32, 0::32>>)

      assert {:ok, <<_magic::64, 8::32, 0x80000001::32, 0::32>>} =
               :gen_tcp.recv(socket, 20, 2_000)

      assert {:ok, <<size::64, _flags::16>>} = export_name(socket, @export)
      assert size == @size
    end

    test "NBD_OPT_GO selects the export and enters transmission", %{port: port} do
      socket = connect(port)
      assert {:ok, _greeting} = :gen_tcp.recv(socket, 18, 2_000)
      :ok = :gen_tcp.send(socket, <<1::32>>)

      name = @export
      payload = <<byte_size(name)::32, name::binary, 0::16>>
      :ok = :gen_tcp.send(socket, <<@ihaveopt::64, 7::32, byte_size(payload)::32>> <> payload)

      # NBD_REP_INFO(export), NBD_REP_INFO(block size), NBD_REP_ACK.
      assert {:ok, <<_::64, 7::32, 3::32, 12::32, 0::16, size::64, _flags::16>>} =
               :gen_tcp.recv(socket, 20 + 12, 2_000)

      assert size == @size

      assert {:ok, <<_::64, 7::32, 3::32, 14::32, _info::binary-size(14)>>} =
               :gen_tcp.recv(socket, 20 + 14, 2_000)

      assert {:ok, <<_::64, 7::32, 1::32, 0::32>>} = :gen_tcp.recv(socket, 20, 2_000)

      :ok = :gen_tcp.send(socket, request(@read, 1, 0, @block))

      assert {:ok, <<@simple_reply_magic::32, 0::32, 1::64>>} = :gen_tcp.recv(socket, 16, 2_000)
      assert {:ok, data} = :gen_tcp.recv(socket, @block, 2_000)
      assert byte_size(data) == @block
    end

    test "answers an option it cannot parse by its numeric code", %{port: port} do
      socket = connect(port)
      assert {:ok, _greeting} = :gen_tcp.recv(socket, 18, 2_000)
      :ok = :gen_tcp.send(socket, <<1::32>>)

      # NBD_OPT_GO with no payload: recognised code, unparseable body, so the
      # codec hands back the code and the reply has to be addressed by it.
      :ok = :gen_tcp.send(socket, <<@ihaveopt::64, 7::32, 0::32>>)

      assert {:ok, <<_magic::64, 7::32, 0x80000001::32, 0::32>>} =
               :gen_tcp.recv(socket, 20, 2_000)

      assert {:ok, <<size::64, _flags::16>>} = export_name(socket, @export)
      assert size == @size
    end
  end

  describe "transmission" do
    test "a read streams every chunk of its range", %{port: port} do
      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      :ok = :gen_tcp.send(socket, request(@read, 0, 0, 2 * @block))

      assert {:ok, <<@simple_reply_magic::32, 0::32, 0::64>>} = :gen_tcp.recv(socket, 16, 2_000)
      assert {:ok, payload} = :gen_tcp.recv(socket, 2 * @block, 2_000)

      assert byte_size(payload) == 2 * @block
      assert :binary.part(payload, 0, 1) == <<0xA5>>
      assert :binary.part(payload, 2 * @block - 1, 1) == <<0x5A>>
    end

    test "a write reaches the backing store and is acknowledged", %{port: port} do
      test = self()
      stub_core(fn _module, function, args -> record(test, function, args) end)

      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      payload = :binary.copy(<<0xEF>>, @block)
      :ok = :gen_tcp.send(socket, request(@write, 1, @block, @block) <> payload)

      assert_receive {:core_call, :write, ["vol", "file-id", 4096, ^payload]}, 2_000
      assert {:ok, <<@simple_reply_magic::32, 0::32, 1::64>>} = :gen_tcp.recv(socket, 16, 2_000)
    end

    test "a write's command event carries the chunk bytes core charged for it", %{port: port} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :command]])

      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      :ok = :gen_tcp.send(socket, request(@write, 1, 0, @block) <> :binary.copy(<<0xEF>>, @block))

      assert_receive {[:neonfs, :block, :command], ^ref, measurements, %{command: :write}}, 2_000

      # Both halves of the ratio on one event: the guest asked for a block,
      # the chunk layer moved a whole chunk to store it.
      assert measurements.bytes == @block
      assert measurements.chunk_bytes == @chunk

      :telemetry.detach(ref)
    end

    test "a zero-fill's command event carries what it replaced as well as what it wrote",
         %{port: port} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :command]])

      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      :ok = :gen_tcp.send(socket, request(@trim, 1, 0, @size))

      assert_receive {[:neonfs, :block, :command], ^ref, measurements, %{command: :write_zeroes}},
                     2_000

      # The guest bytes and the chunk-layer bytes are unrelated for a
      # zero-fill, and neither describes it alone — what it cost is the
      # entries it replaced.
      assert measurements.bytes == @size
      assert measurements.chunk_bytes == @chunk
      assert measurements.chunks_replaced == 3

      :telemetry.detach(ref)
    end

    test "a read tags its chunk fetches with the export they served", %{port: port} do
      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      :ok = :gen_tcp.send(socket, request(@read, 1, 0, @block))
      assert {:ok, _reply} = :gen_tcp.recv(socket, 16 + @block, 2_000)

      # A read's chunk bytes are only known inside `ChunkReader`, so the
      # export travels with the request to come back on its telemetry.
      # Without it the block exporter cannot separate one device from
      # another, or its own reads from a co-located FUSE mount's.
      assert_receive {:read_opts, opts}, 2_000
      assert Keyword.fetch!(opts, :telemetry_metadata) == %{export: @export}
    end

    test "a write split across TCP segments is not acted on until it is whole", %{port: port} do
      test = self()
      stub_core(fn _module, function, args -> record(test, function, args) end)

      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      payload = :binary.copy(<<0xEF>>, @block)
      <<head::binary-size(100), tail::binary>> = request(@write, 7, 0, @block) <> payload

      :ok = :gen_tcp.send(socket, head)
      refute_receive {:core_call, :write, _args}, 200

      :ok = :gen_tcp.send(socket, tail)
      assert_receive {:core_call, :write, ["vol", "file-id", 0, ^payload]}, 2_000
    end

    test "a flush is not acknowledged before the backing flush returns", %{port: port} do
      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      :ok = :gen_tcp.send(socket, request(@flush, 2, 0, 0))

      # The stub hands back the process it is blocking, so the release is
      # aimed at the connection rather than guessed at.
      assert_receive {:flush_waiting, waiting}, 2_000

      # This is the assertion the test exists for: replying here would tell a
      # guest its journal is durable before it is.
      assert {:error, :timeout} = :gen_tcp.recv(socket, 16, 300)

      send(waiting, :release_flush)

      assert {:ok, <<@simple_reply_magic::32, 0::32, 2::64>>} = :gen_tcp.recv(socket, 16, 2_000)
    end

    test "TRIM and WRITE ZEROES both zero-fill the range", %{port: port} do
      test = self()
      stub_core(fn _module, function, args -> record(test, function, args) end)

      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      :ok = :gen_tcp.send(socket, request(@trim, 3, @block, 2 * @block))
      assert_receive {:core_call, :write_zeroes, ["vol", "file-id", 4096, 8192]}, 2_000
      assert {:ok, <<_magic::32, 0::32, 3::64>>} = :gen_tcp.recv(socket, 16, 2_000)

      :ok = :gen_tcp.send(socket, request(@write_zeroes, 4, 0, @block))
      assert_receive {:core_call, :write_zeroes, ["vol", "file-id", 0, 4096]}, 2_000
      assert {:ok, <<_magic::32, 0::32, 4::64>>} = :gen_tcp.recv(socket, 16, 2_000)
    end

    test "a refused command answers with an error rather than disconnecting", %{port: port} do
      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      stub_core(fn _module, _function, _args -> {:error, {:out_of_range, 0, 0, @size}} end)

      :ok = :gen_tcp.send(socket, request(@write, 5, 0, @block) <> :binary.copy(<<1>>, @block))

      # EINVAL: an out-of-range request is the client's mistake, not the
      # server failing to serve a valid one.
      assert {:ok, <<_magic::32, 22::32, 5::64>>} = :gen_tcp.recv(socket, 16, 2_000)
    end

    test "a disconnect closes the connection", %{port: port} do
      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      :ok = :gen_tcp.send(socket, request(@disconnect, 6, 0, 0))

      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2_000)
    end
  end

  describe "attach lifecycle" do
    test "two connections share one device, released on the last close", %{port: port} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :block, :attached],
          [:neonfs, :block, :detached]
        ])

      first = connect(port)
      {:ok, _export} = handshake(first, @export)
      assert_receive {[:neonfs, :block, :attached], ^ref, %{holders: 1}, _meta}, 2_000

      second = connect(port)
      {:ok, _export} = handshake(second, @export)
      assert_receive {[:neonfs, :block, :attached], ^ref, %{holders: 2}, _meta}, 2_000

      :ok = :gen_tcp.close(first)
      assert_receive {[:neonfs, :block, :detached], ^ref, %{holders: 1}, _meta}, 2_000
      assert DeviceRegistry.attached() == %{@export => 1}

      :ok = :gen_tcp.close(second)
      assert_receive {[:neonfs, :block, :detached], ^ref, %{holders: 0}, _meta}, 2_000
      assert DeviceRegistry.attached() == %{}

      :telemetry.detach(ref)
    end

    test "an export that does not resolve closes rather than serving", %{port: port} do
      stub_core(fn _module, _function, _args -> {:error, :not_found} end)

      socket = connect(port)
      assert {:ok, _greeting} = :gen_tcp.recv(socket, 18, 2_000)
      :ok = :gen_tcp.send(socket, <<1::32>>)
      :ok = :gen_tcp.send(socket, export_name_option("vol:/missing.img"))

      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2_000)
    end
  end

  defp stub_core(fun) do
    Application.put_env(:neonfs_block, :core_call_fn, fun)
  end

  defp record(test, function, args) do
    send(test, {:core_call, function, args})

    case function do
      :open_device -> open_device_reply()
      :write -> write_reply()
      :write_zeroes -> write_zeroes_reply()
      _other -> :ok
    end
  end

  # A 4 KiB guest write costs a whole chunk rewrite; `BlockBacking.write/5`
  # does that arithmetic on core and returns it, because the chunk geometry
  # is not known here.
  defp write_reply, do: {:ok, %{chunk_bytes: @chunk, chunks_rewritten: 1}}

  # A zero-fill's two costs are unrelated numbers: the one chunk it clipped
  # was rewritten, and the chunks it covered were replaced by hash.
  defp write_zeroes_reply,
    do: {:ok, %{chunk_bytes: @chunk, chunks_rewritten: 1, chunks_replaced: 3}}

  defp open_device_reply do
    {:ok,
     %{
       file_id: "file-id",
       size: @size,
       logical_block_bytes: @block,
       physical_block_bytes: @block
     }}
  end

  defp hold_flush(test) do
    send(test, {:flush_waiting, self()})

    receive do
      :release_flush -> :ok
    after
      5_000 -> {:error, :flush_stub_never_released}
    end
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
    export_name(socket, export)
  end

  # NBD_OPT_EXPORT_NAME's reply is 8 bytes of size, 2 of flags, then 124
  # zeroes, since this client does not ask for NBD_FLAG_NO_ZEROES.
  defp export_name(socket, export) do
    :ok = :gen_tcp.send(socket, export_name_option(export))

    with {:ok, <<head::binary-size(10), _zeroes::binary>>} <- :gen_tcp.recv(socket, 134, 2_000) do
      {:ok, head}
    end
  end

  defp export_name_option(export) do
    <<@ihaveopt::64, 1::32, byte_size(export)::32, export::binary>>
  end

  defp request(type, cookie, offset, length) do
    <<@request_magic::32, 0::16, type::16, cookie::64, offset::64, length::32>>
  end

  defp listener_port(pid) do
    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)
    port
  end
end
