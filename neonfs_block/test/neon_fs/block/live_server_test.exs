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
  alias NeonFS.Error.PermissionDenied

  @block 4096
  @chunk 4 * @block
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

    stub_core(fn _module, function, args ->
      case function do
        :open_device -> open_device_reply()
        :flush -> hold_flush(test)
        :read_refs -> read_refs_reply(args)
        :commit_written -> commit_reply()
        _other -> :ok
      end
    end)

    stub_data_plane()

    Application.put_env(:neonfs_block, :read_stream_fn, fn _device, opts ->
      send(test, {:read_opts, opts})
      length = Keyword.fetch!(opts, :length)

      # Two elements, so a read that forwards only its first chunk fails here.
      half = div(length, 2)
      {:ok, %{stream: [:binary.copy(<<0xA5>>, half), :binary.copy(<<0x5A>>, length - half)]}}
    end)

    Application.put_env(:neonfs_block, :coordinator_call_fn, fn
      :claim_path_for, _args -> {:ok, "claim"}
      :release, _args -> :ok
    end)

    on_exit(fn ->
      Application.delete_env(:neonfs_block, :core_call_fn)
      Application.delete_env(:neonfs_block, :read_stream_fn)
      Application.delete_env(:neonfs_block, :coordinator_call_fn)
      Application.delete_env(:neonfs_block, :write_chunks_fn)
      Application.delete_env(:neonfs_block, :fetch_chunk_fn)
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

      # The bytes went over the data plane; what reaches core is the map,
      # naming the extent the write landed in and the target its read saw.
      assert_receive {:core_call, :commit_written, ["vol", "/dev.img", [{0, hash}], opts]}, 2_000

      assert is_binary(hash)
      assert Keyword.fetch!(opts, :epoch) == 0
      assert Keyword.fetch!(opts, :expect) == [{0, :hole}]

      assert {:ok, <<@simple_reply_magic::32, 0::32, 1::64>>} = :gen_tcp.recv(socket, 16, 2_000)
    end

    # A buffered write has moved nothing yet, so its amplification is the
    # drain's to report — and the drain is where the coalescing shows up.
    test "the chunk cost of writes is charged to the drain, not to each write", %{port: port} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :block, :command],
          [:neonfs, :block, :window_drain]
        ])

      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      # Four writes into one extent, then a flush to land them.
      for {cookie, offset} <- Enum.with_index(0..(3 * @block)//@block, 1) do
        :ok =
          :gen_tcp.send(
            socket,
            request(@write, cookie, offset, @block) <> :binary.copy(<<7>>, @block)
          )

        assert {:ok, <<_magic::32, 0::32, _cookie::64>>} = :gen_tcp.recv(socket, 16, 2_000)
      end

      assert_receive {[:neonfs, :block, :command], ^ref, %{bytes: @block} = write_measurements,
                      %{command: :write}},
                     2_000

      refute Map.has_key?(write_measurements, :chunk_bytes)

      :ok = :gen_tcp.send(socket, request(@flush, 9, 0, 0))

      assert_receive {[:neonfs, :block, :window_drain], ^ref, drain, %{reason: :flush}}, 5_000

      # The whole point: four guest writes, one extent, one commit.
      assert drain.writes == 4
      assert drain.extents == 1
      assert drain.chunk_bytes == @chunk

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
      # zero-fill, and neither describes it alone — a whole-device TRIM
      # writes nothing at all and costs the entries it dropped.
      assert measurements.bytes == @size
      assert measurements.chunk_bytes == 0
      assert measurements.chunks_replaced == div(@size, @chunk)

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

    # `NBD_FLAG_CAN_MULTI_CONN` tells a client several sockets to one export
    # are safe. The flag is a promise, and a promise nothing checks is how it
    # stops being kept — so this asserts the half that is decidable here: a
    # flush issued on one connection covers a write made on another. That the
    # two share one window, which is what makes cross-connection reads
    # coherent too, is asserted in `DeviceRegistryTest`; the read path is
    # stubbed here and never reaches the window overlay.
    test "a flush on one connection covers a write made on another", %{port: port} do
      test = self()
      stub_core(fn _module, function, args -> record(test, function, args) end)

      a = connect(port)
      {:ok, _export} = handshake(a, @export)
      b = connect(port)
      {:ok, _export} = handshake(b, @export)

      :ok = :gen_tcp.send(a, request(@write, 1, 0, @block) <> :binary.copy(<<0xC3>>, @block))
      assert {:ok, <<@simple_reply_magic::32, 0::32, 1::64>>} = :gen_tcp.recv(a, 16, 2_000)

      # A guest filesystem's journal depends on this: it may issue the flush
      # on whichever queue is free, not the one that carried the write.
      :ok = :gen_tcp.send(b, request(@flush, 2, 0, 0))
      assert {:ok, <<@simple_reply_magic::32, 0::32, 2::64>>} = :gen_tcp.recv(b, 16, 5_000)

      assert_receive {:core_call, :commit_written, ["vol", "/dev.img", _extents, _opts]}, 5_000
    end

    test "a write split across TCP segments is not acted on until it is whole", %{port: port} do
      test = self()
      stub_core(fn _module, function, args -> record(test, function, args) end)

      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      payload = :binary.copy(<<0xEF>>, @block)
      <<head::binary-size(100), tail::binary>> = request(@write, 7, 0, @block) <> payload

      :ok = :gen_tcp.send(socket, head)
      refute_receive {:core_call, :commit_written, _args}, 200

      :ok = :gen_tcp.send(socket, tail)

      assert_receive {:core_call, :commit_written, ["vol", "/dev.img", [{0, _hash}], _opts]},
                     2_000
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

      # Neither range covers an extent end to end, so both are read-modify
      # -written rather than punched — the map still moves, and both commands
      # arrive at the same place.
      :ok = :gen_tcp.send(socket, request(@trim, 3, @block, 2 * @block))

      assert_receive {:core_call, :commit_written, ["vol", "/dev.img", [{0, _}], _opts]}, 2_000
      assert {:ok, <<_magic::32, 0::32, 3::64>>} = :gen_tcp.recv(socket, 16, 2_000)

      :ok = :gen_tcp.send(socket, request(@write_zeroes, 4, 0, @block))

      assert_receive {:core_call, :commit_written, ["vol", "/dev.img", [{0, _}], _opts]}, 2_000
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

    # A denial is an answer, not a device fault. `EIO` — what an unmapped
    # reason gets — reads to a guest as "the disk is broken", and ext4
    # typically remounts read-only over one.
    test "a core authorisation refusal answers EPERM, not EIO", %{port: port} do
      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      stub_core(fn _module, _function, _args ->
        {:error, PermissionDenied.exception(operation: :write, uid: 1000)}
      end)

      :ok = :gen_tcp.send(socket, request(@write, 7, 0, @block) <> :binary.copy(<<1>>, @block))

      assert {:ok, <<_magic::32, 1::32, 7::64>>} = :gen_tcp.recv(socket, 16, 2_000)
    end

    # NBD has no "retry this" status, so a contended commit that core gave up
    # on has to be retried by the only party that still owns it — which is
    # now the write window, since that is where the commit happens.
    test "a contended commit is retried, not failed", %{port: port} do
      test = self()
      {:ok, attempts} = Agent.start_link(fn -> 0 end)
      ref = make_ref()

      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      stub_core(fn _module, function, args ->
        case function do
          :commit_written ->
            case Agent.get_and_update(attempts, &{&1, &1 + 1}) do
              n when n < 2 ->
                send(test, {:stale, n})
                {:error, :stale_chunks}

              _settled ->
                commit_reply()
            end

          other ->
            default_reply(other, args)
        end
      end)

      :ok = :gen_tcp.send(socket, request(@write, 8, 0, @block) <> :binary.copy(<<2>>, @block))
      assert {:ok, <<_magic::32, 0::32, 8::64>>} = :gen_tcp.recv(socket, 16, 2_000)

      :ok = :gen_tcp.send(socket, request(@flush, 14, 0, 0))

      assert_receive {:stale, 0}, 5_000
      assert_receive {:stale, 1}, 5_000

      # The flush lands once the contention clears, which is the whole point
      # of retrying rather than failing.
      assert {:ok, <<_magic::32, 0::32, 14::64>>} = :gen_tcp.recv(socket, 16, 5_000)

      :telemetry.detach(ref)
    end

    # A fenced holder has been preempted: its epoch is behind the device's, so
    # it can no longer write — and a connection that keeps answering reads out
    # of a map it cannot write is the worse half, because it looks healthy.
    test "a fenced write ends the connection rather than reporting a fault", %{port: port} do
      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      stub_core(fn _module, function, args ->
        if function == :commit_written,
          do: {:error, {:fenced, 9}},
          else: default_reply(function, args)
      end)

      # The write buffers and is acknowledged; the fence surfaces at the
      # flush, which is where durability was promised in the first place.
      :ok = :gen_tcp.send(socket, request(@write, 12, 0, @block) <> :binary.copy(<<4>>, @block))
      assert {:ok, <<_magic::32, 0::32, 12::64>>} = :gen_tcp.recv(socket, 16, 2_000)

      :ok = :gen_tcp.send(socket, request(@flush, 13, 0, 0))

      # ESHUTDOWN, not EIO: the disk is not broken, this server has stopped
      # serving it — and a guest ext4 remounts read-only over EIO rather than
      # letting the attach be retaken elsewhere.
      assert {:ok, <<_magic::32, 108::32, 13::64>>} = :gen_tcp.recv(socket, 16, 2_000)
      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2_000)
    end

    # EAGAIN past the budget: honest about what happened, and the caveat that
    # a client may not act on it is why the retry above exists at all.
    test "a span contended past the retry budget answers EAGAIN", %{port: port} do
      Application.put_env(:neonfs_block, :stale_write_retries, 1)
      Application.put_env(:neonfs_block, :stale_write_backoff_ms, 1)

      on_exit(fn ->
        Application.delete_env(:neonfs_block, :stale_write_retries)
        Application.delete_env(:neonfs_block, :stale_write_backoff_ms)
      end)

      socket = connect(port)
      {:ok, _export} = handshake(socket, @export)

      stub_core(fn _module, function, args ->
        if function == :commit_written,
          do: {:error, :stale_chunks},
          else: default_reply(function, args)
      end)

      :ok = :gen_tcp.send(socket, request(@write, 9, 0, @block) <> :binary.copy(<<3>>, @block))
      assert {:ok, <<_magic::32, 0::32, 9::64>>} = :gen_tcp.recv(socket, 16, 2_000)

      # The window retries its own drain, so what reaches the guest is the
      # flush that could not land — with EAGAIN, since nothing was lost.
      :ok = :gen_tcp.send(socket, request(@flush, 10, 0, 0))
      assert {:ok, <<_magic::32, 11::32, 10::64>>} = :gen_tcp.recv(socket, 16, 2_000)
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
    default_reply(function, args)
  end

  defp default_reply(:open_device, _args), do: open_device_reply()
  defp default_reply(:read_refs, args), do: read_refs_reply(args)
  defp default_reply(:commit_written, _args), do: commit_reply()
  defp default_reply(_other, _args), do: :ok

  # The device's extents, as core would describe them for a range: every one
  # a hole, which is what a device nothing has written to looks like and what
  # keeps the stub from having to model stored chunks.
  defp read_refs_reply([_volume, _path, offset, length]) do
    first = div(offset, @chunk)
    last = div(offset + length - 1, @chunk)

    extents =
      Enum.map(first..last, fn index ->
        extent_start = index * @chunk
        span_start = max(offset, extent_start)
        span_end = min(offset + length, extent_start + @chunk)

        %{
          index: index,
          width: min(@chunk, @size - extent_start),
          read_start: span_start - extent_start,
          read_length: span_end - span_start,
          target: :hole,
          hash: nil,
          locations: [],
          compression: :none,
          encrypted: false
        }
      end)

    {:ok, %{chunk_bytes: @chunk, size: @size, extents: extents}}
  end

  defp commit_reply, do: {:ok, %{chunks_published: 0}}

  # The data plane, as far as this test needs one: a write answers with a ref
  # per chunk it was handed, and a fetch answers with the bytes an extent's
  # chunk would hold.
  defp stub_data_plane do
    Application.put_env(:neonfs_block, :write_chunks_fn, fn _volume, chunks ->
      {:ok,
       Enum.map(chunks, fn data ->
         %{
           hash: :crypto.hash(:sha256, data),
           locations: [%{node: node(), drive_id: "default", tier: :hot}],
           size: byte_size(data),
           codec: %{compression: :none, crypto: nil, original_size: byte_size(data)}
         }
       end)}
    end)

    Application.put_env(:neonfs_block, :fetch_chunk_fn, fn _volume, ref, _opts ->
      {:ok, :binary.copy(<<0xC7>>, ref.width)}
    end)
  end

  defp open_device_reply do
    {:ok,
     %{
       id: "device-id",
       size: @size,
       chunk_bytes: @chunk,
       epoch: 0,
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
