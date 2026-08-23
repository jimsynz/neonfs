defmodule NeonFS.Block.WriteWindowTest do
  @moduledoc """
  The window's own behaviour: what it coalesces, what bounds it, what it
  answers reads with, and what it does when a drain fails.

  Core and the data plane are stubbed at the four calls `Device` makes, so
  what is under test is the buffering rather than the transport.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Block.{Device, WriteWindow}

  @block 4096
  @chunk 4 * @block
  @size 4 * @chunk
  @export "vol:/dev.img"

  setup do
    test = self()

    Application.put_env(:neonfs_block, :core_call_fn, fn _module, function, args ->
      send(test, {:core_call, function, args})
      reply(function, args)
    end)

    Application.put_env(:neonfs_block, :write_chunks_fn, fn _volume, chunks ->
      send(test, {:written, Enum.map(chunks, &byte_size/1)})
      {:ok, Enum.map(chunks, &chunk_ref/1)}
    end)

    Application.put_env(:neonfs_block, :fetch_chunk_fn, fn _volume, ref, _opts ->
      send(test, {:fetched, ref.index})
      {:ok, :binary.copy(<<0xD9>>, ref.width)}
    end)

    on_exit(fn ->
      for key <- [
            :core_call_fn,
            :write_chunks_fn,
            :fetch_chunk_fn,
            :write_window_bytes,
            :write_window_ms,
            :written_extents,
            :failing_commits
          ] do
        Application.delete_env(:neonfs_block, key)
      end
    end)

    {:ok, device: device()}
  end

  describe "coalescing" do
    test "many writes into one extent cost one placement and one commit", %{device: device} do
      window = start_window(device)

      for offset <- 0..(@chunk - @block)//@block do
        assert :ok = WriteWindow.write(window, offset, :binary.copy(<<1>>, @block))
      end

      # The extent is read once, on first touch, and not again.
      assert_received {:fetched, 0}
      refute_received {:fetched, 0}
      refute_received {:written, _sizes}

      assert :ok = WriteWindow.flush(window)

      assert_received {:written, [@chunk]}
      assert_received {:core_call, :commit_written, [_volume, _path, [{0, _hash}], _opts]}
      refute_received {:core_call, :commit_written, _args}
    end

    test "writes spanning several extents drain as one commit", %{device: device} do
      window = start_window(device)

      assert :ok = WriteWindow.write(window, 0, :binary.copy(<<2>>, @block))
      assert :ok = WriteWindow.write(window, 2 * @chunk, :binary.copy(<<3>>, @block))

      assert :ok = WriteWindow.flush(window)

      assert_received {:written, [@chunk, @chunk]}
      assert_received {:core_call, :commit_written, [_volume, _path, extents, _opts]}
      assert [{0, _}, {2, _}] = Enum.sort(extents)
    end

    test "an extent covered end to end is never read", %{device: device} do
      window = start_window(device)

      assert :ok = WriteWindow.write(window, 0, :binary.copy(<<4>>, @chunk))
      refute_received {:fetched, 0}

      assert :ok = WriteWindow.flush(window)

      # And it names no expectation: it owes nothing to what was there before.
      assert_received {:core_call, :commit_written, [_volume, _path, _extents, opts]}
      assert Keyword.fetch!(opts, :expect) == []
    end
  end

  describe "bounds" do
    test "the byte cap drains before answering, so it bounds memory", %{device: device} do
      Application.put_env(:neonfs_block, :write_window_bytes, 2 * @chunk)
      window = start_window(device)

      assert :ok = WriteWindow.write(window, 0, :binary.copy(<<5>>, @block))
      refute_received {:written, _sizes}

      # The second extent takes it to the cap.
      assert :ok = WriteWindow.write(window, @chunk, :binary.copy(<<6>>, @block))

      assert_received {:written, [@chunk, @chunk]}
    end

    # A guest that never flushes is legal under NBD and alarming in a
    # post-mortem, so time alone has to make a write land.
    test "the time cap drains without a flush", %{device: device} do
      Application.put_env(:neonfs_block, :write_window_ms, 20)
      window = start_window(device)
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :window_drain]])

      assert :ok = WriteWindow.write(window, 0, :binary.copy(<<7>>, @block))

      assert_receive {[:neonfs, :block, :window_drain], ^ref, %{writes: 1}, %{reason: :time}},
                     2_000

      assert_received {:written, [@chunk]}
      :telemetry.detach(ref)
    end

    # A timer re-armed by every write is a timer a steady stream never lets
    # fire, which is not a bound.
    test "a steady stream of writes still drains on time", %{device: device} do
      Application.put_env(:neonfs_block, :write_window_ms, 60)
      window = start_window(device)
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :window_drain]])

      for offset <- 0..(7 * @block)//@block do
        assert :ok = WriteWindow.write(window, offset, :binary.copy(<<8>>, @block))
        Process.sleep(15)
      end

      assert_receive {[:neonfs, :block, :window_drain], ^ref, _measurements, %{reason: :time}},
                     2_000

      :telemetry.detach(ref)
    end

    test "a byte cap of zero drains every write as it arrives", %{device: device} do
      Application.put_env(:neonfs_block, :write_window_bytes, 0)
      window = start_window(device)

      assert :ok = WriteWindow.write(window, 0, :binary.copy(<<9>>, @block))

      assert_received {:written, [@chunk]}
      assert_received {:core_call, :commit_written, _args}
    end
  end

  # A write-back cache that does not answer reads is a correctness bug, not
  # a slower cache: the guest would read back what was there before.
  describe "read-through" do
    test "a buffered extent is answered from the window", %{device: device} do
      window = start_window(device)
      assert :ok = WriteWindow.write(window, 0, :binary.copy(<<0xAB>>, @block))

      assert {:ok, bytes} = WriteWindow.buffered(window, 0)
      assert byte_size(bytes) == @chunk
      assert binary_part(bytes, 0, @block) == :binary.copy(<<0xAB>>, @block)
    end

    test "an extent nothing has written is a miss", %{device: device} do
      window = start_window(device)

      assert :miss = WriteWindow.buffered(window, 3)
    end

    test "a drained extent stops being a hit", %{device: device} do
      window = start_window(device)
      assert :ok = WriteWindow.write(window, 0, :binary.copy(<<0xAC>>, @block))
      assert :ok = WriteWindow.flush(window)

      assert :miss = WriteWindow.buffered(window, 0)
    end

    # The device's read path is what has to consult it, or the overlay is
    # decoration.
    test "Device.read_stream sees a write the window is still holding", %{device: device} do
      window = start_window(device)
      device = Map.put(device, :window, window)

      assert :ok = Device.write(device, 0, :binary.copy(<<0xAD>>, @block))

      assert {:ok, stream} = Device.read_stream(device, 0, @block)
      assert [bytes] = Enum.to_list(stream)
      assert bytes == :binary.copy(<<0xAD>>, @block)
    end
  end

  describe "a failed drain" do
    test "poisons the window, so the next flush says so rather than lying", %{device: device} do
      window = start_window(device)
      fail_commits(99, :eperm)

      assert :ok = WriteWindow.write(window, 0, :binary.copy(<<0xAE>>, @block))
      assert {:error, :eperm} = WriteWindow.flush(window)

      # The writes it held were acknowledged and are gone; carrying on would
      # let a guest believe a later journal commit succeeded over them.
      assert {:error, :eperm} = WriteWindow.flush(window)
      assert {:error, :eperm} = WriteWindow.write(window, 0, :binary.copy(<<1>>, @block))
      assert {:error, :eperm} = WriteWindow.buffered(window, 0)
    end

    test "a contended drain is retried rather than poisoning it", %{device: device} do
      window = start_window(device)
      fail_commits(1, :stale_chunks)

      assert :ok = WriteWindow.write(window, 0, :binary.copy(<<0xAF>>, @block))
      assert :ok = WriteWindow.flush(window)
    end
  end

  describe "ordering" do
    # A punch issued after a write must not land before it, or the write
    # comes back from the dead.
    test "drain/1 lands what is held before an operation that bypasses it", %{device: device} do
      window = start_window(device)

      assert :ok = WriteWindow.write(window, 0, :binary.copy(<<0xB0>>, @block))
      refute_received {:core_call, :commit_written, _args}

      assert :ok = WriteWindow.drain(window)
      assert_received {:core_call, :commit_written, _args}
    end

    test "draining an empty window is a no-op", %{device: device} do
      window = start_window(device)

      assert :ok = WriteWindow.drain(window)
      refute_received {:core_call, :commit_written, _args}
    end
  end

  # ─── The stub cluster ──────────────────────────────────────────────────

  defp start_window(device), do: start_supervised!({WriteWindow, device})

  defp device do
    {:ok, opened} = Device.open(@export)
    opened
  end

  defp fail_commits(count, reason) do
    Application.put_env(:neonfs_block, :failing_commits, {count, reason})
  end

  defp reply(:open_device, _args) do
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

  defp reply(:read_refs, [_volume, _path, offset, length]) do
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
          target: {:chunk, :crypto.hash(:sha256, "extent-#{index}")},
          hash: :crypto.hash(:sha256, "extent-#{index}"),
          locations: [%{node: node(), drive_id: "default", tier: :hot}],
          compression: :none,
          encrypted: false
        }
      end)

    {:ok, %{chunk_bytes: @chunk, size: @size, extents: extents}}
  end

  defp reply(:commit_written, [_volume, _path, extents, _opts]) do
    case Application.get_env(:neonfs_block, :failing_commits) do
      {remaining, reason} when remaining > 0 ->
        Application.put_env(:neonfs_block, :failing_commits, {remaining - 1, reason})
        {:error, reason}

      _settled ->
        {:ok, %{chunks_published: length(extents)}}
    end
  end

  defp reply(_other, _args), do: :ok

  defp chunk_ref(data) do
    %{
      hash: :crypto.hash(:sha256, data),
      locations: [%{node: node(), drive_id: "default", tier: :hot}],
      size: byte_size(data),
      codec: %{compression: :none, crypto: nil, original_size: byte_size(data)}
    }
  end
end
