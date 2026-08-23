defmodule NeonFS.Block.DeviceTest do
  @moduledoc """
  The extent arithmetic, which lives on this node now that it moves its own
  bytes.

  Core is stubbed at the two calls that remain — `read_refs` describing the
  map and `commit_written` publishing it — and so are the two data-plane
  calls. What is under test is what happens between them: which extents a
  request touches, which of them have to be read before they can be
  written, and what the commit ends up naming.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Block.Device

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

    # The seam asserts the ref's shape rather than accepting anything: the
    # real `ChunkReader.fetch_chunk/3` dials `hash` and `locations`, and a ref
    # missing either is a crash on the first real read.
    Application.put_env(:neonfs_block, :fetch_chunk_fn, fn _volume, ref, opts ->
      send(test, {:fetched, ref.index, opts})
      %{hash: hash, locations: [_ | _]} = ref
      true = is_binary(hash)
      {:ok, stored_bytes(ref)}
    end)

    on_exit(fn ->
      Application.delete_env(:neonfs_block, :core_call_fn)
      Application.delete_env(:neonfs_block, :write_chunks_fn)
      Application.delete_env(:neonfs_block, :fetch_chunk_fn)
      Application.delete_env(:neonfs_block, :written_extents)
      Application.delete_env(:neonfs_block, :failing_commits)
    end)

    # An attached device has a write window, because that is what
    # `DeviceRegistry` gives it. Driving `Device` without one would be
    # driving something that cannot exist.
    {:ok, opened} = Device.open(@export)
    window = start_supervised!({NeonFS.Block.WriteWindow, opened})

    {:ok, device: Map.put(opened, :window, window)}
  end

  describe "read_stream/3" do
    test "synthesises a hole's zeroes rather than fetching anything", %{device: device} do
      assert {:ok, stream} = Device.read_stream(device, 0, 2 * @block)
      assert IO.iodata_to_binary(Enum.to_list(stream)) == :binary.copy(<<0>>, 2 * @block)

      refute_received {:fetched, _index, _opts}
    end

    test "yields one element per extent and slices each to the range", %{device: device} do
      write_extents([0, 1, 2, 3])

      assert {:ok, stream} = Device.read_stream(device, @chunk - @block, 2 * @block)
      parts = Enum.to_list(stream)

      assert Enum.map(parts, &byte_size/1) == [@block, @block]
      assert_received {:fetched, 0, _}
      assert_received {:fetched, 1, _}
    end

    # The ref core hands back has to be one the data plane can use as-is —
    # a hash to dial and locations to dial it at.
    test "hands the data plane a ref it can dial", %{device: device} do
      write_extents([0])

      {:ok, stream} = Device.read_stream(device, 0, @block)
      assert [bytes] = Enum.to_list(stream)
      assert byte_size(bytes) == @block

      assert_received {:fetched, 0, _opts}
    end

    test "tags its fetches with the export, so one device is separable", %{device: device} do
      write_extents([0])

      {:ok, stream} = Device.read_stream(device, 0, @block)
      Enum.to_list(stream)

      assert_received {:fetched, 0, opts}
      assert Keyword.fetch!(opts, :telemetry_metadata) == %{export: @export}
    end
  end

  describe "write/3" do
    test "an extent the write covers end to end is not read first", %{device: device} do
      write_extents([0])

      assert :ok = Device.write(device, 0, :binary.copy(<<0xAA>>, @chunk))
      assert :ok = Device.flush(device)

      refute_received {:fetched, 0, _opts}
      assert_received {:written, [@chunk]}
    end

    test "a sub-extent write reads the extent, splices, and writes it whole", %{device: device} do
      write_extents([0])

      assert :ok = Device.write(device, @block, :binary.copy(<<0xBB>>, @block))
      assert :ok = Device.flush(device)

      assert_received {:fetched, 0, _opts}
      assert_received {:written, [@chunk]}
    end

    # The commit compares only what the write read. An extent it overwrote
    # end to end owes nothing to what was there before, and naming it would
    # make two writers to different extents collide for no reason.
    test "names only the extents it read as its expectation", %{device: device} do
      write_extents([0, 1])

      # Covers extent 0's tail and all of extent 1.
      assert :ok = Device.write(device, @chunk - @block, :binary.copy(<<0xCC>>, @chunk + @block))
      assert :ok = Device.flush(device)

      assert_received {:core_call, :commit_written, [_volume, _path, extents, opts]}
      assert [{0, _}, {1, _}] = extents
      assert [{0, {:chunk, _}}] = Keyword.fetch!(opts, :expect)
    end

    test "stamps the commit with the attachment's epoch", %{device: device} do
      assert :ok = Device.write(device, 0, :binary.copy(<<1>>, @chunk))
      assert :ok = Device.flush(device)

      assert_received {:core_call, :commit_written, [_volume, _path, _extents, opts]}
      assert Keyword.fetch!(opts, :epoch) == 0
    end

    test "one data-plane call places every extent the write spans", %{device: device} do
      assert :ok = Device.write(device, 0, :binary.copy(<<2>>, 3 * @chunk))
      assert :ok = Device.flush(device)

      assert_received {:written, [@chunk, @chunk, @chunk]}
      refute_received {:written, _sizes}
    end
  end

  # Losing a race is not a device fault. The metadata layer answers both of
  # these when a burst of writes collides with itself, and a guest that is
  # handed an IO error for one remounts read-only over a queue depth.
  describe "contended writes" do
    test "retries a commit whose extent moved under its read", %{device: device} do
      Application.put_env(:neonfs_block, :stale_write_backoff_ms, 1)
      on_exit(fn -> Application.delete_env(:neonfs_block, :stale_write_backoff_ms) end)

      fail_commits(1, :stale_chunks)
      assert :ok = Device.write(device, 0, :binary.copy(<<1>>, @block))
      assert :ok = Device.flush(device)
    end

    test "retries a commit whose compare-and-swap ran out of attempts", %{device: device} do
      Application.put_env(:neonfs_block, :stale_write_backoff_ms, 1)
      on_exit(fn -> Application.delete_env(:neonfs_block, :stale_write_backoff_ms) end)

      fail_commits(1, {:chunk_index_failed, {:cas_retries_exhausted, %{}}})
      assert :ok = Device.write(device, 0, :binary.copy(<<1>>, @block))
      assert :ok = Device.flush(device)
    end

    # An error that is not contention is the caller's answer, not something
    # to grind against.
    test "does not retry an error that is not contention", %{device: device} do
      fail_commits(99, :eperm)
      assert :ok = Device.write(device, 0, :binary.copy(<<1>>, @block))
      assert {:error, :eperm} = Device.flush(device)
    end
  end

  describe "write_zeroes/3" do
    test "punches an extent the range covers end to end, writing nothing", %{device: device} do
      write_extents([0, 1, 2, 3])

      assert :ok = Device.write_zeroes(device, 0, @size)

      assert_received {:core_call, :commit_written, [_volume, _path, extents, _opts]}
      assert extents == [{0, :hole}, {1, :hole}, {2, :hole}, {3, :hole}]
      refute_received {:written, _sizes}
    end

    test "read-modify-writes an extent it only clips", %{device: device} do
      write_extents([0])

      assert :ok = Device.write_zeroes(device, 0, @block)

      assert_received {:fetched, 0, _opts}
      assert_received {:written, [@chunk]}
    end

    # Zeroing the last non-zero part of an extent leaves nothing worth
    # storing, so it is dropped like one the range covered outright.
    test "punches a clipped extent that ends up entirely zeroes", %{device: device} do
      assert :ok = Device.write_zeroes(device, 0, @block)

      assert_received {:core_call, :commit_written, [_volume, _path, [{0, :hole}], _opts]}
      refute_received {:written, _sizes}
    end
  end

  # ─── The stub cluster ──────────────────────────────────────────────────

  defp fail_commits(count, reason) do
    Application.put_env(:neonfs_block, :failing_commits, {count, reason})
    on_exit(fn -> Application.delete_env(:neonfs_block, :failing_commits) end)
  end

  defp write_extents(indices) do
    Application.put_env(:neonfs_block, :written_extents, MapSet.new(indices))
  end

  defp written?(index) do
    :neonfs_block
    |> Application.get_env(:written_extents, MapSet.new())
    |> MapSet.member?(index)
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
          target: if(written?(index), do: {:chunk, extent_hash(index)}, else: :hole),
          hash: if(written?(index), do: extent_hash(index)),
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
        {:ok, %{chunks_published: Enum.count(extents, &is_binary(elem(&1, 1)))}}
    end
  end

  defp reply(_other, _args), do: :ok

  defp extent_hash(index), do: :crypto.hash(:sha256, "extent-#{index}")

  # A written extent holds a recognisable non-zero byte, so a splice that
  # keeps the wrong part of it is visible rather than silently zero.
  defp stored_bytes(ref), do: :binary.copy(<<0xD9>>, ref.width)

  defp chunk_ref(data) do
    %{
      hash: :crypto.hash(:sha256, data),
      locations: [%{node: node(), drive_id: "default", tier: :hot}],
      size: byte_size(data),
      codec: %{compression: :none, crypto: nil, original_size: byte_size(data)}
    }
  end
end
