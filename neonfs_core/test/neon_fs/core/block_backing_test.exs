defmodule NeonFS.Core.BlockBackingTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{BlockBacking, VolumeRegistry}

  @moduletag :tmp_dir

  @chunk BlockBacking.chunk_bytes()
  @block 4096

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_drive_registry()
    start_blob_store()
    start_chunk_index()
    start_file_index()
    start_stripe_index()
    start_volume_registry()
    ensure_chunk_access_tracker()

    on_exit(fn -> cleanup_test_dirs() end)

    volume_name = "block-#{:rand.uniform(999_999)}"
    {:ok, volume} = VolumeRegistry.create(volume_name, [])

    {:ok, volume: volume, volume_name: volume_name}
  end

  describe "create_device/4" do
    test "lays down a sized device whose zero chunks dedup to one blob", %{
      volume_name: volume_name
    } do
      size = 8 * @chunk

      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", size)

      assert device.size == size
      assert device.chunk_bytes == @chunk
      assert device.logical_block_bytes == @block
      assert device.physical_block_bytes == @block

      {:ok, meta} = NeonFS.Core.get_file_meta_by_id(volume_name, device.file_id)

      assert length(meta.chunks) == 8
      assert meta.chunks |> Enum.uniq() |> length() == 1
    end

    test "an unwritten device reads as zeroes", %{volume_name: volume_name} do
      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", 4 * @chunk)

      assert {:ok, data} = BlockBacking.read(volume_name, device.file_id, 0, 2 * @block)
      assert data == :binary.copy(<<0>>, 2 * @block)
    end

    test "refuses a size that is not a positive multiple of the logical block", %{
      volume_name: volume_name
    } do
      assert {:error, {:invalid_device_size, 0}} =
               BlockBacking.create_device(volume_name, "/dev.img", 0)

      assert {:error, {:invalid_device_size, 4097}} =
               BlockBacking.create_device(volume_name, "/dev.img", 4097)
    end

    test "refuses a device larger than the volume's max_size" do
      volume_name = "block-capped-#{:rand.uniform(999_999)}"
      {:ok, _volume} = VolumeRegistry.create(volume_name, max_size: 2 * @chunk)

      assert {:error, {:device_exceeds_volume_max_size, _size, _max}} =
               BlockBacking.create_device(volume_name, "/dev.img", 4 * @chunk)
    end

    test "refuses a chunk strategy other than the forced fixed one", %{volume_name: volume_name} do
      assert {:error, {:unsupported_chunk_strategy, :auto}} =
               BlockBacking.create_device(volume_name, "/dev.img", @chunk, chunk_strategy: :auto)

      assert {:error, {:unsupported_chunk_strategy, {:fastcdc, 65_536}}} =
               BlockBacking.create_device(volume_name, "/dev.img", @chunk,
                 chunk_strategy: {:fastcdc, 65_536}
               )
    end

    test "refuses to take a path that already holds a device", %{volume_name: volume_name} do
      {:ok, _device} = BlockBacking.create_device(volume_name, "/dev.img", @chunk)

      assert {:error, %NeonFS.Error.AlreadyExists{resource: "/dev.img"}} =
               BlockBacking.create_device(volume_name, "/dev.img", @chunk)
    end
  end

  describe "write/5" do
    setup %{volume_name: volume_name} do
      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", 8 * @chunk)
      {:ok, device: device}
    end

    test "a block write rewrites only the chunk it lands in", %{
      volume_name: volume_name,
      device: device
    } do
      ref =
        :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :write]])

      payload = :binary.copy(<<0xAB>>, @block)
      offset = 3 * @chunk

      assert :ok = BlockBacking.write(volume_name, device.file_id, offset, payload)

      assert_receive {[:neonfs, :block, :write], ^ref, measurements, _meta}, 1_000
      assert measurements.guest_bytes == @block
      assert measurements.chunks_rewritten == 1
      assert measurements.chunk_bytes == @chunk

      assert {:ok, ^payload} = BlockBacking.read(volume_name, device.file_id, offset, @block)

      assert {:ok, tail} =
               BlockBacking.read(volume_name, device.file_id, offset + @block, @block)

      assert tail == :binary.copy(<<0>>, @block)

      {:ok, meta} = NeonFS.Core.get_file_meta_by_id(volume_name, device.file_id)
      assert length(meta.chunks) == 8
      assert meta.size == 8 * @chunk
    end

    test "a write spanning a chunk boundary rewrites both chunks", %{
      volume_name: volume_name,
      device: device
    } do
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :write]])

      offset = @chunk - @block
      payload = :binary.copy(<<0xCD>>, 2 * @block)

      assert :ok = BlockBacking.write(volume_name, device.file_id, offset, payload)

      assert_receive {[:neonfs, :block, :write], ^ref, measurements, _meta}, 1_000
      assert measurements.chunks_rewritten == 2
      assert measurements.chunk_bytes == 2 * @chunk

      assert {:ok, ^payload} =
               BlockBacking.read(volume_name, device.file_id, offset, 2 * @block)
    end

    test "refuses an unaligned offset or length", %{volume_name: volume_name, device: device} do
      assert {:error, {:unaligned_request, 1, _}} =
               BlockBacking.write(volume_name, device.file_id, 1, :binary.copy(<<1>>, @block))

      assert {:error, {:unaligned_request, 0, 3}} =
               BlockBacking.write(volume_name, device.file_id, 0, <<1, 2, 3>>)
    end

    test "refuses a write past the end of the device", %{
      volume_name: volume_name,
      device: device
    } do
      assert {:error, {:out_of_range, _offset, _length, _size}} =
               BlockBacking.write(
                 volume_name,
                 device.file_id,
                 8 * @chunk - @block,
                 :binary.copy(<<1>>, 2 * @block)
               )

      {:ok, meta} = NeonFS.Core.get_file_meta_by_id(volume_name, device.file_id)
      assert meta.size == 8 * @chunk
    end

    test "refuses a chunk strategy other than the forced fixed one", %{
      volume_name: volume_name,
      device: device
    } do
      assert {:error, {:unsupported_chunk_strategy, :auto}} =
               BlockBacking.write(
                 volume_name,
                 device.file_id,
                 0,
                 :binary.copy(<<1>>, @block),
                 chunk_strategy: :auto
               )
    end
  end

  describe "read_stream/4" do
    test "yields one element per chunk of the range", %{volume_name: volume_name} do
      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", 4 * @chunk)

      assert {:ok, stream} = BlockBacking.read_stream(volume_name, device.file_id, 0, 4 * @chunk)

      sizes = stream |> Enum.map(&byte_size/1)

      assert Enum.sum(sizes) == 4 * @chunk
      assert Enum.all?(sizes, &(&1 <= @chunk))
    end
  end

  describe "write_zeroes/4 and discard/4" do
    setup %{volume_name: volume_name} do
      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", 4 * @chunk)
      {:ok, meta} = NeonFS.Core.get_file_meta_by_id(volume_name, device.file_id)
      {:ok, device: device, zero_hash: hd(meta.chunks)}
    end

    test "zeroing written chunks returns them to the canonical zero chunk", %{
      volume_name: volume_name,
      device: device,
      zero_hash: zero_hash
    } do
      :ok =
        BlockBacking.write(volume_name, device.file_id, 0, :binary.copy(<<0xEF>>, 2 * @chunk))

      {:ok, dirty} = NeonFS.Core.get_file_meta_by_id(volume_name, device.file_id)
      refute Enum.at(dirty.chunks, 0) == zero_hash

      assert :ok = BlockBacking.write_zeroes(volume_name, device.file_id, 0, 2 * @chunk)

      {:ok, clean} = NeonFS.Core.get_file_meta_by_id(volume_name, device.file_id)
      assert clean.chunks == List.duplicate(zero_hash, 4)
      assert clean.size == 4 * @chunk

      assert {:ok, data} = BlockBacking.read(volume_name, device.file_id, 0, 2 * @block)
      assert data == :binary.copy(<<0>>, 2 * @block)
    end

    test "a sub-chunk discard zero-fills without disturbing its neighbours", %{
      volume_name: volume_name,
      device: device
    } do
      :ok = BlockBacking.write(volume_name, device.file_id, 0, :binary.copy(<<0xEF>>, @chunk))

      assert :ok = BlockBacking.discard(volume_name, device.file_id, 0, @block)

      assert {:ok, discarded} = BlockBacking.read(volume_name, device.file_id, 0, @block)
      assert discarded == :binary.copy(<<0>>, @block)

      assert {:ok, kept} = BlockBacking.read(volume_name, device.file_id, @block, @block)
      assert kept == :binary.copy(<<0xEF>>, @block)
    end
  end

  describe "flush/2" do
    test "returns once the backing file's chunks are durable", %{volume_name: volume_name} do
      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", @chunk)
      :ok = BlockBacking.write(volume_name, device.file_id, 0, :binary.copy(<<7>>, @block))

      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :flush]])

      assert :ok = BlockBacking.flush(volume_name, device.file_id)
      assert_receive {[:neonfs, :block, :flush], ^ref, _measurements, %{status: :ok}}, 1_000
    end
  end

  describe "open_device/2 and device_info/2" do
    test "a device stays addressable by id across a rename", %{volume_name: volume_name} do
      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", @chunk)

      assert {:ok, opened} = BlockBacking.open_device(volume_name, "/dev.img")
      assert opened.file_id == device.file_id

      :ok = NeonFS.Core.rename_file(volume_name, "/dev.img", "/renamed.img")

      assert {:ok, info} = BlockBacking.device_info(volume_name, device.file_id)
      assert info.size == @chunk
      assert info.path == "/renamed.img"
    end
  end
end
