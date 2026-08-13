defmodule NeonFS.Core.BlockDeviceInvariantTest do
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

    name = "blkinv-#{:rand.uniform(999_999)}"
    {:ok, _volume} = VolumeRegistry.create(name, [])
    {:ok, device} = BlockBacking.create_device(name, "/dev.img", 32 * @chunk)
    {:ok, volume_name: name, device: device}
  end

  test "a device's chunk list keeps its length under many partial writes", %{
    volume_name: volume_name,
    device: device
  } do
    expected = div(device.size, @chunk)

    for _ <- 1..40 do
      offset = :rand.uniform(div(device.size, @block)) * @block - @block
      payload = :binary.copy(<<:rand.uniform(255)>>, @block)
      assert {:ok, _cost} = BlockBacking.write(volume_name, device.file_id, offset, payload)
    end

    {:ok, meta} = NeonFS.Core.get_file_meta_by_id(volume_name, device.file_id)

    assert length(meta.chunks) == expected,
           "chunk list grew from #{expected} to #{length(meta.chunks)}"

    assert meta.size == device.size
  end

  test "every written block reads back after many overlapping writes", %{
    volume_name: volume_name,
    device: device
  } do
    writes =
      for index <- 0..31 do
        offset = index * @block
        payload = :binary.copy(<<index>>, @block)
        assert {:ok, _} = BlockBacking.write(volume_name, device.file_id, offset, payload)
        {offset, payload}
      end

    for {offset, payload} <- writes do
      assert {:ok, ^payload} = BlockBacking.read(volume_name, device.file_id, offset, @block)
    end
  end

  # What `fio --verify` at iodepth=8 does, and what a guest filesystem does
  # whenever it has more than one write in flight — eight is `fio`'s
  # `iodepth=8`, the shape that found this. Each write is a
  # read-modify-write of the file's chunk list, so without the commit
  # comparing the snapshot it was computed from, two in flight at once
  # each commit a list built from the same starting point and the second
  # silently discards the first's chunk.
  @tag timeout: 120_000
  test "concurrent writes to distinct chunks all survive", %{
    volume_name: volume_name,
    device: device
  } do
    expected = div(device.size, @chunk)
    parent = self()

    writers =
      for index <- 0..7 do
        offset = index * @chunk
        payload = :binary.copy(<<index + 1>>, @block)

        spawn(fn ->
          result = BlockBacking.write(volume_name, device.file_id, offset, payload)
          send(parent, {:written, index, result})
        end)

        {offset, payload}
      end

    for index <- 0..7 do
      assert_receive {:written, ^index, {:ok, _cost}}, 60_000
    end

    {:ok, meta} = NeonFS.Core.get_file_meta_by_id(volume_name, device.file_id)
    assert length(meta.chunks) == expected

    for {offset, payload} <- writers do
      assert {:ok, ^payload} = BlockBacking.read(volume_name, device.file_id, offset, @block),
             "the write at offset #{offset} did not survive"
    end
  end
end
