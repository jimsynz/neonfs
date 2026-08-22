defmodule NeonFS.Core.BlockDeviceInvariantTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{BlockBacking, BlockIndex}

  @moduletag :tmp_dir

  @chunk BlockBacking.chunk_bytes()
  @block 4096

  setup %{tmp_dir: tmp_dir} do
    {:ok, _cluster_id} = start_provisioned_cluster(tmp_dir)

    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    name = "blkinv-#{:rand.uniform(999_999)}"
    {:ok, _volume} = create_provisioned_volume(name)
    {:ok, device} = BlockBacking.create_device(name, "/dev.img", 32 * @chunk)
    {:ok, volume_name: name, device: device}
  end

  test "the extent map holds one entry per extent a write touched, however many writes", %{
    volume_name: volume_name,
    device: device
  } do
    extents = div(device.size, @chunk)

    touched =
      for _ <- 1..40, into: MapSet.new() do
        offset = :rand.uniform(div(device.size, @block)) * @block - @block
        payload = :binary.copy(<<:rand.uniform(255)>>, @block)
        assert {:ok, _cost} = BlockBacking.write(volume_name, device.path, offset, payload)
        div(offset, @chunk)
      end

    assert {:ok, written} = BlockIndex.range(volume_name, 0, extents - 1)

    assert written |> Enum.map(&elem(&1, 0)) |> MapSet.new() == touched,
           "the map holds an entry for exactly the extents that were written"
  end

  test "every written block reads back after many overlapping writes", %{
    volume_name: volume_name,
    device: device
  } do
    writes =
      for index <- 0..31 do
        offset = index * @block
        payload = :binary.copy(<<index>>, @block)
        assert {:ok, _} = BlockBacking.write(volume_name, device.path, offset, payload)
        {offset, payload}
      end

    for {offset, payload} <- writes do
      assert {:ok, ^payload} = BlockBacking.read(volume_name, device.path, offset, @block)
    end
  end

  # What `fio --verify` at iodepth=8 does, and what a guest filesystem does
  # whenever it has more than one write in flight. Disjoint extents are
  # distinct keys, so the writers collide only on the shard roots they share
  # — which the commit's compare-and-swap has to resolve by retrying rather
  # than by the last writer winning.
  @tag timeout: 120_000
  test "concurrent writes to distinct extents all survive", %{
    volume_name: volume_name,
    device: device
  } do
    parent = self()

    writers =
      for index <- 0..7 do
        offset = index * @chunk
        payload = :binary.copy(<<index + 1>>, @block)

        spawn(fn ->
          result = BlockBacking.write(volume_name, device.path, offset, payload)
          send(parent, {:written, index, result})
        end)

        {offset, payload}
      end

    for index <- 0..7 do
      assert_receive {:written, ^index, {:ok, _cost}}, 60_000
    end

    for {offset, payload} <- writers do
      assert {:ok, ^payload} = BlockBacking.read(volume_name, device.path, offset, @block),
             "the write at offset #{offset} did not survive"
    end
  end
end
