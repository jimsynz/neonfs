defmodule NeonFS.Core.BlockBackingTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{BlockBacking, BlockEpoch, BlockIndex, ChunkIndex}

  @moduletag :tmp_dir

  @chunk BlockBacking.chunk_bytes()
  @block 4096

  # The extent map is a real metadata tree, so these need a provisioned
  # cluster rather than the index GenServers alone — and the fencing epoch
  # is a consensus read, so they need Ra as well.
  setup %{tmp_dir: tmp_dir} do
    {:ok, _cluster_id} = start_provisioned_cluster(tmp_dir)

    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    volume_name = "block-#{:rand.uniform(999_999)}"
    {:ok, volume} = create_provisioned_volume(volume_name)

    {:ok, volume: volume, volume_name: volume_name}
  end

  describe "create_device/4" do
    test "publishes a header and no device data at all", %{volume_name: volume_name} do
      size = 8 * @chunk

      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", size)

      assert device.size == size
      assert device.chunk_bytes == @chunk
      assert device.logical_block_bytes == @block
      assert device.physical_block_bytes == @block

      assert {:ok, header} = BlockIndex.get_device(volume_name)
      assert header.path == "/dev.img"
      assert header.size_bytes == size

      # No file, and no extent: every extent of a fresh device is a hole.
      assert {:error, _reason} = NeonFS.Core.get_file_meta(volume_name, "/dev.img")
      assert {:ok, []} = BlockIndex.range(volume_name, 0, 7)
    end

    test "an unwritten device reads as zeroes", %{volume_name: volume_name} do
      {:ok, _device} = BlockBacking.create_device(volume_name, "/dev.img", 4 * @chunk)

      assert {:ok, data} = BlockBacking.read(volume_name, "/dev.img", 0, 2 * @block)
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
      {:ok, _volume} = create_provisioned_volume(volume_name, max_size: 2 * @chunk)

      assert {:error, {:device_exceeds_volume_max_size, _size, _max}} =
               BlockBacking.create_device(volume_name, "/dev.img", 4 * @chunk)
    end

    test "refuses a second device, naming the one the volume already holds", %{
      volume_name: volume_name
    } do
      {:ok, _device} = BlockBacking.create_device(volume_name, "/dev.img", @chunk)

      assert {:error, %NeonFS.Error.AlreadyExists{resource: "/dev.img"}} =
               BlockBacking.create_device(volume_name, "/second.img", @chunk)
    end
  end

  describe "per-volume extent width" do
    test "defaults to the spike's 128 KiB, so the measured baseline carries over" do
      name = "block-default-#{:rand.uniform(999_999)}"
      {:ok, volume} = NeonFS.Core.create_volume(name, type: :block, max_size: 4 * @chunk)

      assert volume.block_chunk_bytes == BlockBacking.chunk_bytes()
      assert {:ok, device} = BlockBacking.open_device(name, BlockBacking.device_path())
      assert device.chunk_bytes == BlockBacking.chunk_bytes()
    end

    test "a volume stores its device at the width it names" do
      name = "block-sized-#{:rand.uniform(999_999)}"
      width = 4096 * 8

      {:ok, volume} =
        NeonFS.Core.create_volume(name,
          type: :block,
          max_size: 16 * width,
          block_chunk_bytes: width
        )

      assert volume.block_chunk_bytes == width

      assert {:ok, device} = BlockBacking.open_device(name, BlockBacking.device_path())
      assert device.chunk_bytes == width

      # One extent per `width` bytes, so a whole-device write lays down 16.
      {:ok, _} =
        BlockBacking.write(name, device.path, 0, :binary.copy(<<0xAA>>, 16 * width))

      assert {:ok, extents} = BlockIndex.range(name, 0, 15)
      assert length(extents) == 16
    end

    # A volume predating the field reads as the default rather than as `nil`,
    # which would divide by zero somewhere further down.
    test "a volume with no width recorded reads as the default" do
      assert BlockBacking.chunk_bytes_for(%{block_chunk_bytes: nil}) ==
               BlockBacking.chunk_bytes()

      assert BlockBacking.chunk_bytes_for(%{}) == BlockBacking.chunk_bytes()
    end

    test "a filesystem volume records no block extent width" do
      name = "fs-nochunk-#{:rand.uniform(999_999)}"
      {:ok, volume} = NeonFS.Core.create_volume(name, max_size: 4 * @chunk)

      assert volume.block_chunk_bytes == nil
    end
  end

  describe "provision_volume_device/1" do
    test "a block volume created through core owns a device sized to its maximum" do
      name = "block-provisioned-#{:rand.uniform(999_999)}"

      {:ok, volume} =
        NeonFS.Core.create_volume(name, type: :block, max_size: 4 * @chunk)

      assert volume.type == :block

      assert {:ok, device} = BlockBacking.open_device(name, BlockBacking.device_path())
      assert device.size == 4 * @chunk
    end

    test "a filesystem volume is left alone" do
      name = "fs-#{:rand.uniform(999_999)}"

      {:ok, _volume} = NeonFS.Core.create_volume(name, max_size: 4 * @chunk)

      assert {:error, {:device_not_found, ^name, _path}} =
               BlockBacking.open_device(name, BlockBacking.device_path())
    end

    test "deleting a block volume takes its device with it" do
      start_namespace_coordination()
      name = "block-deleted-#{:rand.uniform(999_999)}"

      {:ok, _volume} = NeonFS.Core.create_volume(name, type: :block, max_size: 2 * @chunk)
      assert {:ok, _device} = BlockBacking.open_device(name, BlockBacking.device_path())

      assert :ok = NeonFS.Core.delete_volume(name)
      refute NeonFS.Core.volume_exists?(name)
    end

    test "a filesystem volume's content still blocks its deletion" do
      start_namespace_coordination()
      name = "fs-nonempty-#{:rand.uniform(999_999)}"

      {:ok, _volume} = NeonFS.Core.create_volume(name, [])
      {:ok, _meta} = NeonFS.Core.write_file_streamed(name, "/a.txt", ["content"])

      assert {:error, error} = NeonFS.Core.delete_volume(name)
      assert Exception.message(error) =~ "cannot delete"
    end

    test "a device that cannot be published takes its volume with it" do
      name = "block-unaligned-#{:rand.uniform(999_999)}"

      assert {:error, {:invalid_device_size, 4097}} =
               NeonFS.Core.create_volume(name, type: :block, max_size: 4097)

      refute NeonFS.Core.volume_exists?(name)
    end
  end

  describe "write/5" do
    setup %{volume_name: volume_name} do
      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", 8 * @chunk)
      {:ok, device: device}
    end

    test "a block write rewrites only the extent it lands in", %{
      volume_name: volume_name,
      device: device
    } do
      ref =
        :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :write]])

      payload = :binary.copy(<<0xAB>>, @block)
      offset = 3 * @chunk

      assert {:ok, cost} = BlockBacking.write(volume_name, device.path, offset, payload)

      assert_receive {[:neonfs, :block, :write], ^ref, measurements, _meta}, 1_000
      assert measurements.guest_bytes == @block
      assert measurements.chunks_rewritten == 1
      assert measurements.chunk_bytes == @chunk

      # The reply carries the same cost as the event, because a caller on
      # another node never sees this node's telemetry.
      assert cost == %{chunk_bytes: @chunk, chunks_rewritten: 1}

      assert {:ok, ^payload} = BlockBacking.read(volume_name, device.path, offset, @block)

      assert {:ok, tail} =
               BlockBacking.read(volume_name, device.path, offset + @block, @block)

      assert tail == :binary.copy(<<0>>, @block)

      # Only the extent it landed in exists; the rest of the device is holes.
      assert {:ok, [{3, {:chunk, _hash}}]} = BlockIndex.range(volume_name, 0, 7)
    end

    test "a write spanning an extent boundary rewrites both extents", %{
      volume_name: volume_name,
      device: device
    } do
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :write]])

      offset = @chunk - @block
      payload = :binary.copy(<<0xCD>>, 2 * @block)

      assert {:ok, cost} = BlockBacking.write(volume_name, device.path, offset, payload)

      assert_receive {[:neonfs, :block, :write], ^ref, measurements, _meta}, 1_000
      assert measurements.chunks_rewritten == 2
      assert measurements.chunk_bytes == 2 * @chunk

      assert cost == %{chunk_bytes: 2 * @chunk, chunks_rewritten: 2}

      assert {:ok, ^payload} =
               BlockBacking.read(volume_name, device.path, offset, 2 * @block)
    end

    test "the chunks a write published are committed, not left holding its ref", %{
      volume_name: volume_name,
      volume: volume,
      device: device
    } do
      assert {:ok, _cost} =
               BlockBacking.write(volume_name, device.path, 0, :binary.copy(<<9>>, @block))

      assert {:ok, [{0, {:chunk, hash}}]} = BlockIndex.range(volume_name, 0, 0)
      assert {:ok, chunk_meta} = ChunkIndex.get(volume.id, hash)
      assert chunk_meta.commit_state == :committed
      assert MapSet.size(chunk_meta.active_write_refs) == 0
    end

    test "refuses an unaligned offset or length", %{volume_name: volume_name, device: device} do
      assert {:error, {:unaligned_request, 1, _}} =
               BlockBacking.write(volume_name, device.path, 1, :binary.copy(<<1>>, @block))

      assert {:error, {:unaligned_request, 0, 3}} =
               BlockBacking.write(volume_name, device.path, 0, <<1, 2, 3>>)
    end

    test "refuses a write past the end of the device", %{
      volume_name: volume_name,
      device: device
    } do
      assert {:error, {:out_of_range, _offset, _length, _size}} =
               BlockBacking.write(
                 volume_name,
                 device.path,
                 8 * @chunk - @block,
                 :binary.copy(<<1>>, 2 * @block)
               )
    end
  end

  describe "fencing" do
    setup %{volume_name: volume_name} do
      {:ok, _device} = BlockBacking.create_device(volume_name, "/dev.img", 2 * @chunk)
      {:ok, attached} = BlockBacking.open_device(volume_name, "/dev.img")
      {:ok, attached: attached}
    end

    test "a preempted holder's write is refused, and the preemptor's is not", %{
      volume: volume,
      volume_name: volume_name,
      attached: attached
    } do
      payload = :binary.copy(<<0x5A>>, @block)

      assert {:ok, _cost} =
               BlockBacking.write(volume_name, attached.path, 0, payload, epoch: attached.epoch)

      assert {:ok, preemptor} = BlockEpoch.bump({volume.id, attached.path})
      assert preemptor > attached.epoch

      assert {:error, {:fenced, ^preemptor}} =
               BlockBacking.write(volume_name, attached.path, @chunk, payload,
                 epoch: attached.epoch
               )

      assert {:ok, _cost} =
               BlockBacking.write(volume_name, attached.path, @chunk, payload, epoch: preemptor)
    end

    test "a fenced write leaves nothing behind for its extent", %{
      volume: volume,
      volume_name: volume_name,
      attached: attached
    } do
      {:ok, _preemptor} = BlockEpoch.bump({volume.id, attached.path})

      assert {:error, {:fenced, _current}} =
               BlockBacking.write(volume_name, attached.path, 0, :binary.copy(<<3>>, @block),
                 epoch: attached.epoch
               )

      assert {:ok, []} = BlockIndex.range(volume_name, 0, 1)
      assert {:ok, zeroes} = BlockBacking.read(volume_name, attached.path, 0, @block)
      assert zeroes == :binary.copy(<<0>>, @block)
    end
  end

  describe "read_stream/4" do
    test "yields one element per extent of the range", %{volume_name: volume_name} do
      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", 4 * @chunk)

      assert {:ok, stream} = BlockBacking.read_stream(volume_name, device.path, 0, 4 * @chunk)

      sizes = stream |> Enum.map(&byte_size/1)

      assert Enum.sum(sizes) == 4 * @chunk
      assert Enum.all?(sizes, &(&1 <= @chunk))
      assert length(sizes) == 4
    end
  end

  describe "write_zeroes/5 and discard/5" do
    setup %{volume_name: volume_name} do
      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", 4 * @chunk)
      {:ok, device: device}
    end

    test "zeroing written extents drops them from the map", %{
      volume_name: volume_name,
      device: device
    } do
      {:ok, _} =
        BlockBacking.write(volume_name, device.path, 0, :binary.copy(<<0xEF>>, 2 * @chunk))

      assert {:ok, [{0, _}, {1, _}]} = BlockIndex.range(volume_name, 0, 3)

      assert {:ok, _cost} = BlockBacking.write_zeroes(volume_name, device.path, 0, 2 * @chunk)

      assert {:ok, []} = BlockIndex.range(volume_name, 0, 3)

      assert {:ok, data} = BlockBacking.read(volume_name, device.path, 0, 2 * @block)
      assert data == :binary.copy(<<0>>, 2 * @block)
    end

    test "a whole-device zero-fill stores nothing and punches every extent", %{
      volume_name: volume_name,
      device: device
    } do
      {:ok, _} =
        BlockBacking.write(volume_name, device.path, 0, :binary.copy(<<0xEF>>, 4 * @chunk))

      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :write_zeroes]])

      assert {:ok, cost} = BlockBacking.write_zeroes(volume_name, device.path, 0, 4 * @chunk)

      assert cost == %{chunk_bytes: 0, chunks_rewritten: 0, chunks_replaced: 4}

      assert_receive {[:neonfs, :block, :write_zeroes], ^ref, measurements, _meta}, 1_000
      assert measurements.guest_bytes == 4 * @chunk
      assert measurements.chunk_bytes == 0
      assert measurements.chunks_replaced == 4

      :telemetry.detach(ref)

      assert {:ok, []} = BlockIndex.range(volume_name, 0, 3)
    end

    # A device whose size is not a whole multiple of the extent width ends in
    # a short extent, which a range reaching the end still covers entirely.
    test "a short final extent is punched like any other", %{volume_name: volume_name} do
      size = 2 * @chunk + @block
      other = "ragged-#{:rand.uniform(999_999)}"
      {:ok, _volume} = create_provisioned_volume(other)
      {:ok, ragged} = BlockBacking.create_device(other, "/ragged.img", size)

      {:ok, _} = BlockBacking.write(other, ragged.path, 0, :binary.copy(<<0xEF>>, size))

      assert {:ok, cost} = BlockBacking.write_zeroes(other, ragged.path, 0, size)

      assert cost == %{chunk_bytes: 0, chunks_rewritten: 0, chunks_replaced: 3}

      assert {:ok, []} = BlockIndex.range(other, 0, 2)
      assert {:ok, data} = BlockBacking.read(other, ragged.path, 2 * @chunk, @block)
      assert data == :binary.copy(<<0>>, @block)
    end

    test "a partial extent at each end is read-modify-written, the middle punched", %{
      volume_name: volume_name,
      device: device
    } do
      {:ok, _} =
        BlockBacking.write(volume_name, device.path, 0, :binary.copy(<<0xEF>>, 4 * @chunk))

      # Straddles extent 0's tail and extent 3's head, covering 1 and 2 whole.
      offset = @chunk - @block
      length = 2 * @chunk + 2 * @block

      assert {:ok, cost} =
               BlockBacking.write_zeroes(volume_name, device.path, offset, length)

      assert cost == %{chunk_bytes: 2 * @chunk, chunks_rewritten: 2, chunks_replaced: 2}

      assert {:ok, [{0, _}, {3, _}]} = BlockIndex.range(volume_name, 0, 3)

      assert {:ok, zeroed} = BlockBacking.read(volume_name, device.path, offset, length)
      assert zeroed == :binary.copy(<<0>>, length)

      assert {:ok, kept_head} = BlockBacking.read(volume_name, device.path, 0, @block)
      assert kept_head == :binary.copy(<<0xEF>>, @block)

      assert {:ok, kept_tail} =
               BlockBacking.read(volume_name, device.path, offset + length, @block)

      assert kept_tail == :binary.copy(<<0xEF>>, @block)
    end

    test "an extent zeroed a block at a time ends up punched, not stored", %{
      volume_name: volume_name,
      device: device
    } do
      {:ok, _} =
        BlockBacking.write(volume_name, device.path, 0, :binary.copy(<<0xEF>>, @chunk))

      for offset <- 0..(div(@chunk, @block) - 1) do
        assert {:ok, _cost} =
                 BlockBacking.discard(volume_name, device.path, offset * @block, @block)
      end

      assert {:ok, []} = BlockIndex.range(volume_name, 0, 0)
    end

    test "a sub-extent discard zero-fills without disturbing its neighbours", %{
      volume_name: volume_name,
      device: device
    } do
      {:ok, _} =
        BlockBacking.write(volume_name, device.path, 0, :binary.copy(<<0xEF>>, @chunk))

      assert {:ok, cost} = BlockBacking.discard(volume_name, device.path, 0, @block)

      # A 4 KiB discard covers no extent end to end, so it buys nothing the
      # equivalent write would not have cost.
      assert cost == %{chunk_bytes: @chunk, chunks_rewritten: 1, chunks_replaced: 0}

      assert {:ok, discarded} = BlockBacking.read(volume_name, device.path, 0, @block)
      assert discarded == :binary.copy(<<0>>, @block)

      assert {:ok, kept} = BlockBacking.read(volume_name, device.path, @block, @block)
      assert kept == :binary.copy(<<0xEF>>, @block)
    end
  end

  # What an interface node needs to move the bytes itself. The refs describe
  # the map; the bytes never cross distribution.
  describe "read_refs/4" do
    # A block volume stores its device uncompressed, which is what lets the
    # caller pull an extent's chunk over the data plane and check it against
    # its own hash. The shared volume in this file compresses, so these use
    # their own.
    setup do
      name = "refs-#{:rand.uniform(999_999)}"

      {:ok, volume} =
        NeonFS.Core.create_volume(name, type: :block, max_size: 4 * @chunk)

      {:ok, device} = BlockBacking.open_device(name, BlockBacking.device_path())
      {:ok, volume: volume, volume_name: name, device: device}
    end

    test "reports an unwritten extent as a hole rather than omitting it", %{
      volume_name: volume_name,
      device: device
    } do
      assert {:ok, %{chunk_bytes: @chunk, size: size, extents: extents}} =
               BlockBacking.read_refs(volume_name, device.path, 0, 2 * @chunk)

      assert size == 4 * @chunk
      assert [first, second] = extents
      assert first.index == 0
      assert first.target == :hole
      assert first.width == @chunk
      assert first.read_start == 0
      assert first.read_length == @chunk
      assert second.index == 1
      assert second.target == :hole
    end

    test "names the chunk an extent resolves to, and where to fetch it", %{
      volume: volume,
      volume_name: volume_name,
      device: device
    } do
      {:ok, _} =
        BlockBacking.write(volume_name, device.path, 0, :binary.copy(<<0x2B>>, @chunk))

      assert {:ok, %{extents: [ref]}} =
               BlockBacking.read_refs(volume_name, device.path, 0, @block)

      assert {:chunk, hash} = ref.target
      assert ref.read_start == 0
      assert ref.read_length == @block
      assert ref.width == @chunk
      refute ref.encrypted
      assert ref.compression == :none

      # The locations are what the caller dials, so they have to be the
      # chunk's real ones rather than a placeholder.
      assert {:ok, meta} = ChunkIndex.get(volume.id, hash)
      assert ref.locations == meta.locations
      refute ref.locations == []
    end

    test "clips the first and last extents to the range", %{
      volume_name: volume_name,
      device: device
    } do
      assert {:ok, %{extents: [first, last]}} =
               BlockBacking.read_refs(volume_name, device.path, @chunk - @block, 2 * @block)

      assert first.index == 0
      assert first.read_start == @chunk - @block
      assert first.read_length == @block
      assert last.index == 1
      assert last.read_start == 0
      assert last.read_length == @block
    end

    test "refuses a range past the end of the device", %{
      volume_name: volume_name,
      device: device
    } do
      assert {:error, {:out_of_range, _offset, _length, _size}} =
               BlockBacking.read_refs(volume_name, device.path, 4 * @chunk - @block, 2 * @block)
    end

    # A compressed chunk's stored bytes do not hash to its id, so the caller
    # cannot verify what the data plane hands it and has to ask core instead.
    # It can only know that from the ref.
    test "says so when an extent's chunk cannot be served by the data plane" do
      name = "refs-zstd-#{:rand.uniform(999_999)}"
      {:ok, _volume} = create_provisioned_volume(name)
      {:ok, device} = BlockBacking.create_device(name, "/compressed.img", 2 * @chunk)

      {:ok, _} = BlockBacking.write(name, device.path, 0, :binary.copy(<<0x11>>, @chunk))

      assert {:ok, %{extents: [ref]}} = BlockBacking.read_refs(name, device.path, 0, @block)

      assert ref.compression == :zstd
    end
  end

  # The inverse half of `write/5`: the caller placed the bytes and reports
  # where, and this only has to check the claim and publish the map.
  describe "commit_written/4" do
    setup %{volume_name: volume_name} do
      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", 4 * @chunk)
      {:ok, device: device}
    end

    test "publishes extents whose chunks are really where the writer said", %{
      volume: volume,
      volume_name: volume_name,
      device: device
    } do
      payload = :binary.copy(<<0x3C>>, @chunk)
      {:ok, hash} = stage_chunk(volume, payload)

      assert {:ok, %{chunks_published: 1}} =
               BlockBacking.commit_written(volume_name, device.path, [{2, hash}],
                 locations: %{hash => [local_location()]},
                 chunk_codecs: %{hash => %{compression: :none, crypto: nil}}
               )

      assert {:ok, ^payload} =
               BlockBacking.read(volume_name, device.path, 2 * @chunk, @chunk)

      assert {:ok, chunk_meta} = ChunkIndex.get(volume.id, hash)
      assert chunk_meta.commit_state == :committed
    end

    # The writer's report is the very thing in doubt when a chunk is
    # missing, so a map published on it would name data that is not there.
    test "refuses to publish a chunk no reported location holds", %{
      volume_name: volume_name,
      device: device
    } do
      absent = :crypto.hash(:sha256, "never written")

      assert {:error, {:missing_chunk, ^absent}} =
               BlockBacking.commit_written(volume_name, device.path, [{0, absent}],
                 locations: %{absent => [local_location()]},
                 chunk_codecs: %{}
               )

      assert {:ok, []} = BlockIndex.range(volume_name, 0, 3)
    end

    test "punches an extent whose target is a hole", %{
      volume: volume,
      volume_name: volume_name,
      device: device
    } do
      {:ok, hash} = stage_chunk(volume, :binary.copy(<<0x4D>>, @chunk))

      {:ok, _} =
        BlockBacking.commit_written(volume_name, device.path, [{0, hash}],
          locations: %{hash => [local_location()]},
          chunk_codecs: %{}
        )

      assert {:ok, [{0, _}]} = BlockIndex.range(volume_name, 0, 3)

      assert {:ok, _} = BlockBacking.commit_written(volume_name, device.path, [{0, :hole}])
      assert {:ok, []} = BlockIndex.range(volume_name, 0, 3)
    end

    test "refuses an extent index the device does not have", %{
      volume_name: volume_name,
      device: device
    } do
      assert {:error, {:extent_out_of_range, 4, 3}} =
               BlockBacking.commit_written(volume_name, device.path, [{4, :hole}])
    end

    test "refuses a commit whose expectation has moved", %{
      volume: volume,
      volume_name: volume_name,
      device: device
    } do
      {:ok, first} = stage_chunk(volume, :binary.copy(<<0x5E>>, @chunk))
      {:ok, second} = stage_chunk(volume, :binary.copy(<<0x6F>>, @chunk))

      {:ok, _} =
        BlockBacking.commit_written(volume_name, device.path, [{0, first}],
          locations: %{first => [local_location()]},
          chunk_codecs: %{}
        )

      assert {:error, :stale_chunks} =
               BlockBacking.commit_written(volume_name, device.path, [{0, second}],
                 locations: %{second => [local_location()]},
                 chunk_codecs: %{},
                 expect: [{0, :hole}]
               )
    end

    test "refuses a commit from a preempted holder", %{
      volume: volume,
      volume_name: volume_name,
      device: device
    } do
      {:ok, hash} = stage_chunk(volume, :binary.copy(<<0x7A>>, @chunk))
      {:ok, attached} = BlockBacking.open_device(volume_name, device.path)
      {:ok, preemptor} = BlockEpoch.bump({volume.id, device.path})

      assert {:error, {:fenced, ^preemptor}} =
               BlockBacking.commit_written(volume_name, device.path, [{0, hash}],
                 locations: %{hash => [local_location()]},
                 chunk_codecs: %{},
                 epoch: attached.epoch
               )
    end
  end

  describe "flush/2" do
    test "returns once the device's chunks are durable", %{volume_name: volume_name} do
      {:ok, device} = BlockBacking.create_device(volume_name, "/dev.img", @chunk)
      {:ok, _} = BlockBacking.write(volume_name, device.path, 0, :binary.copy(<<7>>, @block))

      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :block, :flush]])

      assert :ok = BlockBacking.flush(volume_name, device.path)
      assert_receive {[:neonfs, :block, :flush], ^ref, _measurements, %{status: :ok}}, 1_000
    end
  end

  describe "open_device/2" do
    test "carries the device's current epoch", %{volume: volume, volume_name: volume_name} do
      {:ok, _device} = BlockBacking.create_device(volume_name, "/dev.img", @chunk)

      assert {:ok, first} = BlockBacking.open_device(volume_name, "/dev.img")
      assert first.epoch == 0

      {:ok, bumped} = BlockEpoch.bump({volume.id, "/dev.img"})

      assert {:ok, second} = BlockBacking.open_device(volume_name, "/dev.img")
      assert second.epoch == bumped
    end

    test "refuses a name the volume's device does not answer to", %{volume_name: volume_name} do
      {:ok, _device} = BlockBacking.create_device(volume_name, "/dev.img", @chunk)

      assert {:error, {:device_path_mismatch, "/other.img", "/dev.img"}} =
               BlockBacking.open_device(volume_name, "/other.img")
    end

    # A device from before the extent map is a file with a chunk list. There
    # is no conversion, so the refusal has to say which of the two it is.
    test "names a file-backed device as the reason rather than reporting it absent", %{
      volume_name: volume_name
    } do
      {:ok, _meta} =
        NeonFS.Core.write_file_streamed(volume_name, "/dev.img", [:binary.copy(<<0>>, @block)])

      assert {:error, {:file_backed_device, ^volume_name, "/dev.img"}} =
               BlockBacking.open_device(volume_name, "/dev.img")
    end
  end

  # Puts a chunk on this node's blob store the way an interface node would
  # over the data plane, so `commit_written/4` has something real to verify.
  defp stage_chunk(volume, data) do
    hash = :crypto.hash(:sha256, data)

    case NeonFS.Core.BlobStore.write_chunk(data, "default", "hot") do
      {:ok, ^hash, _info} -> {:ok, hash}
      other -> other
    end
  end

  defp local_location, do: %{node: node(), drive_id: "default", tier: :hot}
end
