defmodule NeonFS.Core.BlockIndexTest do
  use NeonFS.TestCase, async: false

  alias NeonFS.Core.BlockIndex
  alias NeonFS.Core.Volume.BlockDevice
  alias NeonFS.Core.Volume.BlockExtent

  @volume_name "block-index-test-vol"
  @volume_id "0123456789abcdef0123456789abcdef"
  @hash :crypto.hash(:sha256, "extent-0")

  defmodule StubCommitter do
    @moduledoc false

    # Runs the precondition the way the real worker does, so a test of the
    # compare-and-swap is a test of the contract rather than of the stub.
    def commit(volume_id, mutations, opts) do
      case Keyword.get(opts, :precondition, fn -> :ok end).() do
        :ok ->
          send(self(), {:committed, volume_id, mutations, opts})
          {:ok, %{0 => "root"}}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defmodule StubReader do
    @moduledoc false
    def get(_volume_id, kind, key, _opts) do
      case Process.get({:entry, kind, key}) do
        nil -> {:error, :not_found}
        entry -> {:ok, entry}
      end
    end

    def range(_volume_id, _kind, _start_key, _end_key, _opts) do
      {:ok, Process.get(:range, [])}
    end
  end

  setup do
    start_volume_registry()
    :ok
  end

  # The registry's own create path goes through Ra; these tests only need
  # the name→volume lookup `BlockIndex` resolves through, so seed its ETS
  # directly rather than standing up consensus for it.
  defp register_volume(chunk_bytes \\ 131_072) do
    volume = %NeonFS.Core.Volume{
      id: @volume_id,
      name: @volume_name,
      type: :block,
      block_chunk_bytes: chunk_bytes
    }

    :ets.insert(:volumes_by_id, {volume.id, volume})
    :ets.insert(:volumes_by_name, {volume.name, volume.id})
    volume
  end

  defp opts do
    [volume_committer: StubCommitter, metadata_reader: StubReader]
  end

  # A partitioned holder still believes it owns the device. The epoch is what
  # turns its next commit into a refusal rather than a write.
  describe "commit/3 fencing" do
    test "a commit stamped with the current epoch is published" do
      register_volume()

      assert {:ok, _roots} =
               BlockIndex.commit(
                 @volume_name,
                 [{0, {:chunk, @hash}}],
                 opts() ++
                   [
                     epoch: 3,
                     device_path: "/dev.img",
                     epoch_checker: fn {@volume_id, "/dev.img"}, 3 -> :ok end
                   ]
               )

      assert_received {:committed, @volume_id, _mutations, _opts}
    end

    test "a commit from a preempted holder is refused, and nothing is published" do
      register_volume()

      assert {:error, {:fenced, 4}} =
               BlockIndex.commit(
                 @volume_name,
                 [{0, {:chunk, @hash}}],
                 opts() ++
                   [
                     epoch: 3,
                     device_path: "/dev.img",
                     epoch_checker: fn _key, 3 -> {:error, {:fenced, 4}} end
                   ]
               )

      refute_received {:committed, _, _, _}
    end

    # GC, repair and provisioning hold no device, so they commit unfenced —
    # and must not pay a consensus read to do it.
    test "a commit with no epoch is not checked" do
      register_volume()

      assert {:ok, _roots} =
               BlockIndex.commit(
                 @volume_name,
                 [{0, {:chunk, @hash}}],
                 opts() ++
                   [epoch_checker: fn _key, _epoch -> flunk("checked an unfenced commit") end]
               )

      assert_received {:committed, @volume_id, _mutations, _opts}
    end

    # An epoch without the device it belongs to would silently fence nothing,
    # which is worse than refusing: the caller believes it is protected.
    test "an epoch without a device path is refused" do
      register_volume()

      assert {:error, :epoch_without_device_path} =
               BlockIndex.commit(@volume_name, [{0, {:chunk, @hash}}], opts() ++ [epoch: 3])
    end

    # The injected keys are test seams and a `VolumeCommitter` option list is
    # not the place for them.
    test "fencing options do not reach the committer" do
      register_volume()

      assert {:ok, _roots} =
               BlockIndex.commit(
                 @volume_name,
                 [{0, {:chunk, @hash}}],
                 opts() ++
                   [epoch: 1, device_path: "/dev.img", epoch_checker: fn _key, _epoch -> :ok end]
               )

      assert_received {:committed, @volume_id, _mutations, committer_opts}
      refute Keyword.has_key?(committer_opts, :epoch)
      refute Keyword.has_key?(committer_opts, :device_path)
      refute Keyword.has_key?(committer_opts, :epoch_checker)
    end
  end

  describe "commit/3" do
    test "publishes every extent as one batch of mutations" do
      register_volume()

      other = :crypto.hash(:sha256, "extent-1")

      assert {:ok, _roots} =
               BlockIndex.commit(
                 @volume_name,
                 [{0, {:chunk, @hash}}, {1, {:chunk, other}}],
                 opts()
               )

      assert_received {:committed, @volume_id, mutations, _opts}
      assert length(mutations) == 2

      assert [
               {:put, :block_index, key_0, entry_0},
               {:put, :block_index, key_1, _entry_1}
             ] = mutations

      assert BlockExtent.extent_index(key_0) == 0
      assert BlockExtent.extent_index(key_1) == 1
      assert {:ok, {:chunk, @hash}} = BlockExtent.decode(entry_0)
    end

    test "a :hole target drops the extent rather than writing an entry" do
      register_volume()

      assert {:ok, _roots} = BlockIndex.commit(@volume_name, [{7, :hole}], opts())

      assert_received {:committed, @volume_id, [{:delete, :block_index, key}], _opts}
      assert BlockExtent.extent_index(key) == 7
    end

    test "a batch may mix writes and punches" do
      register_volume()

      assert {:ok, _roots} =
               BlockIndex.commit(@volume_name, [{0, {:chunk, @hash}}, {1, :hole}], opts())

      assert_received {:committed, @volume_id, mutations, _opts}
      assert [{:put, :block_index, _, _}, {:delete, :block_index, _}] = mutations
    end

    test "a stripe member commits as a stripe target" do
      register_volume()
      stripe_id = UUIDv7.generate()

      assert {:ok, _roots} =
               BlockIndex.commit(@volume_name, [{3, {:stripe, stripe_id, 2}}], opts())

      assert_received {:committed, @volume_id, [{:put, :block_index, _key, entry}], _opts}
      assert {:ok, {:stripe, ^stripe_id, 2}} = BlockExtent.decode(entry)
    end

    test "does not leak the injection opts into the writer" do
      register_volume()

      assert {:ok, _roots} =
               BlockIndex.commit(@volume_name, [{0, {:chunk, @hash}}, {1, :hole}], opts())

      assert_received {:committed, @volume_id, _mutations, writer_opts}
      refute Keyword.has_key?(writer_opts, :volume_committer)
      refute Keyword.has_key?(writer_opts, :metadata_reader)
    end

    test "an unknown volume is refused before anything is published" do
      assert {:error, _} = BlockIndex.commit("no-such-volume", [{0, {:chunk, @hash}}], opts())
      refute_received {:committed, _, _, _}
    end
  end

  describe "discard/4" do
    test "drops every extent in the range in one commit" do
      register_volume()

      assert {:ok, _roots} = BlockIndex.discard(@volume_name, 4, 6, opts())

      assert_received {:committed, @volume_id, mutations, _opts}

      assert Enum.map(mutations, fn {:delete, :block_index, key} ->
               BlockExtent.extent_index(key)
             end) == [4, 5, 6]
    end
  end

  describe "get/3" do
    test "returns the extent's target" do
      register_volume()
      Process.put({:entry, :block_index, BlockExtent.key(2)}, BlockExtent.encode({:chunk, @hash}))

      assert {:ok, {:chunk, @hash}} = BlockIndex.get(@volume_name, 2, opts())
    end

    test "an extent with no entry is a hole" do
      register_volume()

      assert {:ok, :hole} = BlockIndex.get(@volume_name, 99, opts())
    end
  end

  describe "range/4" do
    test "decodes entries back into extent indices, ascending" do
      register_volume()

      Process.put(:range, [
        {BlockExtent.key(4), BlockExtent.encode({:chunk, @hash})},
        {BlockExtent.key(9), BlockExtent.encode({:chunk, @hash})}
      ])

      assert {:ok, [{4, {:chunk, @hash}}, {9, {:chunk, @hash}}]} =
               BlockIndex.range(@volume_name, 0, 10, opts())
    end

    test "surfaces a malformed entry rather than skipping it" do
      register_volume()
      Process.put(:range, [{BlockExtent.key(1), <<9::8, 0::256>>}])

      assert {:error, {:malformed_extent, {:unknown_kind, 9}}} =
               BlockIndex.range(@volume_name, 0, 5, opts())
    end
  end

  # A sub-extent write reads a whole extent, splices its bytes in and writes
  # the extent back, so two of them against one extent both start from the
  # same value and the later commit discards the earlier one's bytes.
  describe "commit/3 compare-and-swap" do
    test "a commit whose expectation still holds is published" do
      register_volume()
      Process.put({:entry, :block_index, BlockExtent.key(4)}, BlockExtent.encode({:chunk, @hash}))

      assert {:ok, _roots} =
               BlockIndex.commit(
                 @volume_name,
                 [{4, {:chunk, @hash}}],
                 opts() ++ [expect: [{4, {:chunk, @hash}}]]
               )

      assert_received {:committed, @volume_id, _mutations, _opts}
    end

    test "a commit whose extent moved under it is refused, and nothing is published" do
      register_volume()
      moved = :crypto.hash(:sha256, "someone-else")
      Process.put({:entry, :block_index, BlockExtent.key(4)}, BlockExtent.encode({:chunk, moved}))

      assert {:error, :stale_chunks} =
               BlockIndex.commit(
                 @volume_name,
                 [{4, {:chunk, @hash}}],
                 opts() ++ [expect: [{4, {:chunk, @hash}}]]
               )

      refute_received {:committed, @volume_id, _mutations, _opts}
    end

    # An extent a caller read as a hole and someone else has since filled is
    # the same race, and the absent entry must not read as "unchanged".
    test "an expectation of a hole is refused once the extent exists" do
      register_volume()
      Process.put({:entry, :block_index, BlockExtent.key(4)}, BlockExtent.encode({:chunk, @hash}))

      assert {:error, :stale_chunks} =
               BlockIndex.commit(
                 @volume_name,
                 [{4, {:chunk, @hash}}],
                 opts() ++ [expect: [{4, :hole}]]
               )
    end

    test "an extent the caller never read is not compared, so disjoint writers do not collide" do
      register_volume()

      assert {:ok, _roots} =
               BlockIndex.commit(@volume_name, [{4, {:chunk, @hash}}], opts() ++ [expect: []])

      assert_received {:committed, @volume_id, _mutations, opts}
      refute Keyword.has_key?(opts, :precondition)
    end
  end

  describe "device header" do
    test "round-trips through put_device/3 and get_device/2" do
      register_volume()

      device =
        BlockDevice.new(
          id: "dev-1",
          path: "/dev.img",
          size_bytes: 8 * 1024 * 1024,
          chunk_bytes: 131_072
        )

      assert {:ok, %{0 => "root"}} = BlockIndex.put_device(@volume_name, device, opts())
      assert_receive {:committed, @volume_id, [{:put, :block_index, key, encoded}], _opts}
      assert key == BlockDevice.key()

      Process.put({:entry, :block_index, BlockDevice.key()}, encoded)

      assert {:ok, read_back} = BlockIndex.get_device(@volume_name, opts())
      assert read_back.id == "dev-1"
      assert read_back.size_bytes == 8 * 1024 * 1024
      assert read_back.chunk_bytes == 131_072
    end

    test "reports a volume with no header rather than inventing geometry" do
      register_volume()

      assert {:error, :not_found} = BlockIndex.get_device(@volume_name, opts())
    end

    # The header's key is chosen so no extent index can produce it. Were it a
    # sentinel integer, this is where an off-by-one in a bound would clobber it.
    test "the header key is not an extent key" do
      refute BlockExtent.extent_key?(BlockDevice.key())
      assert BlockExtent.extent_key?(BlockExtent.key(0))
      assert BlockExtent.extent_key?(BlockExtent.key(0xFFFFFFFFFFFFFFFF))
    end
  end

  describe "iteration excludes the device header" do
    # `range/3` decodes every entry it is handed as a fixed-width extent, so a
    # header reaching the decoder is a malformed-entry error rather than a
    # skipped row — which is what makes this a filter and not an optimisation.
    test "range/4 skips it" do
      register_volume()

      device =
        BlockDevice.new(id: "dev-1", path: "/dev.img", size_bytes: 1024, chunk_bytes: 131_072)

      Process.put(:range, [
        {BlockDevice.key(), BlockDevice.encode(device)},
        {BlockExtent.key(4), BlockExtent.encode({:chunk, @hash})}
      ])

      assert {:ok, [{4, {:chunk, @hash}}]} = BlockIndex.range(@volume_name, 0, 10, opts())
    end

    # Without this filter GC resolves the header as a chunk target — either
    # failing the mark phase outright, or treating the device's own geometry as
    # a chunk hash it should keep.
    test "referenced_targets/2 skips it" do
      register_volume()

      device =
        BlockDevice.new(id: "dev-1", path: "/dev.img", size_bytes: 1024, chunk_bytes: 131_072)

      Process.put(:range, [
        {BlockDevice.key(), BlockDevice.encode(device)},
        {BlockExtent.key(0), BlockExtent.encode({:chunk, @hash})}
      ])

      assert {:ok, %{chunks: chunks, stripes: stripes}} =
               BlockIndex.referenced_targets(@volume_name, opts())

      assert MapSet.to_list(chunks) == [@hash]
      assert MapSet.size(stripes) == 0
    end
  end

  describe "read_extent/3" do
    test "a hole reads as the volume's extent width in zeroes" do
      register_volume(65_536)

      assert {:ok, zeroes} = BlockIndex.read_extent(@volume_name, 12, opts())
      assert byte_size(zeroes) == 65_536
      assert zeroes == <<0::size(65_536)-unit(8)>>
    end

    test "a chunk extent reads its chunk's bytes" do
      register_volume()
      Process.put({:entry, :block_index, BlockExtent.key(1)}, BlockExtent.encode({:chunk, @hash}))

      reader = fn _volume_id, hash, _opts ->
        assert hash == @hash
        {:ok, "extent bytes"}
      end

      assert {:ok, "extent bytes"} =
               BlockIndex.read_extent(@volume_name, 1, opts() ++ [chunk_reader: reader])
    end

    test "an erasure extent is refused rather than answered with wrong bytes" do
      register_volume()

      Process.put(
        {:entry, :block_index, BlockExtent.key(1)},
        BlockExtent.encode({:stripe, UUIDv7.generate(), 0})
      )

      assert {:error, :erasure_extent_unsupported} =
               BlockIndex.read_extent(@volume_name, 1, opts())
    end
  end
end
