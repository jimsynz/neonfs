defmodule NeonFS.Core.Volume.BlockExtentTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Volume.BlockExtent

  @hash :crypto.hash(:sha256, "extent")

  describe "key/1 and extent_index/1" do
    test "round-trips an extent index" do
      for extent <- [0, 1, 255, 65_536, 1_000_000_000] do
        assert extent |> BlockExtent.key() |> BlockExtent.extent_index() == extent
      end
    end

    test "keys are fixed width" do
      assert byte_size(BlockExtent.key(0)) == 8
      assert byte_size(BlockExtent.key(1_000_000_000)) == 8
    end

    test "byte order matches LBA order, so a range scan is a contiguous walk" do
      keys = Enum.map([0, 1, 2, 255, 256, 1_000_000], &BlockExtent.key/1)
      assert Enum.sort(keys) == keys
    end
  end

  describe "extent_index_at/2" do
    test "divides the offset by the volume's chunk size" do
      assert BlockExtent.extent_index_at(0, 131_072) == 0
      assert BlockExtent.extent_index_at(131_071, 131_072) == 0
      assert BlockExtent.extent_index_at(131_072, 131_072) == 1
      assert BlockExtent.extent_index_at(262_143, 131_072) == 1
    end

    test "tracks a different per-volume chunk size" do
      assert BlockExtent.extent_index_at(131_072, 65_536) == 2
    end
  end

  describe "encode/1 and decode/1" do
    test "every target encodes to the same fixed width" do
      for target <- [:hole, {:chunk, @hash}, {:stripe, UUIDv7.generate(), 3}] do
        assert byte_size(BlockExtent.encode(target)) == BlockExtent.entry_size()
      end
    end

    test "round-trips a hole" do
      assert {:ok, :hole} = :hole |> BlockExtent.encode() |> BlockExtent.decode()
    end

    test "round-trips a chunk target" do
      assert {:ok, {:chunk, @hash}} =
               {:chunk, @hash} |> BlockExtent.encode() |> BlockExtent.decode()
    end

    test "round-trips a stripe member, returning the text UUID it was given" do
      stripe_id = UUIDv7.generate()

      assert {:ok, {:stripe, ^stripe_id, 7}} =
               {:stripe, stripe_id, 7} |> BlockExtent.encode() |> BlockExtent.decode()
    end

    test "round-trips the widest member index" do
      stripe_id = UUIDv7.generate()

      assert {:ok, {:stripe, ^stripe_id, 65_535}} =
               {:stripe, stripe_id, 65_535} |> BlockExtent.encode() |> BlockExtent.decode()
    end

    test "a hole is distinguishable from a chunk of zeroes" do
      zero_chunk = BlockExtent.encode({:chunk, <<0::256>>})

      refute zero_chunk == BlockExtent.encode(:hole)
      assert {:ok, {:chunk, <<0::256>>}} = BlockExtent.decode(zero_chunk)
    end

    test "a chunk target and a stripe target are distinguishable" do
      stripe_id = UUIDv7.generate()

      assert BlockExtent.encode({:chunk, @hash}) != BlockExtent.encode({:stripe, stripe_id, 0})
    end

    test "rejects a truncated entry rather than reading it as a hole" do
      truncated = :binary.part(BlockExtent.encode({:chunk, @hash}), 0, 12)

      assert {:error, {:malformed_extent, {:wrong_size, 12}}} = BlockExtent.decode(truncated)
    end

    test "rejects an unknown target kind" do
      assert {:error, {:malformed_extent, {:unknown_kind, 9}}} =
               BlockExtent.decode(<<9::8, 0::256>>)
    end

    test "rejects a hole carrying a target" do
      assert {:error, {:malformed_extent, :hole_with_target}} =
               BlockExtent.decode(<<0::8, @hash::binary>>)
    end
  end

  describe "group/1" do
    test "groups run in extent order" do
      group_size = BlockExtent.group_size()

      assert BlockExtent.group(0) == 0
      assert BlockExtent.group(group_size - 1) == 0
      assert BlockExtent.group(group_size) == 1
      assert BlockExtent.group(2 * group_size + 1) == 2
    end
  end
end
