defmodule NeonFS.Core.Volume.ShardTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.Volume.{BlockDevice, BlockExtent, Shard}

  # The unit suite pins `metadata_shard_count` to 1, which collapses every
  # mapping onto shard 0 and hides the distinctions these tests are about.
  defp with_shard_count(count, fun) do
    previous = Application.get_env(:neonfs_core, :metadata_shard_count)
    Application.put_env(:neonfs_core, :metadata_shard_count, count)

    try do
      fun.()
    after
      Application.put_env(:neonfs_core, :metadata_shard_count, previous)
    end
  end

  describe "count/0 and all/0" do
    test "all/0 enumerates 0..count-1" do
      assert Shard.all() == Enum.to_list(0..(Shard.count() - 1))
      assert length(Shard.all()) == Shard.count()
    end
  end

  describe "for_key/2 on a hashed index kind" do
    test "returns a shard in range" do
      for kind <- [:file_index, :chunk_index, :stripe_index],
          key <- ["file:abc", "dir:vol:/", "dirent:vol:/" <> <<0>> <> "x", "stripe:s-1"] do
        shard = Shard.for_key(kind, key)
        assert shard >= 0 and shard < Shard.count()
      end
    end

    test "is deterministic for a given key" do
      assert Shard.for_key(:file_index, "file:xyz") == Shard.for_key(:file_index, "file:xyz")
    end

    test "does not depend on the index kind" do
      assert Shard.for_key(:file_index, "k") == Shard.for_key(:chunk_index, "k")
    end
  end

  describe "for_key/2 on :block_index" do
    test "returns a shard in range" do
      with_shard_count(64, fn ->
        for extent <- [0, 1, 63, 64, 1_000, 10_000_000] do
          shard = Shard.for_key(:block_index, BlockExtent.key(extent))
          assert shard >= 0 and shard < Shard.count()
        end
      end)
    end

    test "every extent in a group lands on one shard" do
      with_shard_count(64, fn ->
        group_size = BlockExtent.group_size()
        base = 5 * group_size

        shards =
          base..(base + group_size - 1)
          |> Enum.map(&Shard.for_key(:block_index, BlockExtent.key(&1)))
          |> Enum.uniq()

        assert length(shards) == 1
      end)
    end

    test "adjacent groups land on adjacent shards" do
      with_shard_count(64, fn ->
        group_size = BlockExtent.group_size()

        first = Shard.for_key(:block_index, BlockExtent.key(0))
        second = Shard.for_key(:block_index, BlockExtent.key(group_size))

        assert second == first + 1
      end)
    end

    test "a window straddling a group boundary touches exactly two shards" do
      with_shard_count(64, fn ->
        group_size = BlockExtent.group_size()
        boundary = 3 * group_size

        shards =
          (boundary - 2)..(boundary + 1)
          |> Enum.map(&Shard.for_key(:block_index, BlockExtent.key(&1)))
          |> Enum.uniq()

        assert length(shards) == 2
      end)
    end

    test "a window shorter than a group and inside one touches a single shard" do
      with_shard_count(64, fn ->
        group_size = BlockExtent.group_size()
        base = 3 * group_size

        shards =
          base..(base + div(group_size, 2))
          |> Enum.map(&Shard.for_key(:block_index, BlockExtent.key(&1)))
          |> Enum.uniq()

        assert length(shards) == 1
      end)
    end

    test "does not hash: sequential extents do not scatter" do
      with_shard_count(64, fn ->
        keys = Enum.map(0..255, &BlockExtent.key/1)

        by_group = keys |> Enum.map(&Shard.for_key(:block_index, &1)) |> Enum.uniq() |> length()
        by_hash = keys |> Enum.map(&Shard.for_key(:chunk_index, &1)) |> Enum.uniq() |> length()

        assert by_group == div(256, BlockExtent.group_size())
        assert by_hash > by_group
      end)
    end

    test "wraps onto shard 0 once the group index exceeds the shard count" do
      with_shard_count(4, fn ->
        group_size = BlockExtent.group_size()

        assert Shard.for_key(:block_index, BlockExtent.key(4 * group_size)) ==
                 Shard.for_key(:block_index, BlockExtent.key(0))
      end)
    end
  end

  describe "for_key/2 on the block_index device header" do
    # The header has no extent index, so `BlockExtent.extent_index/1` raises on
    # its key. Without an explicit clause the shard mapping crashes rather than
    # answering — which is why the group is declared rather than derived.
    test "maps the header key to BlockDevice.shard/0" do
      with_shard_count(64, fn ->
        assert Shard.for_key(:block_index, BlockDevice.key()) ==
                 rem(BlockDevice.shard(), 64)
      end)
    end

    test "is stable across shard counts, modulo the count" do
      for count <- [1, 4, 64] do
        with_shard_count(count, fn ->
          shard = Shard.for_key(:block_index, BlockDevice.key())
          assert shard == rem(BlockDevice.shard(), count)
          assert shard < count
        end)
      end
    end

    test "still maps extent keys by their group" do
      with_shard_count(64, fn ->
        group_size = BlockExtent.group_size()

        assert Shard.for_key(:block_index, BlockExtent.key(0)) == 0
        assert Shard.for_key(:block_index, BlockExtent.key(group_size)) == 1
      end)
    end
  end
end
