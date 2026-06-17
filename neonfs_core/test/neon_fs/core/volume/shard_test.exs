defmodule NeonFS.Core.Volume.ShardTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Volume.Shard

  describe "count/0 and all/0" do
    test "all/0 enumerates 0..count-1" do
      assert Shard.all() == Enum.to_list(0..(Shard.count() - 1))
      assert length(Shard.all()) == Shard.count()
    end
  end

  describe "for_key/1" do
    test "returns a shard in range" do
      for key <- ["file:abc", "dir:vol:/", "dirent:vol:/" <> <<0>> <> "x", "stripe:s-1"] do
        shard = Shard.for_key(key)
        assert shard >= 0 and shard < Shard.count()
      end
    end

    test "is deterministic for a given key" do
      assert Shard.for_key("file:xyz") == Shard.for_key("file:xyz")
    end
  end
end
