defmodule NeonFS.Core.Blob.NativeIndexTreeTest do
  use ExUnit.Case

  alias NeonFS.Core.Blob.Native

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    {:ok, store} = Native.store_open(tmp_dir, 2)
    {:ok, store: store}
  end

  describe "index_tree_get/4" do
    test "returns {:ok, nil} for an empty tree (root_hash = <<>>)", %{store: store} do
      assert {:ok, nil} = Native.index_tree_get(store, <<>>, "hot", "any-key")
    end

    test "returns {:error, _} for an invalid tier", %{store: store} do
      assert {:error, _} = Native.index_tree_get(store, <<>>, "lukewarm", "any-key")
    end

    test "returns {:error, _} for a malformed root_hash (not 0 or 32 bytes)",
         %{store: store} do
      assert {:error, _} = Native.index_tree_get(store, <<1, 2, 3>>, "hot", "key")
    end

    test "returns {:ok, nil} when the root chunk isn't present in the store",
         %{store: store} do
      # 32 bytes of zeroes — well-formed hash, but no such chunk written.
      bogus_root = :binary.copy(<<0>>, 32)

      assert {:error, message} =
               Native.index_tree_get(store, bogus_root, "hot", "any-key")

      assert message =~ "missing chunk"
    end
  end

  describe "index_tree_range/5" do
    test "returns {:ok, []} for an empty tree", %{store: store} do
      assert {:ok, []} = Native.index_tree_range(store, <<>>, "hot", <<>>, <<>>)
    end

    test "returns {:error, _} for an invalid tier", %{store: store} do
      assert {:error, _} = Native.index_tree_range(store, <<>>, "icy", <<>>, <<>>)
    end
  end
end
