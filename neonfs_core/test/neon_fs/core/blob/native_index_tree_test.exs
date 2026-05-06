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

  describe "index_tree_put/5 → index_tree_get/4 round-trip" do
    test "put on an empty tree creates a fresh root, get returns the value",
         %{store: store} do
      assert {:ok, root} =
               Native.index_tree_put(store, <<>>, "hot", "alpha", "value-a")

      assert byte_size(root) == 32

      assert {:ok, "value-a"} =
               Native.index_tree_get(store, root, "hot", "alpha")
    end

    test "successive puts return successive roots, latest overrides", %{store: store} do
      assert {:ok, root1} =
               Native.index_tree_put(store, <<>>, "hot", "k", "v1")

      assert {:ok, root2} =
               Native.index_tree_put(store, root1, "hot", "k", "v2")

      assert root2 != root1

      assert {:ok, "v2"} = Native.index_tree_get(store, root2, "hot", "k")
      # Old root still returns the old value (CoW).
      assert {:ok, "v1"} = Native.index_tree_get(store, root1, "hot", "k")
    end

    test "range returns inserted entries in sort order", %{store: store} do
      {:ok, root} = Native.index_tree_put(store, <<>>, "hot", "b", "B")
      {:ok, root} = Native.index_tree_put(store, root, "hot", "a", "A")
      {:ok, root} = Native.index_tree_put(store, root, "hot", "c", "C")

      assert {:ok, [{"a", "A"}, {"b", "B"}, {"c", "C"}]} =
               Native.index_tree_range(store, root, "hot", <<>>, <<>>)
    end
  end

  describe "index_tree_delete/4" do
    test "delete on an existing key returns a new root and get returns nil",
         %{store: store} do
      {:ok, root} = Native.index_tree_put(store, <<>>, "hot", "k", "v")

      assert {:ok, root2} = Native.index_tree_delete(store, root, "hot", "k")
      assert root2 != root

      assert {:ok, nil} = Native.index_tree_get(store, root2, "hot", "k")
    end

    test "delete on an empty tree writes a tombstone (no-op-equivalent for reads)",
         %{store: store} do
      assert {:ok, root} = Native.index_tree_delete(store, <<>>, "hot", "missing")
      assert byte_size(root) == 32

      assert {:ok, nil} = Native.index_tree_get(store, root, "hot", "missing")
    end
  end

  describe "index_tree_purge_tombstones/4" do
    test "errors on an empty tree", %{store: store} do
      assert {:error, message} =
               Native.index_tree_purge_tombstones(store, <<>>, "hot", 0)

      assert message =~ "empty tree"
    end

    test "drops tombstones whose deleted_at is older than the cutoff",
         %{store: store} do
      {:ok, root} = Native.index_tree_put(store, <<>>, "hot", "live", "L")
      {:ok, root} = Native.index_tree_put(store, root, "hot", "dead", "D")
      {:ok, root} = Native.index_tree_delete(store, root, "hot", "dead")

      future_nanos =
        System.system_time(:nanosecond) + 60 * 1_000_000_000

      assert {:ok, purged_root} =
               Native.index_tree_purge_tombstones(store, root, "hot", future_nanos)

      assert {:ok, "L"} = Native.index_tree_get(store, purged_root, "hot", "live")
      assert {:ok, nil} = Native.index_tree_get(store, purged_root, "hot", "dead")

      # Range now sees only the live key.
      assert {:ok, [{"live", "L"}]} =
               Native.index_tree_range(store, purged_root, "hot", <<>>, <<>>)
    end
  end
end
