defmodule NeonFS.NFS.MetadataCacheTest do
  use ExUnit.Case, async: false

  alias NeonFS.NFS.MetadataCache

  setup do
    {:ok, _pid} = start_supervised(MetadataCache)
    table = MetadataCache.table()
    {:ok, table: table}
  end

  describe "attrs cache" do
    test "miss on empty cache", %{table: table} do
      assert :miss = MetadataCache.get_attrs(table, "vol1", "/foo.txt")
    end

    test "hit after put", %{table: table} do
      attrs = %{size: 42, mode: 0o644}
      MetadataCache.put_attrs(table, "vol1", "/foo.txt", attrs)
      assert {:ok, ^attrs} = MetadataCache.get_attrs(table, "vol1", "/foo.txt")
    end

    test "different volumes are isolated", %{table: table} do
      attrs_a = %{size: 1}
      attrs_b = %{size: 2}
      MetadataCache.put_attrs(table, "vol_a", "/file", attrs_a)
      MetadataCache.put_attrs(table, "vol_b", "/file", attrs_b)

      assert {:ok, ^attrs_a} = MetadataCache.get_attrs(table, "vol_a", "/file")
      assert {:ok, ^attrs_b} = MetadataCache.get_attrs(table, "vol_b", "/file")
    end
  end

  describe "dir listing cache" do
    test "miss on empty cache", %{table: table} do
      assert :miss = MetadataCache.get_dir_listing(table, "vol1", "/")
    end

    test "hit after put", %{table: table} do
      entries = [%{"name" => "a"}, %{"name" => "b"}]
      MetadataCache.put_dir_listing(table, "vol1", "/", entries)
      assert {:ok, ^entries} = MetadataCache.get_dir_listing(table, "vol1", "/")
    end
  end

  describe "lookup cache" do
    test "miss on empty cache", %{table: table} do
      assert :miss = MetadataCache.get_lookup(table, "vol1", "/", "readme.txt")
    end

    test "hit after put", %{table: table} do
      result = %{"type" => "lookup", "file_id" => 5}
      MetadataCache.put_lookup(table, "vol1", "/", "readme.txt", result)
      assert {:ok, ^result} = MetadataCache.get_lookup(table, "vol1", "/", "readme.txt")
    end
  end

  describe "invalidate_volume" do
    test "clears all entries for a volume", %{table: table} do
      MetadataCache.put_attrs(table, "vol1", "/a", %{size: 1})
      MetadataCache.put_attrs(table, "vol1", "/b", %{size: 2})
      MetadataCache.put_dir_listing(table, "vol1", "/", [])
      MetadataCache.put_lookup(table, "vol1", "/", "a", %{})

      # Also add entries for another volume
      MetadataCache.put_attrs(table, "vol2", "/x", %{size: 10})

      MetadataCache.invalidate_volume(table, "vol1")

      assert :miss = MetadataCache.get_attrs(table, "vol1", "/a")
      assert :miss = MetadataCache.get_attrs(table, "vol1", "/b")
      assert :miss = MetadataCache.get_dir_listing(table, "vol1", "/")
      assert :miss = MetadataCache.get_lookup(table, "vol1", "/", "a")

      # Other volume unaffected
      assert {:ok, %{size: 10}} = MetadataCache.get_attrs(table, "vol2", "/x")
    end
  end

  describe "graceful handling of invalid table" do
    test "get_attrs returns :miss for bad table" do
      assert :miss = MetadataCache.get_attrs(:nonexistent_table, "v", "/p")
    end

    test "put_attrs is a no-op for bad table" do
      assert :ok = MetadataCache.put_attrs(:nonexistent_table, "v", "/p", %{})
    end

    test "get_dir_listing returns :miss for bad table" do
      assert :miss = MetadataCache.get_dir_listing(:nonexistent_table, "v", "/p")
    end

    test "get_lookup returns :miss for bad table" do
      assert :miss = MetadataCache.get_lookup(:nonexistent_table, "v", "/p", "n")
    end

    test "invalidate_volume is a no-op for bad table" do
      assert :ok = MetadataCache.invalidate_volume(:nonexistent_table, "v")
    end
  end
end
