defmodule NeonFS.Core.Blob.NativeMetadataTest do
  use ExUnit.Case

  alias NeonFS.Core.Blob.Native

  @segment_hex String.duplicate("aa", 32)

  setup do
    tmp_dir = Path.join(System.tmp_dir!(), "neonfs_meta_nif_test_#{:rand.uniform(1_000_000)}")
    {:ok, store} = Native.store_open(tmp_dir, 2)
    on_exit(fn -> File.rm_rf!(tmp_dir) end)
    {:ok, store: store, tmp_dir: tmp_dir}
  end

  describe "metadata_write/4 and metadata_read/3" do
    test "write then read roundtrip", %{store: store} do
      key_hash = :crypto.hash(:sha256, "chunk:abc123")
      data = "some serialised metadata"

      assert {:ok, {}} = Native.metadata_write(store, @segment_hex, key_hash, data)
      assert {:ok, ^data} = Native.metadata_read(store, @segment_hex, key_hash)
    end

    test "read nonexistent returns error", %{store: store} do
      key_hash = :crypto.hash(:sha256, "nonexistent")

      assert {:error, reason} = Native.metadata_read(store, @segment_hex, key_hash)
      assert reason =~ "metadata not found"
    end

    test "overwrite updates value", %{store: store} do
      key_hash = :crypto.hash(:sha256, "overwrite_key")

      assert {:ok, {}} = Native.metadata_write(store, @segment_hex, key_hash, "version_1")
      assert {:ok, {}} = Native.metadata_write(store, @segment_hex, key_hash, "version_2")

      assert {:ok, "version_2"} = Native.metadata_read(store, @segment_hex, key_hash)
    end

    test "binary data with null bytes", %{store: store} do
      key_hash = :crypto.hash(:sha256, "binary_key")
      data = <<0, 1, 2, 0, 255, 254, 0, 128>>

      assert {:ok, {}} = Native.metadata_write(store, @segment_hex, key_hash, data)
      assert {:ok, ^data} = Native.metadata_read(store, @segment_hex, key_hash)
    end

    test "large records", %{store: store} do
      key_hash = :crypto.hash(:sha256, "large_key")
      data = :binary.copy(<<42>>, 100_000)

      assert {:ok, {}} = Native.metadata_write(store, @segment_hex, key_hash, data)
      assert {:ok, ^data} = Native.metadata_read(store, @segment_hex, key_hash)
    end

    test "empty data", %{store: store} do
      key_hash = :crypto.hash(:sha256, "empty_key")

      assert {:ok, {}} = Native.metadata_write(store, @segment_hex, key_hash, "")
      assert {:ok, ""} = Native.metadata_read(store, @segment_hex, key_hash)
    end
  end

  describe "metadata_delete/3" do
    test "delete existing key", %{store: store} do
      key_hash = :crypto.hash(:sha256, "to_delete")

      assert {:ok, {}} = Native.metadata_write(store, @segment_hex, key_hash, "data")
      assert {:ok, {}} = Native.metadata_delete(store, @segment_hex, key_hash)
      assert {:error, _} = Native.metadata_read(store, @segment_hex, key_hash)
    end

    test "delete nonexistent returns error", %{store: store} do
      key_hash = :crypto.hash(:sha256, "missing")

      assert {:error, reason} = Native.metadata_delete(store, @segment_hex, key_hash)
      assert reason =~ "metadata not found"
    end
  end

  describe "metadata_list_segment/2" do
    test "lists keys in segment", %{store: store} do
      key1 = :crypto.hash(:sha256, "key_one")
      key2 = :crypto.hash(:sha256, "key_two")
      key3 = :crypto.hash(:sha256, "key_three")

      assert {:ok, {}} = Native.metadata_write(store, @segment_hex, key1, "val1")
      assert {:ok, {}} = Native.metadata_write(store, @segment_hex, key2, "val2")
      assert {:ok, {}} = Native.metadata_write(store, @segment_hex, key3, "val3")

      assert {:ok, hashes} = Native.metadata_list_segment(store, @segment_hex)
      assert length(hashes) == 3

      sorted = Enum.sort(hashes)
      expected = Enum.sort([key1, key2, key3])
      assert sorted == expected
    end

    test "empty segment returns empty list", %{store: store} do
      empty_segment = String.duplicate("bb", 32)
      assert {:ok, []} = Native.metadata_list_segment(store, empty_segment)
    end

    test "different segments are independent", %{store: store} do
      seg_a = String.duplicate("cc", 32)
      seg_b = String.duplicate("dd", 32)
      key = :crypto.hash(:sha256, "shared_key")

      assert {:ok, {}} = Native.metadata_write(store, seg_a, key, "val_a")
      assert {:ok, {}} = Native.metadata_write(store, seg_b, key, "val_b")

      assert {:ok, hashes_a} = Native.metadata_list_segment(store, seg_a)
      assert {:ok, hashes_b} = Native.metadata_list_segment(store, seg_b)

      assert length(hashes_a) == 1
      assert length(hashes_b) == 1
    end
  end
end
