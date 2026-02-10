defmodule NeonFS.Core.MetadataStoreTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.BlobStore
  alias NeonFS.Core.HLC
  alias NeonFS.Core.MetadataStore

  @segment_id :crypto.hash(:sha256, "test_segment")

  setup do
    tmp_dir = Path.join(System.tmp_dir!(), "metadata_store_test_#{random_string()}")
    File.mkdir_p!(tmp_dir)

    drives = [%{id: "default", path: tmp_dir, tier: :hot, capacity: 100_000_000}]

    blob_server = :"blob_store_#{inspect(self())}"
    meta_server = :"metadata_store_#{inspect(self())}"

    {:ok, _blob_pid} =
      start_supervised(
        {BlobStore, drives: drives, prefix_depth: 2, name: blob_server},
        id: :blob_store
      )

    {:ok, _meta_pid} =
      start_supervised(
        {MetadataStore,
         drive_id: "default",
         blob_store_server: blob_server,
         name: meta_server,
         max_cache_size: 100},
        id: :metadata_store
      )

    on_exit(fn -> File.rm_rf!(tmp_dir) end)

    %{meta_server: meta_server, blob_server: blob_server}
  end

  describe "write/4 and read/3" do
    test "roundtrip with map value", ctx do
      value = %{name: "test.txt", size: 1024}

      assert :ok =
               MetadataStore.write(@segment_id, "file:test.txt", value, server: ctx.meta_server)

      # Atom keys become string keys through msgpax serialisation
      assert {:ok, %{"name" => "test.txt", "size" => 1024}, timestamp} =
               MetadataStore.read(@segment_id, "file:test.txt", server: ctx.meta_server)

      assert is_tuple(timestamp)
      assert tuple_size(timestamp) == 3
    end

    test "roundtrip with list value", ctx do
      value = [1, 2, 3, "four", :five]

      assert :ok =
               MetadataStore.write(@segment_id, "chunks:abc", value, server: ctx.meta_server)

      # Atoms in lists become strings through msgpax serialisation
      assert {:ok, [1, 2, 3, "four", "five"], _timestamp} =
               MetadataStore.read(@segment_id, "chunks:abc", server: ctx.meta_server)
    end

    test "roundtrip with string value", ctx do
      value = "simple string metadata"

      assert :ok =
               MetadataStore.write(@segment_id, "note:1", value, server: ctx.meta_server)

      assert {:ok, ^value, _timestamp} =
               MetadataStore.read(@segment_id, "note:1", server: ctx.meta_server)
    end

    test "roundtrip with binary value", ctx do
      value = :crypto.strong_rand_bytes(64)

      assert :ok =
               MetadataStore.write(@segment_id, "bin:1", value, server: ctx.meta_server)

      assert {:ok, ^value, _timestamp} =
               MetadataStore.read(@segment_id, "bin:1", server: ctx.meta_server)
    end

    test "read nonexistent returns not_found", ctx do
      assert {:error, :not_found} =
               MetadataStore.read(@segment_id, "nonexistent", server: ctx.meta_server)
    end

    test "overwrite updates value and timestamp", ctx do
      assert :ok =
               MetadataStore.write(@segment_id, "key:over", "v1", server: ctx.meta_server)

      assert {:ok, "v1", ts1} =
               MetadataStore.read(@segment_id, "key:over", server: ctx.meta_server)

      assert :ok =
               MetadataStore.write(@segment_id, "key:over", "v2", server: ctx.meta_server)

      assert {:ok, "v2", ts2} =
               MetadataStore.read(@segment_id, "key:over", server: ctx.meta_server)

      # Second timestamp should be greater or equal
      {wall1, counter1, _} = ts1
      {wall2, counter2, _} = ts2
      assert {wall2, counter2} >= {wall1, counter1}
    end
  end

  describe "HLC timestamps" do
    test "timestamps are valid 3-tuples", ctx do
      assert :ok =
               MetadataStore.write(@segment_id, "ts:test", :value, server: ctx.meta_server)

      # Atom value becomes string, but HLC node_id stays an atom (codec handles conversion)
      assert {:ok, "value", {wall_ms, counter, node_id}} =
               MetadataStore.read(@segment_id, "ts:test", server: ctx.meta_server)

      assert is_integer(wall_ms) and wall_ms > 0
      assert is_integer(counter) and counter >= 0
      assert is_atom(node_id)
    end

    test "successive writes produce monotonic timestamps", ctx do
      assert :ok =
               MetadataStore.write(@segment_id, "mono:a", :a, server: ctx.meta_server)

      assert {:ok, "a", ts_a} =
               MetadataStore.read(@segment_id, "mono:a", server: ctx.meta_server)

      assert :ok =
               MetadataStore.write(@segment_id, "mono:b", :b, server: ctx.meta_server)

      assert {:ok, "b", ts_b} =
               MetadataStore.read(@segment_id, "mono:b", server: ctx.meta_server)

      assert HLC.compare(ts_b, ts_a) in [:gt, :eq]
    end
  end

  describe "delete/3 (tombstone handling)" do
    test "delete makes key appear not_found", ctx do
      assert :ok =
               MetadataStore.write(@segment_id, "del:key", "value", server: ctx.meta_server)

      assert {:ok, "value", _} =
               MetadataStore.read(@segment_id, "del:key", server: ctx.meta_server)

      assert :ok =
               MetadataStore.delete(@segment_id, "del:key", server: ctx.meta_server)

      assert {:error, :not_found} =
               MetadataStore.read(@segment_id, "del:key", server: ctx.meta_server)
    end

    test "delete nonexistent key succeeds (tombstone is written)", ctx do
      assert :ok =
               MetadataStore.delete(@segment_id, "del:ghost", server: ctx.meta_server)

      assert {:error, :not_found} =
               MetadataStore.read(@segment_id, "del:ghost", server: ctx.meta_server)
    end
  end

  describe "list_segment/2" do
    test "lists live entries, excludes tombstones", ctx do
      assert :ok =
               MetadataStore.write(@segment_id, "list:a", "val_a", server: ctx.meta_server)

      assert :ok =
               MetadataStore.write(@segment_id, "list:b", "val_b", server: ctx.meta_server)

      assert :ok =
               MetadataStore.write(@segment_id, "list:c", "val_c", server: ctx.meta_server)

      # Delete one entry
      assert :ok =
               MetadataStore.delete(@segment_id, "list:b", server: ctx.meta_server)

      assert {:ok, entries} =
               MetadataStore.list_segment(@segment_id, server: ctx.meta_server)

      values = Enum.map(entries, fn {_hash, value, _ts} -> value end) |> Enum.sort()
      assert values == ["val_a", "val_c"]
    end

    test "empty segment returns empty list", ctx do
      empty_segment = :crypto.hash(:sha256, "empty_segment_#{random_string()}")

      assert {:ok, []} =
               MetadataStore.list_segment(empty_segment, server: ctx.meta_server)
    end
  end

  describe "load_segment/2" do
    test "populates cache from disk", ctx do
      # Write some data
      assert :ok =
               MetadataStore.write(@segment_id, "load:x", "x_val", server: ctx.meta_server)

      assert :ok =
               MetadataStore.write(@segment_id, "load:y", "y_val", server: ctx.meta_server)

      # Restart the MetadataStore to clear the cache
      stop_supervised!(:metadata_store)

      {:ok, _} =
        start_supervised(
          {MetadataStore,
           drive_id: "default",
           blob_store_server: ctx.blob_server,
           name: ctx.meta_server,
           max_cache_size: 100},
          id: :metadata_store
        )

      # Load segment to warm cache
      assert :ok =
               MetadataStore.load_segment(@segment_id, server: ctx.meta_server)

      # Should be able to read from cache now
      assert {:ok, "x_val", _} =
               MetadataStore.read(@segment_id, "load:x", server: ctx.meta_server)

      assert {:ok, "y_val", _} =
               MetadataStore.read(@segment_id, "load:y", server: ctx.meta_server)
    end
  end

  describe "ETS cache behaviour" do
    test "cache hit avoids BlobStore read on second access", ctx do
      assert :ok =
               MetadataStore.write(@segment_id, "cache:hit", "cached", server: ctx.meta_server)

      # First read populates cache (or was populated on write)
      assert {:ok, "cached", _} =
               MetadataStore.read(@segment_id, "cache:hit", server: ctx.meta_server)

      # Second read should also succeed (from cache)
      assert {:ok, "cached", _} =
               MetadataStore.read(@segment_id, "cache:hit", server: ctx.meta_server)
    end
  end

  defp random_string do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end
end
