defmodule NeonFS.Core.Volume.MetadataCacheTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.Volume.MetadataCache

  setup do
    # In test env `start_children?: false` keeps the supervision tree
    # idle. Start the cache for this test run; on_exit cleans up so
    # subsequent tests get a fresh instance.
    case :ets.whereis(MetadataCache) do
      :undefined -> start_supervised!(MetadataCache)
      _ -> :ets.delete_all_objects(MetadataCache)
    end

    on_exit(fn ->
      if :ets.whereis(MetadataCache) != :undefined do
        :ets.delete_all_objects(MetadataCache)
      end
    end)

    :ok
  end

  describe "get/3 + put/4" do
    test "returns :miss for an absent key" do
      assert :miss = MetadataCache.get("vol-1", <<1>>, "k")
    end

    test "returns {:ok, value} after a put" do
      :ok = MetadataCache.put("vol-1", <<1>>, "k", :value)
      assert {:ok, :value} = MetadataCache.get("vol-1", <<1>>, "k")
    end

    test "different root_chunk_hash is a different key (CoW versioning)" do
      :ok = MetadataCache.put("vol-1", <<1>>, "k", :v1)
      :ok = MetadataCache.put("vol-1", <<2>>, "k", :v2)

      assert {:ok, :v1} = MetadataCache.get("vol-1", <<1>>, "k")
      assert {:ok, :v2} = MetadataCache.get("vol-1", <<2>>, "k")
    end

    test "different volume_id is a different key" do
      :ok = MetadataCache.put("vol-a", <<1>>, "k", :a)
      :ok = MetadataCache.put("vol-b", <<1>>, "k", :b)

      assert {:ok, :a} = MetadataCache.get("vol-a", <<1>>, "k")
      assert {:ok, :b} = MetadataCache.get("vol-b", <<1>>, "k")
    end

    test "put overwrites a prior value at the same key" do
      :ok = MetadataCache.put("vol-1", <<1>>, "k", :first)
      :ok = MetadataCache.put("vol-1", <<1>>, "k", :second)

      assert {:ok, :second} = MetadataCache.get("vol-1", <<1>>, "k")
    end
  end

  describe "evict_volume/1" do
    test "wipes every entry for the given volume" do
      :ok = MetadataCache.put("vol-1", <<1>>, "k1", :a)
      :ok = MetadataCache.put("vol-1", <<2>>, "k2", :b)
      :ok = MetadataCache.put("vol-2", <<1>>, "k1", :other)

      assert 2 = MetadataCache.evict_volume("vol-1")

      assert :miss = MetadataCache.get("vol-1", <<1>>, "k1")
      assert :miss = MetadataCache.get("vol-1", <<2>>, "k2")
      # Other volume's entries untouched.
      assert {:ok, :other} = MetadataCache.get("vol-2", <<1>>, "k1")
    end

    test "returns 0 when the volume has no entries" do
      assert 0 = MetadataCache.evict_volume("absent")
    end
  end

  describe "telemetry" do
    test "emits :hit on a cache hit" do
      :ok = MetadataCache.put("vol-1", <<1>>, "k", :v)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :volume, :metadata_cache, :hit]
        ])

      _ = MetadataCache.get("vol-1", <<1>>, "k")

      assert_receive {[:neonfs, :volume, :metadata_cache, :hit], ^ref, %{},
                      %{volume_id: "vol-1", root_chunk_hash: <<1>>}}
    end

    test "emits :miss on an absent key" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :volume, :metadata_cache, :miss]
        ])

      _ = MetadataCache.get("vol-1", <<99>>, "k")

      assert_receive {[:neonfs, :volume, :metadata_cache, :miss], ^ref, %{},
                      %{volume_id: "vol-1", root_chunk_hash: <<99>>}}
    end

    test "emits :evict_volume with the eviction count" do
      :ok = MetadataCache.put("vol-1", <<1>>, "k1", :a)
      :ok = MetadataCache.put("vol-1", <<2>>, "k2", :b)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :volume, :metadata_cache, :evict_volume]
        ])

      _ = MetadataCache.evict_volume("vol-1")

      assert_receive {[:neonfs, :volume, :metadata_cache, :evict_volume], ^ref, %{count: 2},
                      %{volume_id: "vol-1"}}
    end
  end

  describe "size/0" do
    test "reflects the current entry count" do
      assert MetadataCache.size() == 0
      :ok = MetadataCache.put("vol-1", <<1>>, "k1", :v)
      assert MetadataCache.size() == 1
      :ok = MetadataCache.put("vol-1", <<1>>, "k2", :v)
      assert MetadataCache.size() == 2
    end
  end
end
