defmodule NeonFS.FUSE.MetadataCacheTest do
  use ExUnit.Case, async: false

  alias NeonFS.Events.Envelope
  alias NeonFS.FUSE.MetadataCache

  @volume_id "test-volume-id"

  setup do
    # Start event infrastructure needed by MetadataCache
    start_supervised!(%{id: :pg_neonfs_events, start: {:pg, :start_link, [:neonfs_events]}})
    start_supervised!({Registry, keys: :duplicate, name: NeonFS.Events.Registry})
    start_supervised!(NeonFS.Events.Relay)

    # Start the MetadataCache under test
    {:ok, cache_pid} = start_supervised({MetadataCache, volume_id: @volume_id})
    table = MetadataCache.table(cache_pid)

    %{cache_pid: cache_pid, table: table}
  end

  # -- Cache API tests --

  describe "attrs cache" do
    test "put_attrs/4 and get_attrs/3 roundtrip", %{table: table} do
      attrs = %{path: "/foo.txt", size: 42, mode: 0o100644}
      :ok = MetadataCache.put_attrs(table, @volume_id, "/foo.txt", attrs)

      assert {:ok, ^attrs} = MetadataCache.get_attrs(table, @volume_id, "/foo.txt")
    end

    test "get_attrs/3 returns :miss on cache miss", %{table: table} do
      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/nonexistent.txt")
    end
  end

  describe "dir_listing cache" do
    test "put_dir_listing/4 and get_dir_listing/3 roundtrip", %{table: table} do
      entries = [{"file.txt", "/dir/file.txt", 0o100644}, {"sub", "/dir/sub", 0o040755}]
      :ok = MetadataCache.put_dir_listing(table, @volume_id, "/dir", entries)

      assert {:ok, ^entries} = MetadataCache.get_dir_listing(table, @volume_id, "/dir")
    end

    test "get_dir_listing/3 returns :miss on cache miss", %{table: table} do
      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/nonexistent")
    end
  end

  describe "lookup cache" do
    test "put_lookup/5 and get_lookup/4 roundtrip", %{table: table} do
      file = %{path: "/dir/file.txt", size: 100, mode: 0o100644}
      :ok = MetadataCache.put_lookup(table, @volume_id, "/dir", "file.txt", file)

      assert {:ok, ^file} = MetadataCache.get_lookup(table, @volume_id, "/dir", "file.txt")
    end

    test "get_lookup/4 returns :miss on cache miss", %{table: table} do
      assert :miss = MetadataCache.get_lookup(table, @volume_id, "/dir", "missing.txt")
    end
  end

  describe "invalidate_all/2" do
    test "clears all entries", %{table: table} do
      MetadataCache.put_attrs(table, @volume_id, "/a.txt", %{size: 1})
      MetadataCache.put_dir_listing(table, @volume_id, "/", [{"a.txt", "/a.txt", 0o100644}])
      MetadataCache.put_lookup(table, @volume_id, "/", "a.txt", %{size: 1})

      :ok = MetadataCache.invalidate_all(table, @volume_id)

      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/a.txt")
      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/")
      assert :miss = MetadataCache.get_lookup(table, @volume_id, "/", "a.txt")
    end
  end

  # -- Event invalidation tests --

  describe "FileCreated event" do
    test "invalidates parent directory listing", %{cache_pid: pid, table: table} do
      MetadataCache.put_dir_listing(table, @volume_id, "/dir", [
        {"old.txt", "/dir/old.txt", 0o100644}
      ])

      MetadataCache.put_lookup(table, @volume_id, "/dir", "new.txt", %{size: 0})

      send_event(pid, %NeonFS.Events.FileCreated{
        volume_id: @volume_id,
        file_id: "f1",
        path: "/dir/new.txt"
      })

      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/dir")
      assert :miss = MetadataCache.get_lookup(table, @volume_id, "/dir", "new.txt")
    end
  end

  describe "FileContentUpdated event" do
    test "invalidates file attributes", %{cache_pid: pid, table: table} do
      MetadataCache.put_attrs(table, @volume_id, "/file.txt", %{size: 42})

      send_event(pid, %NeonFS.Events.FileContentUpdated{
        volume_id: @volume_id,
        file_id: "f1",
        path: "/file.txt"
      })

      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/file.txt")
    end
  end

  describe "FileTruncated event" do
    test "invalidates file attributes", %{cache_pid: pid, table: table} do
      MetadataCache.put_attrs(table, @volume_id, "/file.txt", %{size: 42})

      send_event(pid, %NeonFS.Events.FileTruncated{
        volume_id: @volume_id,
        file_id: "f1",
        path: "/file.txt"
      })

      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/file.txt")
    end
  end

  describe "FileDeleted event" do
    test "invalidates file attributes and parent directory listing", %{
      cache_pid: pid,
      table: table
    } do
      MetadataCache.put_attrs(table, @volume_id, "/dir/file.txt", %{size: 42})

      MetadataCache.put_dir_listing(table, @volume_id, "/dir", [
        {"file.txt", "/dir/file.txt", 0o100644}
      ])

      MetadataCache.put_lookup(table, @volume_id, "/dir", "file.txt", %{size: 42})

      send_event(pid, %NeonFS.Events.FileDeleted{
        volume_id: @volume_id,
        file_id: "f1",
        path: "/dir/file.txt"
      })

      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/dir/file.txt")
      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/dir")
      assert :miss = MetadataCache.get_lookup(table, @volume_id, "/dir", "file.txt")
    end
  end

  describe "FileAttrsChanged event" do
    test "invalidates file attributes", %{cache_pid: pid, table: table} do
      MetadataCache.put_attrs(table, @volume_id, "/file.txt", %{size: 42, mode: 0o100644})

      send_event(pid, %NeonFS.Events.FileAttrsChanged{
        volume_id: @volume_id,
        file_id: "f1",
        path: "/file.txt"
      })

      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/file.txt")
    end
  end

  describe "FileRenamed event" do
    test "invalidates both old and new paths", %{cache_pid: pid, table: table} do
      MetadataCache.put_attrs(table, @volume_id, "/old/file.txt", %{size: 42})

      MetadataCache.put_dir_listing(table, @volume_id, "/old", [
        {"file.txt", "/old/file.txt", 0o100644}
      ])

      MetadataCache.put_dir_listing(table, @volume_id, "/new", [])
      MetadataCache.put_lookup(table, @volume_id, "/old", "file.txt", %{size: 42})

      send_event(pid, %NeonFS.Events.FileRenamed{
        volume_id: @volume_id,
        file_id: "f1",
        old_path: "/old/file.txt",
        new_path: "/new/file.txt"
      })

      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/old/file.txt")
      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/old")
      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/new")
      assert :miss = MetadataCache.get_lookup(table, @volume_id, "/old", "file.txt")
    end
  end

  describe "VolumeAclChanged event" do
    test "invalidates all entries", %{cache_pid: pid, table: table} do
      MetadataCache.put_attrs(table, @volume_id, "/a.txt", %{size: 1})
      MetadataCache.put_attrs(table, @volume_id, "/b.txt", %{size: 2})
      MetadataCache.put_dir_listing(table, @volume_id, "/", [{"a.txt", "/a.txt", 0o100644}])

      send_event(pid, %NeonFS.Events.VolumeAclChanged{volume_id: @volume_id})

      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/a.txt")
      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/b.txt")
      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/")
    end
  end

  describe "FileAclChanged event" do
    test "invalidates file attributes", %{cache_pid: pid, table: table} do
      MetadataCache.put_attrs(table, @volume_id, "/secure.txt", %{size: 10})

      send_event(pid, %NeonFS.Events.FileAclChanged{
        volume_id: @volume_id,
        path: "/secure.txt"
      })

      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/secure.txt")
    end
  end

  describe "DirCreated event" do
    test "invalidates parent directory listing", %{cache_pid: pid, table: table} do
      MetadataCache.put_dir_listing(table, @volume_id, "/parent", [])

      send_event(pid, %NeonFS.Events.DirCreated{
        volume_id: @volume_id,
        path: "/parent/newdir"
      })

      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/parent")
    end
  end

  describe "DirDeleted event" do
    test "invalidates directory and parent directory listing", %{cache_pid: pid, table: table} do
      MetadataCache.put_dir_listing(table, @volume_id, "/parent/child", [
        {"x.txt", "/parent/child/x.txt", 0o100644}
      ])

      MetadataCache.put_dir_listing(table, @volume_id, "/parent", [
        {"child", "/parent/child", 0o040755}
      ])

      send_event(pid, %NeonFS.Events.DirDeleted{
        volume_id: @volume_id,
        path: "/parent/child"
      })

      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/parent/child")
      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/parent")
    end
  end

  describe "DirRenamed event" do
    test "invalidates old directory and parent directory listings for both locations",
         %{cache_pid: pid, table: table} do
      MetadataCache.put_dir_listing(table, @volume_id, "/src/dir", [
        {"f.txt", "/src/dir/f.txt", 0o100644}
      ])

      MetadataCache.put_dir_listing(table, @volume_id, "/src", [{"dir", "/src/dir", 0o040755}])
      MetadataCache.put_dir_listing(table, @volume_id, "/dst", [])

      send_event(pid, %NeonFS.Events.DirRenamed{
        volume_id: @volume_id,
        old_path: "/src/dir",
        new_path: "/dst/dir"
      })

      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/src/dir")
      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/src")
      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/dst")
    end
  end

  describe "VolumeUpdated event" do
    test "invalidates all entries", %{cache_pid: pid, table: table} do
      MetadataCache.put_attrs(table, @volume_id, "/a.txt", %{size: 1})
      MetadataCache.put_dir_listing(table, @volume_id, "/", [])

      send_event(pid, %NeonFS.Events.VolumeUpdated{volume_id: @volume_id})

      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/a.txt")
      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/")
    end
  end

  # -- Gap detection tests --

  describe "gap detection" do
    test "clears all caches when sequence gap detected", %{cache_pid: pid, table: table} do
      MetadataCache.put_attrs(table, @volume_id, "/a.txt", %{size: 1})
      MetadataCache.put_dir_listing(table, @volume_id, "/", [])

      source = :test_node@localhost

      # Send sequence 1
      send_event(
        pid,
        %NeonFS.Events.FileCreated{
          volume_id: @volume_id,
          file_id: "f1",
          path: "/b.txt"
        },
        source: source,
        sequence: 1
      )

      # Re-populate cache
      MetadataCache.put_attrs(table, @volume_id, "/c.txt", %{size: 3})

      # Send sequence 3 (skipping 2) — should trigger gap detection
      send_event(
        pid,
        %NeonFS.Events.FileCreated{
          volume_id: @volume_id,
          file_id: "f2",
          path: "/d.txt"
        },
        source: source,
        sequence: 3
      )

      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/c.txt")
    end

    test "no gap when sequences are contiguous", %{cache_pid: pid, table: table} do
      source = :test_node@localhost

      # Send sequence 1
      send_event(
        pid,
        %NeonFS.Events.FileCreated{
          volume_id: @volume_id,
          file_id: "f1",
          path: "/a.txt"
        },
        source: source,
        sequence: 1
      )

      # Populate cache between events
      MetadataCache.put_attrs(table, @volume_id, "/keep.txt", %{size: 99})

      # Send sequence 2 (contiguous) — should NOT clear all caches
      send_event(
        pid,
        %NeonFS.Events.FileCreated{
          volume_id: @volume_id,
          file_id: "f2",
          path: "/b.txt"
        },
        source: source,
        sequence: 2
      )

      # /keep.txt should still be cached (only normal invalidation happened)
      assert {:ok, %{size: 99}} = MetadataCache.get_attrs(table, @volume_id, "/keep.txt")
    end
  end

  # -- Partition recovery tests --

  describe ":neonfs_invalidate_all" do
    test "clears all caches", %{cache_pid: pid, table: table} do
      MetadataCache.put_attrs(table, @volume_id, "/a.txt", %{size: 1})
      MetadataCache.put_dir_listing(table, @volume_id, "/", [])
      MetadataCache.put_lookup(table, @volume_id, "/", "a.txt", %{size: 1})

      send(pid, :neonfs_invalidate_all)
      # Wait for the message to be processed
      _ = :sys.get_state(pid)

      assert :miss = MetadataCache.get_attrs(table, @volume_id, "/a.txt")
      assert :miss = MetadataCache.get_dir_listing(table, @volume_id, "/")
      assert :miss = MetadataCache.get_lookup(table, @volume_id, "/", "a.txt")
    end
  end

  # -- Helpers --

  defp send_event(pid, event, opts \\ []) do
    envelope = %Envelope{
      event: event,
      source_node: Keyword.get(opts, :source, node()),
      sequence: Keyword.get(opts, :sequence, 1),
      hlc_timestamp: {System.system_time(:millisecond), 0, node()}
    }

    send(pid, {:neonfs_event, envelope})
    # Wait for the message to be processed
    _ = :sys.get_state(pid)
  end
end
