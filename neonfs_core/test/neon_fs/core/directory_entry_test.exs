defmodule NeonFS.Core.DirectoryEntryTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.DirectoryEntry

  describe "new/3" do
    test "creates a directory record with defaults" do
      entry = DirectoryEntry.new("vol1", "/documents")

      assert entry.parent_path == "/documents"
      assert entry.volume_id == "vol1"
      assert entry.mode == 0o755
      assert entry.uid == 0
      assert entry.gid == 0
    end

    test "accepts custom options" do
      entry = DirectoryEntry.new("vol1", "/", mode: 0o700, uid: 1000, gid: 1000)

      assert entry.mode == 0o700
      assert entry.uid == 1000
      assert entry.gid == 1000
    end
  end

  describe "serialisation round-trip" do
    test "to_storable_map/1 and from_storable_map/1" do
      entry = DirectoryEntry.new("vol1", "/docs", mode: 0o700, uid: 1000, gid: 1000)

      restored =
        entry
        |> DirectoryEntry.to_storable_map()
        |> DirectoryEntry.from_storable_map()

      assert restored.parent_path == "/docs"
      assert restored.volume_id == "vol1"
      assert restored.mode == 0o700
      assert restored.uid == 1000
      assert restored.gid == 1000
    end

    test "handles string keys from deserialisation" do
      map = %{
        "parent_path" => "/test",
        "volume_id" => "vol1",
        "mode" => 0o755,
        "uid" => 0,
        "gid" => 0
      }

      restored = DirectoryEntry.from_storable_map(map)
      assert restored.parent_path == "/test"
      assert restored.volume_id == "vol1"
      assert restored.mode == 0o755
    end
  end
end
