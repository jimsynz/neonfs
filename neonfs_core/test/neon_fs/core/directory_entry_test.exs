defmodule NeonFS.Core.DirectoryEntryTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.DirectoryEntry

  describe "new/3" do
    test "creates a directory entry with defaults" do
      entry = DirectoryEntry.new("vol1", "/documents")

      assert entry.parent_path == "/documents"
      assert entry.volume_id == "vol1"
      assert entry.children == %{}
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

  describe "add_child/4" do
    test "adds a file child" do
      entry =
        DirectoryEntry.new("vol1", "/")
        |> DirectoryEntry.add_child("test.txt", :file, "file-id-1")

      assert entry.children == %{
               "test.txt" => %{type: :file, id: "file-id-1"}
             }
    end

    test "adds a directory child" do
      entry =
        DirectoryEntry.new("vol1", "/")
        |> DirectoryEntry.add_child("docs", :dir, "dir-id-1")

      assert entry.children == %{
               "docs" => %{type: :dir, id: "dir-id-1"}
             }
    end

    test "overwrites existing child with same name" do
      entry =
        DirectoryEntry.new("vol1", "/")
        |> DirectoryEntry.add_child("test.txt", :file, "file-id-1")
        |> DirectoryEntry.add_child("test.txt", :file, "file-id-2")

      assert entry.children == %{
               "test.txt" => %{type: :file, id: "file-id-2"}
             }
    end

    test "adds multiple children" do
      entry =
        DirectoryEntry.new("vol1", "/")
        |> DirectoryEntry.add_child("a.txt", :file, "id-a")
        |> DirectoryEntry.add_child("b.txt", :file, "id-b")
        |> DirectoryEntry.add_child("subdir", :dir, "id-sub")

      assert map_size(entry.children) == 3
    end
  end

  describe "remove_child/2" do
    test "removes an existing child" do
      entry =
        DirectoryEntry.new("vol1", "/")
        |> DirectoryEntry.add_child("test.txt", :file, "file-id-1")
        |> DirectoryEntry.remove_child("test.txt")

      assert entry.children == %{}
    end

    test "is a no-op for non-existent child" do
      entry =
        DirectoryEntry.new("vol1", "/")
        |> DirectoryEntry.remove_child("nonexistent.txt")

      assert entry.children == %{}
    end
  end

  describe "rename_child/3" do
    test "renames an existing child" do
      entry =
        DirectoryEntry.new("vol1", "/")
        |> DirectoryEntry.add_child("old.txt", :file, "file-id-1")

      assert {:ok, renamed} = DirectoryEntry.rename_child(entry, "old.txt", "new.txt")
      assert renamed.children == %{"new.txt" => %{type: :file, id: "file-id-1"}}
    end

    test "returns error for non-existent source" do
      entry = DirectoryEntry.new("vol1", "/")
      assert {:error, :not_found} = DirectoryEntry.rename_child(entry, "nope", "new.txt")
    end

    test "returns error if target name already exists" do
      entry =
        DirectoryEntry.new("vol1", "/")
        |> DirectoryEntry.add_child("a.txt", :file, "id-a")
        |> DirectoryEntry.add_child("b.txt", :file, "id-b")

      assert {:error, :already_exists} = DirectoryEntry.rename_child(entry, "a.txt", "b.txt")
    end

    test "allows renaming to the same name" do
      entry =
        DirectoryEntry.new("vol1", "/")
        |> DirectoryEntry.add_child("same.txt", :file, "id")

      assert {:ok, result} = DirectoryEntry.rename_child(entry, "same.txt", "same.txt")
      assert result.children == %{"same.txt" => %{type: :file, id: "id"}}
    end
  end

  describe "has_child?/2" do
    test "returns true for existing child" do
      entry =
        DirectoryEntry.new("vol1", "/")
        |> DirectoryEntry.add_child("test.txt", :file, "id")

      assert DirectoryEntry.has_child?(entry, "test.txt")
    end

    test "returns false for non-existent child" do
      entry = DirectoryEntry.new("vol1", "/")
      refute DirectoryEntry.has_child?(entry, "nope")
    end
  end

  describe "get_child/2" do
    test "returns child info" do
      entry =
        DirectoryEntry.new("vol1", "/")
        |> DirectoryEntry.add_child("test.txt", :file, "id-123")

      assert {:ok, %{type: :file, id: "id-123"}} = DirectoryEntry.get_child(entry, "test.txt")
    end

    test "returns error for non-existent child" do
      entry = DirectoryEntry.new("vol1", "/")
      assert {:error, :not_found} = DirectoryEntry.get_child(entry, "nope")
    end
  end

  describe "serialisation round-trip" do
    test "to_storable_map/1 and from_storable_map/1" do
      entry =
        DirectoryEntry.new("vol1", "/docs", mode: 0o700, uid: 1000, gid: 1000)
        |> DirectoryEntry.add_child("readme.md", :file, "file-id")
        |> DirectoryEntry.add_child("src", :dir, "dir-id")

      map = DirectoryEntry.to_storable_map(entry)
      restored = DirectoryEntry.from_storable_map(map)

      assert restored.parent_path == "/docs"
      assert restored.volume_id == "vol1"
      assert restored.mode == 0o700
      assert restored.uid == 1000
      assert restored.gid == 1000
      assert restored.children["readme.md"] == %{type: :file, id: "file-id"}
      assert restored.children["src"] == %{type: :dir, id: "dir-id"}
    end

    test "handles string keys from deserialisation" do
      map = %{
        "parent_path" => "/test",
        "volume_id" => "vol1",
        "children" => %{
          "file.txt" => %{"type" => "file", "id" => "id-1"}
        },
        "mode" => 0o755,
        "uid" => 0,
        "gid" => 0
      }

      restored = DirectoryEntry.from_storable_map(map)
      assert restored.parent_path == "/test"
      assert restored.children["file.txt"] == %{type: :file, id: "id-1"}
    end
  end
end
