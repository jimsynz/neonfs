defmodule NeonFS.FUSE.InodeTableTest do
  use ExUnit.Case, async: false

  alias NeonFS.FUSE.InodeTable

  setup do
    # Clear the inode table before each test
    InodeTable.clear()
    :ok
  end

  describe "root_inode/0" do
    test "returns 1" do
      assert InodeTable.root_inode() == 1
    end
  end

  describe "allocate_inode/2" do
    test "allocates a new inode for a path" do
      assert {:ok, inode} = InodeTable.allocate_inode("vol1", "/test.txt")
      assert inode > 1
    end

    test "returns same inode for same path" do
      assert {:ok, inode1} = InodeTable.allocate_inode("vol1", "/test.txt")
      assert {:ok, inode2} = InodeTable.allocate_inode("vol1", "/test.txt")
      assert inode1 == inode2
    end

    test "allocates different inodes for different paths" do
      assert {:ok, inode1} = InodeTable.allocate_inode("vol1", "/test1.txt")
      assert {:ok, inode2} = InodeTable.allocate_inode("vol1", "/test2.txt")
      assert inode1 != inode2
    end

    test "allocates different inodes for same path in different volumes" do
      assert {:ok, inode1} = InodeTable.allocate_inode("vol1", "/test.txt")
      assert {:ok, inode2} = InodeTable.allocate_inode("vol2", "/test.txt")
      assert inode1 != inode2
    end

    test "increments inode counter" do
      assert {:ok, 2} = InodeTable.allocate_inode("vol1", "/file1.txt")
      assert {:ok, 3} = InodeTable.allocate_inode("vol1", "/file2.txt")
      assert {:ok, 4} = InodeTable.allocate_inode("vol1", "/file3.txt")
    end
  end

  describe "get_path/1" do
    test "returns root path for inode 1" do
      assert {:ok, {nil, "/"}} = InodeTable.get_path(1)
    end

    test "returns path for allocated inode" do
      {:ok, inode} = InodeTable.allocate_inode("vol1", "/test.txt")
      assert {:ok, {"vol1", "/test.txt"}} = InodeTable.get_path(inode)
    end

    test "returns error for unknown inode" do
      assert {:error, :not_found} = InodeTable.get_path(999)
    end
  end

  describe "get_inode/2" do
    test "returns root inode for root path" do
      assert {:ok, 1} = InodeTable.get_inode(nil, "/")
    end

    test "returns inode for allocated path" do
      {:ok, inode} = InodeTable.allocate_inode("vol1", "/test.txt")
      assert {:ok, ^inode} = InodeTable.get_inode("vol1", "/test.txt")
    end

    test "returns error for unknown path" do
      assert {:error, :not_found} = InodeTable.get_inode("vol1", "/unknown.txt")
    end
  end

  describe "release_inode/1" do
    test "releases an allocated inode" do
      {:ok, inode} = InodeTable.allocate_inode("vol1", "/test.txt")
      assert :ok = InodeTable.release_inode(inode)
      assert {:error, :not_found} = InodeTable.get_path(inode)
      assert {:error, :not_found} = InodeTable.get_inode("vol1", "/test.txt")
    end

    test "cannot release root inode" do
      assert {:error, :cannot_release_root} = InodeTable.release_inode(1)
      assert {:ok, {nil, "/"}} = InodeTable.get_path(1)
    end

    test "returns error for unknown inode" do
      assert {:error, :not_found} = InodeTable.release_inode(999)
    end

    test "allows re-allocation after release" do
      {:ok, inode1} = InodeTable.allocate_inode("vol1", "/test.txt")
      :ok = InodeTable.release_inode(inode1)
      {:ok, inode2} = InodeTable.allocate_inode("vol1", "/test.txt")
      # New allocation gets a different inode
      assert inode1 != inode2
    end
  end

  describe "clear/0" do
    test "clears all mappings except root" do
      InodeTable.allocate_inode("vol1", "/file1.txt")
      InodeTable.allocate_inode("vol1", "/file2.txt")
      InodeTable.allocate_inode("vol2", "/file3.txt")

      assert :ok = InodeTable.clear()

      # Root should still exist
      assert {:ok, {nil, "/"}} = InodeTable.get_path(1)

      # Other paths should be gone
      assert {:error, :not_found} = InodeTable.get_inode("vol1", "/file1.txt")
      assert {:error, :not_found} = InodeTable.get_inode("vol1", "/file2.txt")
      assert {:error, :not_found} = InodeTable.get_inode("vol2", "/file3.txt")
    end

    test "resets inode counter" do
      InodeTable.allocate_inode("vol1", "/file1.txt")
      InodeTable.allocate_inode("vol1", "/file2.txt")

      InodeTable.clear()

      # Next allocation should be inode 2 again
      assert {:ok, 2} = InodeTable.allocate_inode("vol1", "/new.txt")
    end
  end

  describe "concurrent operations" do
    test "handles concurrent allocations" do
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            InodeTable.allocate_inode("vol1", "/file#{i}.txt")
          end)
        end

      results = Task.await_many(tasks)

      # All should succeed with unique inodes
      assert Enum.all?(results, fn {:ok, _} -> true end)
      inodes = Enum.map(results, fn {:ok, inode} -> inode end)
      assert length(Enum.uniq(inodes)) == 10
    end

    test "handles concurrent lookups" do
      {:ok, inode} = InodeTable.allocate_inode("vol1", "/test.txt")

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            InodeTable.get_path(inode)
          end)
        end

      results = Task.await_many(tasks)

      # All should return the same path
      assert Enum.all?(results, fn {:ok, {"vol1", "/test.txt"}} -> true end)
    end
  end
end
