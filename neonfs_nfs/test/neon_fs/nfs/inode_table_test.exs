defmodule NeonFS.NFS.InodeTableTest do
  use ExUnit.Case, async: false

  alias NeonFS.NFS.InodeTable

  setup do
    start_supervised!(InodeTable)
    :ok
  end

  test "root inode is pre-allocated" do
    assert {:ok, {nil, "/"}} = InodeTable.get_path(1)
    assert InodeTable.root_inode() == 1
  end

  test "allocate_inode returns unique inodes" do
    {:ok, inode1} = InodeTable.allocate_inode("vol1", "/file1.txt")
    {:ok, inode2} = InodeTable.allocate_inode("vol1", "/file2.txt")
    assert inode1 != inode2
    assert inode1 > 1
  end

  test "allocate_inode is idempotent for same path" do
    {:ok, inode1} = InodeTable.allocate_inode("vol1", "/test.txt")
    {:ok, inode2} = InodeTable.allocate_inode("vol1", "/test.txt")
    assert inode1 == inode2
  end

  test "different volumes can have same path with different inodes" do
    {:ok, inode1} = InodeTable.allocate_inode("vol1", "/data.txt")
    {:ok, inode2} = InodeTable.allocate_inode("vol2", "/data.txt")
    assert inode1 != inode2
  end

  test "get_path returns volume and path" do
    {:ok, inode} = InodeTable.allocate_inode("photos", "/2024/img.jpg")
    assert {:ok, {"photos", "/2024/img.jpg"}} = InodeTable.get_path(inode)
  end

  test "get_path returns error for unknown inode" do
    assert {:error, :not_found} = InodeTable.get_path(999_999)
  end

  test "get_inode returns inode for known path" do
    {:ok, inode} = InodeTable.allocate_inode("vol1", "/readme.md")
    assert {:ok, ^inode} = InodeTable.get_inode("vol1", "/readme.md")
  end

  test "get_inode returns error for unknown path" do
    assert {:error, :not_found} = InodeTable.get_inode("vol1", "/nonexistent")
  end

  test "release_inode removes mapping" do
    {:ok, inode} = InodeTable.allocate_inode("vol1", "/temp.txt")
    assert :ok = InodeTable.release_inode(inode)
    assert {:error, :not_found} = InodeTable.get_path(inode)
    assert {:error, :not_found} = InodeTable.get_inode("vol1", "/temp.txt")
  end

  test "cannot release root inode" do
    assert {:error, :cannot_release_root} = InodeTable.release_inode(1)
  end

  test "clear resets all mappings except root" do
    {:ok, inode} = InodeTable.allocate_inode("vol1", "/file.txt")
    InodeTable.clear()
    assert {:error, :not_found} = InodeTable.get_path(inode)
    assert {:ok, {nil, "/"}} = InodeTable.get_path(1)
  end
end
