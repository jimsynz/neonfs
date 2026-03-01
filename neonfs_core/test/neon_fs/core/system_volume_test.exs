defmodule NeonFS.Core.SystemVolumeTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.SystemVolume
  alias NeonFS.Core.VolumeRegistry

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()

    # Write cluster.json so VolumeRegistry.create_system_volume/0 can read the cluster name
    master_key = :crypto.strong_rand_bytes(32) |> Base.encode64()
    write_cluster_json(tmp_dir, master_key)

    start_drive_registry()
    start_blob_store()
    start_chunk_index()
    start_file_index()
    start_stripe_index()
    start_volume_registry()
    ensure_chunk_access_tracker()

    # Create the system volume for tests
    {:ok, _volume} = VolumeRegistry.create_system_volume()

    on_exit(fn -> cleanup_test_dirs() end)
    :ok
  end

  describe "read/1" do
    test "returns file content after write" do
      :ok = SystemVolume.write("/test/hello.txt", "hello world")
      assert {:ok, "hello world"} = SystemVolume.read("/test/hello.txt")
    end

    test "returns error for non-existent file" do
      assert {:error, %{class: :not_found}} = SystemVolume.read("/no/such/file.txt")
    end
  end

  describe "write/1" do
    test "creates a new file" do
      assert :ok = SystemVolume.write("/data/file.bin", <<1, 2, 3>>)
      assert {:ok, <<1, 2, 3>>} = SystemVolume.read("/data/file.bin")
    end

    test "overwrites an existing file" do
      :ok = SystemVolume.write("/overwrite.txt", "first")
      :ok = SystemVolume.write("/overwrite.txt", "second")
      assert {:ok, "second"} = SystemVolume.read("/overwrite.txt")
    end
  end

  describe "append/2" do
    test "creates file if it does not exist" do
      assert :ok = SystemVolume.append("/logs/new.jsonl", "line1\n")
      assert {:ok, "line1\n"} = SystemVolume.read("/logs/new.jsonl")
    end

    test "appends to existing file" do
      :ok = SystemVolume.write("/logs/existing.jsonl", "line1\n")
      :ok = SystemVolume.append("/logs/existing.jsonl", "line2\n")
      assert {:ok, "line1\nline2\n"} = SystemVolume.read("/logs/existing.jsonl")
    end

    test "multiple appends accumulate" do
      :ok = SystemVolume.append("/logs/multi.jsonl", "a")
      :ok = SystemVolume.append("/logs/multi.jsonl", "b")
      :ok = SystemVolume.append("/logs/multi.jsonl", "c")
      assert {:ok, "abc"} = SystemVolume.read("/logs/multi.jsonl")
    end
  end

  describe "list/1" do
    test "returns file names under a path" do
      :ok = SystemVolume.write("/dir/alpha.txt", "a")
      :ok = SystemVolume.write("/dir/beta.txt", "b")
      :ok = SystemVolume.write("/dir/gamma.txt", "c")

      assert {:ok, names} = SystemVolume.list("/dir")
      assert "alpha.txt" in names
      assert "beta.txt" in names
      assert "gamma.txt" in names
    end

    test "returns sorted names" do
      :ok = SystemVolume.write("/sorted/c.txt", "c")
      :ok = SystemVolume.write("/sorted/a.txt", "a")
      :ok = SystemVolume.write("/sorted/b.txt", "b")

      assert {:ok, ["a.txt", "b.txt", "c.txt"]} = SystemVolume.list("/sorted")
    end

    test "returns empty list for non-existent directory" do
      assert {:ok, []} = SystemVolume.list("/empty")
    end
  end

  describe "delete/1" do
    test "removes a file" do
      :ok = SystemVolume.write("/delete/target.txt", "doomed")
      assert true == SystemVolume.exists?("/delete/target.txt")

      assert :ok = SystemVolume.delete("/delete/target.txt")
      assert false == SystemVolume.exists?("/delete/target.txt")
    end

    test "returns error for non-existent file" do
      assert {:error, :not_found} = SystemVolume.delete("/no/such/file.txt")
    end
  end

  describe "exists?/1" do
    test "returns true for existing file" do
      :ok = SystemVolume.write("/exists/check.txt", "present")
      assert true == SystemVolume.exists?("/exists/check.txt")
    end

    test "returns false for non-existent file" do
      assert false == SystemVolume.exists?("/no/such/file.txt")
    end

    test "returns false after deletion" do
      :ok = SystemVolume.write("/exists/deleted.txt", "temporary")
      :ok = SystemVolume.delete("/exists/deleted.txt")
      assert false == SystemVolume.exists?("/exists/deleted.txt")
    end
  end

  describe "error handling" do
    test "all functions return :system_volume_not_found when system volume does not exist" do
      # Clear the system volume from the registry
      :ets.delete_all_objects(:volumes_by_id)
      :ets.delete_all_objects(:volumes_by_name)

      assert {:error, :system_volume_not_found} = SystemVolume.read("/any")
      assert {:error, :system_volume_not_found} = SystemVolume.write("/any", "data")
      assert {:error, :system_volume_not_found} = SystemVolume.append("/any", "data")
      assert {:error, :system_volume_not_found} = SystemVolume.list("/any")
      assert {:error, :system_volume_not_found} = SystemVolume.delete("/any")
      assert false == SystemVolume.exists?("/any")
    end
  end
end
