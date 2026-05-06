defmodule NeonFS.Core.Drive.IdentityTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Drive.Identity

  @moduletag :tmp_dir

  describe "new/2" do
    test "stamps current timestamp and version" do
      identity = Identity.new("disk1", "clust_abc")

      assert identity.drive_id == "disk1"
      assert identity.cluster_id == "clust_abc"
      assert identity.schema_version == 1
      assert identity.on_disk_format_version == 1
      assert %DateTime{} = identity.created_at
      assert is_binary(identity.created_by_neonfs_version)
    end
  end

  describe "write/2 and read/1 round-trip" do
    test "writes the file atomically and reads it back", %{tmp_dir: tmp_dir} do
      identity = Identity.new("disk1", "clust_abc")

      assert :ok = Identity.write(tmp_dir, identity)
      assert File.exists?(Identity.path(tmp_dir))

      {:ok, read_back} = Identity.read(tmp_dir)

      assert read_back.drive_id == identity.drive_id
      assert read_back.cluster_id == identity.cluster_id
      assert read_back.schema_version == identity.schema_version
      assert read_back.on_disk_format_version == identity.on_disk_format_version
      assert read_back.created_by_neonfs_version == identity.created_by_neonfs_version
      assert DateTime.compare(read_back.created_at, identity.created_at) == :eq
    end

    test "leaves no `.tmp` file behind", %{tmp_dir: tmp_dir} do
      :ok = Identity.write(tmp_dir, Identity.new("disk1", "clust_abc"))

      refute File.exists?(Identity.path(tmp_dir) <> ".tmp")
    end
  end

  describe "read/1" do
    test "returns :enoent when the file is absent", %{tmp_dir: tmp_dir} do
      assert {:error, :enoent} = Identity.read(tmp_dir)
    end

    test "rejects malformed JSON", %{tmp_dir: tmp_dir} do
      File.write!(Identity.path(tmp_dir), "this is not json")

      assert {:error, {:invalid_json, _}} = Identity.read(tmp_dir)
    end

    test "rejects unsupported schema versions with a clear error", %{tmp_dir: tmp_dir} do
      File.write!(Identity.path(tmp_dir), Jason.encode!(%{"schema_version" => 99}))

      assert {:error, {:unsupported_schema_version, 99}} = Identity.read(tmp_dir)
    end

    test "rejects files missing required fields", %{tmp_dir: tmp_dir} do
      File.write!(
        Identity.path(tmp_dir),
        Jason.encode!(%{"schema_version" => 1, "drive_id" => "disk1"})
      )

      assert {:error, {:malformed, _}} = Identity.read(tmp_dir)
    end
  end

  describe "ensure/3" do
    test "writes a fresh identity when none exists", %{tmp_dir: tmp_dir} do
      assert :ok = Identity.ensure(tmp_dir, "clust_abc", "disk1")

      {:ok, identity} = Identity.read(tmp_dir)
      assert identity.cluster_id == "clust_abc"
      assert identity.drive_id == "disk1"
    end

    test "is idempotent when identity matches", %{tmp_dir: tmp_dir} do
      assert :ok = Identity.ensure(tmp_dir, "clust_abc", "disk1")
      mtime1 = File.stat!(Identity.path(tmp_dir)).mtime
      Process.sleep(1100)
      assert :ok = Identity.ensure(tmp_dir, "clust_abc", "disk1")
      mtime2 = File.stat!(Identity.path(tmp_dir)).mtime

      assert mtime1 == mtime2,
             "ensure/3 should not rewrite the identity file when contents already match"
    end

    test "refuses with :foreign_cluster when an existing file names a different cluster",
         %{tmp_dir: tmp_dir} do
      :ok = Identity.write(tmp_dir, Identity.new("disk1", "clust_other"))

      assert {:error, {:foreign_cluster, expected: "clust_abc", actual: "clust_other"}} =
               Identity.ensure(tmp_dir, "clust_abc", "disk1")

      # Existing file must be untouched.
      {:ok, identity} = Identity.read(tmp_dir)
      assert identity.cluster_id == "clust_other"
    end

    test "refuses with :drive_id_mismatch when the drive_id differs",
         %{tmp_dir: tmp_dir} do
      :ok = Identity.write(tmp_dir, Identity.new("disk-original", "clust_abc"))

      assert {:error, {:drive_id_mismatch, expected: "disk-renamed", actual: "disk-original"}} =
               Identity.ensure(tmp_dir, "clust_abc", "disk-renamed")
    end
  end
end
