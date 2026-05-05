defmodule NeonFS.Core.DriveTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Drive

  describe "normalize_path/1" do
    test "strips a single trailing slash" do
      assert Drive.normalize_path("/mnt/neonfs/disk1/") == "/mnt/neonfs/disk1"
    end

    test "leaves an already-normalised path unchanged" do
      assert Drive.normalize_path("/mnt/neonfs/disk1") == "/mnt/neonfs/disk1"
    end

    test "collapses consecutive slashes" do
      assert Drive.normalize_path("/mnt//neonfs///disk1") == "/mnt/neonfs/disk1"
    end

    test "resolves \".\" segments" do
      assert Drive.normalize_path("/mnt/neonfs/./disk1") == "/mnt/neonfs/disk1"
    end

    test "resolves \"..\" segments" do
      assert Drive.normalize_path("/mnt/neonfs/disk1/../disk2") == "/mnt/neonfs/disk2"
    end

    test "preserves the root \"/\"" do
      assert Drive.normalize_path("/") == "/"
    end

    test "passes the empty string through unchanged" do
      # Callers want to detect missing path themselves rather than have
      # `Path.expand` substitute the daemon's CWD (issue #755).
      assert Drive.normalize_path("") == ""
    end

    test "two trivially-different spellings of the same path collapse" do
      a = Drive.normalize_path("/mnt/neonfs/disk1/")
      b = Drive.normalize_path("/mnt/neonfs/disk1")
      c = Drive.normalize_path("/mnt//neonfs/disk1")
      assert a == b
      assert a == c
    end
  end

  describe "from_config/2" do
    test "normalises path during construction" do
      drive = Drive.from_config(%{id: "d1", path: "/mnt/neonfs/d1/", tier: :hot}, :nonode@nohost)
      assert drive.path == "/mnt/neonfs/d1"
    end
  end
end
