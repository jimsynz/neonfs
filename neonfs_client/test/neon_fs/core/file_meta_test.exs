defmodule NeonFS.Core.FileMetaTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.FileMeta

  describe "new/3" do
    test "creates with required fields and defaults" do
      meta = FileMeta.new("vol1", "/test.txt")

      assert meta.volume_id == "vol1"
      assert meta.path == "/test.txt"
      assert is_binary(meta.id)
      assert meta.chunks == []
      assert meta.stripes == nil
      assert meta.size == 0
      assert meta.content_type == "text/plain"
      assert meta.mode == 0o644
      assert meta.uid == 0
      assert meta.gid == 0
      assert meta.version == 1
      assert meta.previous_version_id == nil
      assert meta.detached == false
      assert meta.pinned_claim_ids == []
      assert %DateTime{} = meta.created_at
      assert %DateTime{} = meta.modified_at
      assert %DateTime{} = meta.accessed_at
      assert %DateTime{} = meta.changed_at
    end

    test "sets changed_at equal to created_at" do
      meta = FileMeta.new("vol1", "/test.txt")

      assert DateTime.compare(meta.changed_at, meta.created_at) == :eq
    end

    test "accepts custom options" do
      meta =
        FileMeta.new("vol1", "/dir/file.bin",
          id: "custom-id",
          chunks: ["chunk1", "chunk2"],
          size: 2048,
          mode: 0o755,
          uid: 1000,
          gid: 1000,
          version: 3,
          previous_version_id: "prev-id"
        )

      assert meta.id == "custom-id"
      assert meta.chunks == ["chunk1", "chunk2"]
      assert meta.size == 2048
      assert meta.mode == 0o755
      assert meta.uid == 1000
      assert meta.gid == 1000
      assert meta.version == 3
      assert meta.previous_version_id == "prev-id"
    end

    test "normalises trailing slash on path" do
      meta = FileMeta.new("vol1", "/some/path/")
      assert meta.path == "/some/path"
    end

    test "generates unique IDs" do
      meta1 = FileMeta.new("vol1", "/a.txt")
      meta2 = FileMeta.new("vol1", "/b.txt")

      refute meta1.id == meta2.id
    end

    test "auto-detects content_type from file extension" do
      assert FileMeta.new("v", "/image.png").content_type == "image/png"
      assert FileMeta.new("v", "/doc.pdf").content_type == "application/pdf"
      assert FileMeta.new("v", "/page.html").content_type == "text/html"
      assert FileMeta.new("v", "/data.json").content_type == "application/json"
      assert FileMeta.new("v", "/style.css").content_type == "text/css"
    end

    test "falls back to application/octet-stream for unknown extensions" do
      assert FileMeta.new("v", "/noext").content_type == "application/octet-stream"
      assert FileMeta.new("v", "/file.xyz123").content_type == "application/octet-stream"
    end

    test "allows content_type override via opts" do
      meta = FileMeta.new("v", "/data.bin", content_type: "text/csv")
      assert meta.content_type == "text/csv"
    end
  end

  describe "update/2" do
    test "increments version" do
      meta = FileMeta.new("vol1", "/test.txt")
      updated = FileMeta.update(meta, size: 100)

      assert updated.version == 2
    end

    test "updates specified fields" do
      meta = FileMeta.new("vol1", "/test.txt")
      updated = FileMeta.update(meta, size: 1024, mode: 0o755)

      assert updated.size == 1024
      assert updated.mode == 0o755
    end

    test "updates modified_at timestamp" do
      meta = FileMeta.new("vol1", "/test.txt")
      updated = FileMeta.update(meta, size: 100)

      assert DateTime.compare(updated.modified_at, meta.modified_at) in [:gt, :eq]
    end

    test "updates changed_at on mode change" do
      meta = FileMeta.new("vol1", "/test.txt")
      updated = FileMeta.update(meta, mode: 0o755)

      assert DateTime.compare(updated.changed_at, meta.changed_at) in [:gt, :eq]
    end

    test "updates changed_at on uid/gid change" do
      meta = FileMeta.new("vol1", "/test.txt")
      updated = FileMeta.update(meta, uid: 1000, gid: 1000)

      assert DateTime.compare(updated.changed_at, meta.changed_at) in [:gt, :eq]
    end

    test "changed_at is always >= created_at after update" do
      meta = FileMeta.new("vol1", "/test.txt")
      updated = FileMeta.update(meta, size: 100)

      assert DateTime.compare(updated.changed_at, updated.created_at) in [:gt, :eq]
    end

    test "preserves unchanged fields" do
      meta = FileMeta.new("vol1", "/test.txt", uid: 1000)
      updated = FileMeta.update(meta, size: 100)

      assert updated.uid == 1000
      assert updated.volume_id == "vol1"
      assert updated.path == "/test.txt"
    end

    test "honours an explicit `:modified_at`" do
      meta = FileMeta.new("vol1", "/test.txt")
      explicit = ~U[2024-06-01 12:00:00Z]
      updated = FileMeta.update(meta, modified_at: explicit)

      assert updated.modified_at == explicit
    end

    test "honours an explicit `:changed_at`" do
      meta = FileMeta.new("vol1", "/test.txt")
      explicit = ~U[2024-06-01 12:00:00Z]
      updated = FileMeta.update(meta, changed_at: explicit)

      assert updated.changed_at == explicit
    end

    test "still auto-stamps timestamps when the caller doesn't supply them" do
      meta = FileMeta.new("vol1", "/test.txt")
      # Sleep is the only reliable way to ensure `:modified_at`
      # actually advances — DateTime.utc_now/0 is monotonic but the
      # creation and update calls can land in the same microsecond.
      Process.sleep(2)
      updated = FileMeta.update(meta, size: 100)

      assert DateTime.compare(updated.modified_at, meta.modified_at) == :gt
      assert DateTime.compare(updated.changed_at, meta.changed_at) == :gt
    end

    test "always rewrites `:version` even if the caller passes one" do
      meta = FileMeta.new("vol1", "/test.txt")
      updated = FileMeta.update(meta, version: 99, size: 100)

      assert updated.version == meta.version + 1
    end
  end

  describe "touch/1" do
    test "updates accessed_at timestamp" do
      meta = FileMeta.new("vol1", "/test.txt")
      touched = FileMeta.touch(meta)

      assert DateTime.compare(touched.accessed_at, meta.accessed_at) in [:gt, :eq]
    end

    test "does not increment version" do
      meta = FileMeta.new("vol1", "/test.txt")
      touched = FileMeta.touch(meta)

      assert touched.version == meta.version
    end

    test "does not update changed_at" do
      meta = FileMeta.new("vol1", "/test.txt")
      touched = FileMeta.touch(meta)

      assert DateTime.compare(touched.changed_at, meta.changed_at) == :eq
    end
  end

  describe "validate_path/1" do
    test "accepts valid absolute paths" do
      assert :ok = FileMeta.validate_path("/")
      assert :ok = FileMeta.validate_path("/test.txt")
      assert :ok = FileMeta.validate_path("/dir/subdir/file.txt")
    end

    test "rejects empty path" do
      assert {:error, :invalid_path} = FileMeta.validate_path("")
    end

    test "rejects path without leading slash" do
      assert {:error, :invalid_path} = FileMeta.validate_path("no-slash")
    end

    test "rejects path with parent directory references" do
      assert {:error, :invalid_path} = FileMeta.validate_path("/../escape")
      assert {:error, :invalid_path} = FileMeta.validate_path("/dir/../other")
    end

    test "rejects trailing slash (except root)" do
      assert {:error, :invalid_path} = FileMeta.validate_path("/dir/")
      assert {:error, :invalid_path} = FileMeta.validate_path("/a/b/")
    end
  end

  describe "normalize_path/1" do
    test "preserves root" do
      assert "/" = FileMeta.normalize_path("/")
    end

    test "strips trailing slash" do
      assert "/test/path" = FileMeta.normalize_path("/test/path/")
    end

    test "preserves paths without trailing slash" do
      assert "/already/good" = FileMeta.normalize_path("/already/good")
    end
  end

  describe "parent_path/1" do
    test "returns nil for root" do
      assert nil == FileMeta.parent_path("/")
    end

    test "returns root for top-level files" do
      assert "/" = FileMeta.parent_path("/test.txt")
    end

    test "returns parent directory" do
      assert "/documents" = FileMeta.parent_path("/documents/report.pdf")
    end

    test "handles deeply nested paths" do
      assert "/a/b/c" = FileMeta.parent_path("/a/b/c/d.txt")
    end
  end
end
