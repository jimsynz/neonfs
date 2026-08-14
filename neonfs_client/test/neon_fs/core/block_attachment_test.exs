defmodule NeonFS.Core.BlockAttachmentTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.BlockAttachment

  describe "path/1" do
    test "starts with the prefix a listing scans for" do
      assert String.starts_with?(BlockAttachment.path("blk8"), BlockAttachment.path_prefix())
    end

    test "gives each volume its own path, so two claims collide" do
      refute BlockAttachment.path("blk8") == BlockAttachment.path("blk16")
    end
  end

  describe "volume_name/1" do
    test "recovers the volume name a path was built from" do
      assert {:ok, "blk8"} = BlockAttachment.volume_name(BlockAttachment.path("blk8"))
    end

    test "refuses a path that is not an attachment claim" do
      assert :error = BlockAttachment.volume_name("/some/file")
    end

    test "refuses the bare prefix, which names no volume" do
      assert :error = BlockAttachment.volume_name(BlockAttachment.path_prefix())
    end
  end
end
