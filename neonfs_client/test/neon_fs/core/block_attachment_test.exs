defmodule NeonFS.Core.BlockAttachmentTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.BlockAttachment

  @device "/dev.img"

  describe "path/2" do
    test "starts with the prefix a listing scans for" do
      assert String.starts_with?(
               BlockAttachment.path("blk8", @device),
               BlockAttachment.path_prefix()
             )
    end

    test "gives each volume its own path, so two claims collide" do
      refute BlockAttachment.path("blk8", @device) == BlockAttachment.path("blk16", @device)
    end

    test "gives each device of one volume its own path" do
      refute BlockAttachment.path("blk8", @device) == BlockAttachment.path("blk8", "/second.img")
    end
  end

  describe "device/1" do
    test "recovers the volume and device a path was built from" do
      assert {:ok, "blk8", @device} =
               BlockAttachment.device(BlockAttachment.path("blk8", @device))
    end

    test "keeps a device path containing a colon whole" do
      assert {:ok, "blk8", "/a:b.img"} =
               BlockAttachment.device(BlockAttachment.path("blk8", "/a:b.img"))
    end

    test "refuses a path that is not an attachment claim" do
      assert :error = BlockAttachment.device("/some/file")
    end

    test "refuses the bare prefix, which names no device" do
      assert :error = BlockAttachment.device(BlockAttachment.path_prefix())
    end

    test "refuses a path naming a volume with no device" do
      assert :error = BlockAttachment.device(BlockAttachment.path_prefix() <> "blk8")
    end
  end
end
