defmodule NeonFS.Core.BlockAttachmentTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.BlockAttachment

  describe "path/1" do
    test "starts with the prefix a listing scans for" do
      assert String.starts_with?(BlockAttachment.path("vol_1"), BlockAttachment.path_prefix())
    end

    test "gives each volume its own path, so two claims collide" do
      refute BlockAttachment.path("vol_1") == BlockAttachment.path("vol_2")
    end
  end

  describe "volume_id/1" do
    test "recovers the volume id a path was built from" do
      assert {:ok, "vol_1"} = BlockAttachment.volume_id(BlockAttachment.path("vol_1"))
    end

    test "refuses a path that is not an attachment claim" do
      assert :error = BlockAttachment.volume_id("/some/file")
    end

    test "refuses the bare prefix, which names no volume" do
      assert :error = BlockAttachment.volume_id(BlockAttachment.path_prefix())
    end
  end
end
