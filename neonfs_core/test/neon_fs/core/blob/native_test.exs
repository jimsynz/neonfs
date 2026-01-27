defmodule NeonFS.Core.Blob.NativeTest do
  use ExUnit.Case

  alias NeonFS.Core.Blob.Native

  describe "add/2" do
    test "adds two positive integers" do
      assert Native.add(1, 2) == 3
    end

    test "adds negative integers" do
      assert Native.add(-5, 3) == -2
    end

    test "adds zero" do
      assert Native.add(0, 42) == 42
    end
  end
end
