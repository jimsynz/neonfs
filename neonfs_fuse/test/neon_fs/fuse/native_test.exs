defmodule NeonFS.FUSE.NativeTest do
  use ExUnit.Case, async: true

  alias NeonFS.FUSE.Native

  doctest Native

  describe "NIF loading" do
    test "add/2 function works" do
      assert Native.add(2, 3) == 5
    end

    test "add/2 handles negative numbers" do
      assert Native.add(-1, 1) == 0
    end

    test "add/2 handles large numbers" do
      assert Native.add(1_000_000, 2_000_000) == 3_000_000
    end
  end
end
