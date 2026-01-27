defmodule NeonFS.FUSETest do
  use ExUnit.Case
  doctest NeonFS.FUSE

  test "greets the world" do
    assert NeonFS.FUSE.hello() == :world
  end
end
