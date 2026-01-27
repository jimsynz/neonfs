defmodule NeonFS.CoreTest do
  use ExUnit.Case
  doctest NeonFS.Core

  test "greets the world" do
    assert NeonFS.Core.hello() == :world
  end
end
