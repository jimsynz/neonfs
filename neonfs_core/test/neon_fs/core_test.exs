defmodule NeonFS.CoreTest do
  use ExUnit.Case

  test "module exists" do
    assert Code.ensure_loaded?(NeonFS.Core)
  end
end
