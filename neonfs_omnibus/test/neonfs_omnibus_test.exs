defmodule NeonFS.Omnibus.ApplicationTest do
  use ExUnit.Case

  test "omnibus module is defined" do
    assert Code.ensure_loaded?(NeonFS.Omnibus)
  end
end
