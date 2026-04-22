defmodule NeonFS.IAMTest do
  use ExUnit.Case, async: true

  alias Ash.Domain.Info

  describe "NeonFS.IAM" do
    test "is an Ash domain with no registered resources yet" do
      assert Info.resources(NeonFS.IAM) == []
    end
  end
end
