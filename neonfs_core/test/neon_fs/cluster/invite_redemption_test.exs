defmodule NeonFS.Cluster.InviteRedemptionTest do
  use ExUnit.Case, async: true

  alias NeonFS.Cluster.InviteRedemption

  describe "redeem/1 validation" do
    test "returns error for missing params" do
      assert {:error, :invalid_params} = InviteRedemption.redeem(%{})
    end

    test "returns error for non-map input" do
      assert {:error, :invalid_params} = InviteRedemption.redeem("not a map")
    end
  end
end
