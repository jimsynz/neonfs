defmodule NeonFS.Cluster.RedeemRateLimiterTest do
  use ExUnit.Case, async: true

  alias NeonFS.Cluster.RedeemRateLimiter

  defp start_limiter(rate_limit) do
    name = :"limiter_#{System.unique_integer([:positive])}"
    {:ok, _pid} = start_supervised({RedeemRateLimiter, name: name, rate_limit: rate_limit})
    name
  end

  describe "allow?/2" do
    test "permits requests up to the limit then rejects within the window" do
      limiter = start_limiter({3, 60})
      ip = {127, 0, 0, 1}

      assert RedeemRateLimiter.allow?(ip, limiter)
      assert RedeemRateLimiter.allow?(ip, limiter)
      assert RedeemRateLimiter.allow?(ip, limiter)
      refute RedeemRateLimiter.allow?(ip, limiter)
      refute RedeemRateLimiter.allow?(ip, limiter)
    end

    test "tracks each source IP independently" do
      limiter = start_limiter({1, 60})

      assert RedeemRateLimiter.allow?({10, 0, 0, 1}, limiter)
      refute RedeemRateLimiter.allow?({10, 0, 0, 1}, limiter)

      # A different IP has its own budget.
      assert RedeemRateLimiter.allow?({10, 0, 0, 2}, limiter)
    end

    test "fails open when the limiter process is not running" do
      assert RedeemRateLimiter.allow?({127, 0, 0, 1}, :no_such_limiter)
    end
  end
end
