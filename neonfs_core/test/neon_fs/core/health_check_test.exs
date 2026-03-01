defmodule NeonFS.Core.HealthCheckTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.HealthCheck

  describe "check/1" do
    test "all healthy subsystems returns healthy overall" do
      report =
        HealthCheck.check(
          checks: [
            cache: fn -> %{status: :healthy} end,
            clock: fn -> %{status: :healthy} end,
            drives: fn -> %{status: :healthy} end,
            ra: fn -> %{status: :healthy} end,
            service_registry: fn -> %{status: :healthy} end,
            storage: fn -> %{status: :healthy} end,
            volumes: fn -> %{status: :healthy} end
          ],
          timeout_ms: 100
        )

      assert report.status == :healthy
      assert report.node == Node.self()
      assert %DateTime{} = report.checked_at

      assert report.checks |> Map.keys() |> Enum.sort() == [
               :cache,
               :clock,
               :drives,
               :ra,
               :service_registry,
               :storage,
               :volumes
             ]
    end

    test "one degraded subsystem returns degraded overall" do
      report =
        HealthCheck.check(
          checks: [
            cache: fn -> %{status: :healthy} end,
            clock: fn -> %{status: :degraded, max_skew_ms: 120} end,
            drives: fn -> %{status: :healthy} end,
            ra: fn -> %{status: :healthy} end,
            service_registry: fn -> %{status: :healthy} end,
            storage: fn -> %{status: :healthy} end,
            volumes: fn -> %{status: :healthy} end
          ],
          timeout_ms: 100
        )

      assert report.status == :degraded
      assert report.checks.clock.status == :degraded
    end

    test "one unhealthy subsystem returns unhealthy overall" do
      report =
        HealthCheck.check(
          checks: [
            cache: fn -> %{status: :healthy} end,
            clock: fn -> %{status: :healthy} end,
            drives: fn -> %{status: :healthy} end,
            ra: fn -> %{status: :unhealthy, reason: :no_leader} end,
            service_registry: fn -> %{status: :healthy} end,
            storage: fn -> %{status: :healthy} end,
            volumes: fn -> %{status: :healthy} end
          ],
          timeout_ms: 100
        )

      assert report.status == :unhealthy
      assert report.checks.ra.status == :unhealthy
    end

    test "timed out subsystem reports unhealthy timeout" do
      report =
        HealthCheck.check(
          checks: [
            cache: fn -> %{status: :healthy} end,
            clock: fn -> %{status: :healthy} end,
            drives: fn -> %{status: :healthy} end,
            ra: fn ->
              receive do
              after
                :infinity -> :ok
              end
            end,
            service_registry: fn -> %{status: :healthy} end,
            storage: fn -> %{status: :healthy} end,
            volumes: fn -> %{status: :healthy} end
          ],
          timeout_ms: 10
        )

      assert report.status == :unhealthy
      assert report.checks.ra.status == :unhealthy
      assert report.checks.ra.reason == :timeout
    end
  end
end
