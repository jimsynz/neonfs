defmodule NeonFS.Core.HealthCheckTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.HealthCheck
  alias NeonFS.Core.HealthCheck, as: CoreHealthCheck

  setup do
    on_exit(fn -> HealthCheck.reset() end)
  end

  describe "register_checks/0" do
    test "registers all 7 core checks" do
      CoreHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)
      check_names = report.checks |> Map.keys() |> Enum.sort()

      assert :core_cache in check_names
      assert :core_clock in check_names
      assert :core_drives in check_names
      assert :core_ra in check_names
      assert :core_service_registry in check_names
      assert :core_storage in check_names
      assert :core_volumes in check_names
      assert length(check_names) == 7
    end
  end

  describe "checks/0" do
    test "returns keyword list with 7 entries" do
      checks = CoreHealthCheck.checks()
      assert length(checks) == 7
      names = Keyword.keys(checks)
      assert :cache in names
      assert :clock in names
      assert :drives in names
      assert :ra in names
      assert :service_registry in names
      assert :storage in names
      assert :volumes in names
    end
  end

  describe "check via client framework with mock checks" do
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
