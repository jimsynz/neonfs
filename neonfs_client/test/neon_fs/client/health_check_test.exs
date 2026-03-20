defmodule NeonFS.Client.HealthCheckTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.HealthCheck

  setup do
    on_exit(fn -> HealthCheck.reset() end)
  end

  describe "check/1 with inline checks" do
    test "all healthy subsystems returns healthy overall" do
      report =
        HealthCheck.check(
          checks: [
            cache: fn -> %{status: :healthy} end,
            clock: fn -> %{status: :healthy} end
          ],
          timeout_ms: 100
        )

      assert report.status == :healthy
      assert report.node == Node.self()
      assert %DateTime{} = report.checked_at
      assert map_size(report.checks) == 2
    end

    test "one degraded subsystem returns degraded overall" do
      report =
        HealthCheck.check(
          checks: [
            a: fn -> %{status: :healthy} end,
            b: fn -> %{status: :degraded, reason: :test} end
          ],
          timeout_ms: 100
        )

      assert report.status == :degraded
    end

    test "one unhealthy subsystem returns unhealthy overall" do
      report =
        HealthCheck.check(
          checks: [
            a: fn -> %{status: :healthy} end,
            b: fn -> %{status: :unhealthy, reason: :broken} end
          ],
          timeout_ms: 100
        )

      assert report.status == :unhealthy
    end

    test "timed out subsystem reports unhealthy timeout" do
      report =
        HealthCheck.check(
          checks: [
            slow: fn ->
              receive do
              after
                :infinity -> :ok
              end
            end
          ],
          timeout_ms: 10
        )

      assert report.status == :unhealthy
      assert report.checks.slow.status == :unhealthy
      assert report.checks.slow.reason == :timeout
    end

    test "exception in check is caught" do
      report =
        HealthCheck.check(
          checks: [
            broken: fn -> raise "boom" end
          ],
          timeout_ms: 100
        )

      assert report.status == :unhealthy
      assert report.checks.broken.reason == {:exception, "boom"}
    end

    test "invalid response is caught" do
      report =
        HealthCheck.check(
          checks: [
            bad: fn -> :not_a_map end
          ],
          timeout_ms: 100
        )

      assert report.status == :unhealthy
      assert {:invalid_response, _} = report.checks.bad.reason
    end
  end

  describe "register/2" do
    test "registers checks with app prefix" do
      HealthCheck.register(:myapp,
        foo: fn -> %{status: :healthy} end,
        bar: fn -> %{status: :healthy} end
      )

      report = HealthCheck.check(timeout_ms: 100)
      assert Map.has_key?(report.checks, :myapp_foo)
      assert Map.has_key?(report.checks, :myapp_bar)
    end

    test "multiple register calls merge checks" do
      HealthCheck.register(:core, cache: fn -> %{status: :healthy} end)
      HealthCheck.register(:fuse, mounts: fn -> %{status: :healthy} end)

      report = HealthCheck.check(timeout_ms: 100)
      assert Map.has_key?(report.checks, :core_cache)
      assert Map.has_key?(report.checks, :fuse_mounts)
    end

    test "check with no registrations returns healthy with empty checks" do
      report = HealthCheck.check(timeout_ms: 100)
      assert report.status == :healthy
      assert report.checks == %{}
    end
  end

  describe "handle_node_status/0" do
    test "returns serialised report" do
      HealthCheck.register(:test, ok: fn -> %{status: :healthy} end)

      assert {:ok, report} = HealthCheck.handle_node_status()
      assert is_binary(report.node)
      assert is_binary(report.status)
      assert is_binary(report.checked_at)
      assert is_map(report.checks)
    end
  end

  describe "normalise_for_json/1" do
    test "converts DateTime to ISO 8601" do
      dt = ~U[2026-01-01 00:00:00Z]
      assert HealthCheck.normalise_for_json(dt) == "2026-01-01T00:00:00Z"
    end

    test "converts atoms to strings" do
      assert HealthCheck.normalise_for_json(:healthy) == "healthy"
    end

    test "preserves nil, true, false" do
      assert HealthCheck.normalise_for_json(nil) == nil
      assert HealthCheck.normalise_for_json(true) == true
      assert HealthCheck.normalise_for_json(false) == false
    end

    test "converts structs to maps" do
      uri = URI.parse("https://example.com")
      result = HealthCheck.normalise_for_json(uri)
      assert is_map(result)
      assert result["host"] == "example.com"
    end

    test "converts tuples to inspect strings" do
      assert HealthCheck.normalise_for_json({:error, :timeout}) == "{:error, :timeout}"
    end

    test "recurses into maps" do
      input = %{status: :healthy, node: :foo@bar}
      result = HealthCheck.normalise_for_json(input)
      assert result == %{"status" => "healthy", "node" => "foo@bar"}
    end

    test "recurses into lists" do
      assert HealthCheck.normalise_for_json([:a, :b]) == ["a", "b"]
    end

    test "preserves numbers" do
      assert HealthCheck.normalise_for_json(42) == 42
      assert HealthCheck.normalise_for_json(3.14) == 3.14
    end
  end

  describe "reset/0" do
    test "clears all registered checks" do
      HealthCheck.register(:test, a: fn -> %{status: :healthy} end)
      HealthCheck.reset()

      report = HealthCheck.check(timeout_ms: 100)
      assert report.checks == %{}
    end

    test "reset is idempotent" do
      HealthCheck.reset()
      HealthCheck.reset()
    end
  end
end
