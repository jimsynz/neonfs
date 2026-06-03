defmodule NeonFS.Core.HealthCheckTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.HealthCheck
  alias NeonFS.Core.HealthCheck, as: CoreHealthCheck
  alias NeonFS.Core.Volume

  defmodule StubVolumeRegistry do
    def list(_opts), do: Process.get(:stub_volumes, [])
  end

  defmodule StubServiceRegistry do
    def list_by_type(:core), do: List.duplicate(%{type: :core}, Process.get(:stub_core_nodes, 1))
  end

  setup do
    on_exit(fn -> HealthCheck.reset() end)
  end

  # Runs the real check_volumes/2 (not a stub) with a controllable set of
  # volumes and core-node count.
  defp run_volumes_check(volumes, core_nodes) do
    Process.put(:stub_volumes, volumes)
    Process.put(:stub_core_nodes, core_nodes)
    Process.register(self(), StubServiceRegistry)

    check =
      CoreHealthCheck.checks(
        volume_registry_mod: StubVolumeRegistry,
        service_registry_mod: StubServiceRegistry
      )
      |> Keyword.fetch!(:volumes)

    check.()
  after
    Process.unregister(StubServiceRegistry)
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

  describe "check_volumes via real Volume structs" do
    test "replicated volumes satisfied by core count are healthy" do
      volumes = [
        Volume.new("data", durability: %{type: :replicate, factor: 1, min_copies: 1}),
        %{
          Volume.new("_system", durability: %{type: :replicate, factor: 1, min_copies: 1})
          | system: true
        }
      ]

      result = run_volumes_check(volumes, 1)

      assert result.status == :healthy
      assert result.degraded_redundancy_count == 0
      assert result.volume_count == 2
      refute Map.get(result, :reason) == :not_available
    end

    test "erasure-coded volumes count data + parity chunks as required replicas" do
      volumes = [
        Volume.new("ec", durability: %{type: :erasure, data_chunks: 2, parity_chunks: 1})
      ]

      assert run_volumes_check(volumes, 3).status == :healthy
      assert run_volumes_check(volumes, 2).status == :unhealthy
    end

    test "volumes requiring more replicas than core nodes are unhealthy" do
      volumes = [
        Volume.new("data", durability: %{type: :replicate, factor: 3, min_copies: 2})
      ]

      result = run_volumes_check(volumes, 1)

      assert result.status == :unhealthy
      assert result.degraded_redundancy_count == 1
    end

    test "a mix of satisfied and under-replicated volumes is degraded" do
      volumes = [
        Volume.new("ok", durability: %{type: :replicate, factor: 1, min_copies: 1}),
        Volume.new("under", durability: %{type: :replicate, factor: 3, min_copies: 2})
      ]

      result = run_volumes_check(volumes, 1)

      assert result.status == :degraded
      assert result.degraded_redundancy_count == 1
    end
  end
end
