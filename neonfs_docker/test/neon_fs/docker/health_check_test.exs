defmodule NeonFS.Docker.HealthCheckTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.HealthCheck
  alias NeonFS.Docker.HealthCheck, as: DockerHealthCheck

  setup do
    HealthCheck.reset()
    on_exit(fn -> HealthCheck.reset() end)
  end

  describe "register_checks/0" do
    test "registers 3 Docker checks" do
      DockerHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)
      check_names = report.checks |> Map.keys() |> Enum.sort()

      assert :docker_mount_tracker in check_names
      assert :docker_registrar in check_names
      assert :docker_volume_store in check_names
      assert length(check_names) == 3
    end

    test "every check returns :unhealthy when subsystems aren't running" do
      DockerHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)

      for {_name, %{status: status}} <- report.checks do
        assert status == :unhealthy
      end

      assert report.status == :unhealthy
    end
  end
end
