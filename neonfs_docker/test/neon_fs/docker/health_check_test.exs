defmodule NeonFS.Docker.HealthCheckTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.HealthCheck
  alias NeonFS.Docker.HealthCheck, as: DockerHealthCheck

  setup do
    HealthCheck.reset()

    # Pin the cluster discovery to a known-empty list so the
    # `:cluster` check is deterministic in tests that don't override.
    Application.put_env(:neonfs_docker, :core_nodes_fn, fn -> [] end)

    on_exit(fn ->
      HealthCheck.reset()
      Application.delete_env(:neonfs_docker, :core_nodes_fn)
    end)
  end

  describe "register_checks/0" do
    test "registers all four Docker checks" do
      DockerHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)
      check_names = report.checks |> Map.keys() |> Enum.sort()

      assert :docker_cluster in check_names
      assert :docker_mount_tracker in check_names
      assert :docker_registrar in check_names
      assert :docker_volume_store in check_names
      assert length(check_names) == 4
    end

    test "every check returns :unhealthy when subsystems aren't running and no core nodes are reachable" do
      DockerHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)

      for {_name, %{status: status}} <- report.checks do
        assert status == :unhealthy
      end

      assert report.status == :unhealthy
    end
  end

  describe "cluster_status/1" do
    test "returns :ok when at least one core node is discoverable" do
      result = DockerHealthCheck.cluster_status(fn -> [:core@host] end)

      assert result.status == :ok
      assert result.writable
      assert result.readable
      assert result.quorum_reachable
      assert result.reason == nil
    end

    test "returns :unavailable when no core nodes are discoverable" do
      result = DockerHealthCheck.cluster_status(fn -> [] end)

      assert result.status == :unavailable
      refute result.writable
      refute result.readable
      refute result.quorum_reachable
      assert result.reason == "no-core-nodes"
    end
  end

  describe "cluster check via the universal framework" do
    test ":docker_cluster is :unhealthy when no core nodes are discoverable" do
      DockerHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)
      cluster = Map.fetch!(report.checks, :docker_cluster)

      assert cluster.status == :unhealthy
      assert cluster.reason == "no-core-nodes"
    end

    test ":docker_cluster is :healthy when a core node is discoverable" do
      Application.put_env(:neonfs_docker, :core_nodes_fn, fn -> [:core@host] end)
      DockerHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)
      cluster = Map.fetch!(report.checks, :docker_cluster)

      assert cluster.status == :healthy
    end
  end
end
