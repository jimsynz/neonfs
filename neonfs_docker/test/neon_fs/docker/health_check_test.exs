defmodule NeonFS.Docker.HealthCheckTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.HealthCheck
  alias NeonFS.Docker.HealthCheck, as: DockerHealthCheck
  alias NeonFS.Docker.MountTracker

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
    test "registers all five Docker checks" do
      DockerHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)
      check_names = report.checks |> Map.keys() |> Enum.sort()

      assert :docker_cluster in check_names
      assert :docker_mount_capacity in check_names
      assert :docker_mount_tracker in check_names
      assert :docker_registrar in check_names
      assert :docker_volume_store in check_names
      assert length(check_names) == 5
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

  describe ":mount_capacity check" do
    setup do
      # Register the global MountTracker name on a fresh GenServer so
      # the health check's `Process.whereis(MountTracker)` lookup
      # finds something. Tests that span the registered name need
      # `async: false` (already set at the module level).
      pid = start_mount_tracker(max_mounts: 2)
      on_exit(fn -> ensure_stopped(pid) end)

      {:ok, tracker: pid}
    end

    test "is :healthy with capacity metadata when below the cap" do
      DockerHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)
      check = Map.fetch!(report.checks, :docker_mount_capacity)

      assert check.status == :healthy
      assert check.used == 0
      assert check.max == 2
    end

    test "is :degraded with reason: :mount_pool_full at the cap" do
      {:ok, _} = MountTracker.mount("vol-a")
      {:ok, _} = MountTracker.mount("vol-b")

      DockerHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)
      check = Map.fetch!(report.checks, :docker_mount_capacity)

      assert check.status == :degraded
      assert check.reason == :mount_pool_full
      assert check.used == 2
      assert check.max == 2
    end

    test "is :healthy with max: nil when no cap is configured" do
      pid = start_mount_tracker([])
      on_exit(fn -> ensure_stopped(pid) end)

      DockerHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)
      check = Map.fetch!(report.checks, :docker_mount_capacity)

      assert check.status == :healthy
      assert check.max == nil
    end
  end

  defp start_mount_tracker(opts) do
    # The tracker registers under the global `MountTracker` atom
    # because the health check's `Process.whereis/1` lookup relies on
    # it. Stop any prior instance first to keep tests deterministic.
    case Process.whereis(MountTracker) do
      nil -> :ok
      pid -> ensure_stopped(pid)
    end

    full_opts =
      Keyword.merge(
        [
          name: MountTracker,
          mount_fn: fn name -> {:ok, {{:mount_id, name}, "/tmp/#{name}"}} end,
          unmount_fn: fn _ -> :ok end
        ],
        opts
      )

    {:ok, pid} = MountTracker.start_link(full_opts)
    Process.unlink(pid)
    pid
  end

  defp ensure_stopped(pid) do
    if Process.alive?(pid) do
      try do
        GenServer.stop(pid, :shutdown, 1_000)
      catch
        :exit, _ -> :ok
      end
    end
  end
end
