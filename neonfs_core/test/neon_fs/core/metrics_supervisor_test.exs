defmodule NeonFS.Core.MetricsSupervisorTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.MetricsSupervisor
  alias NeonFS.Core.Supervisor, as: CoreSupervisor

  setup do
    original_enabled = Application.get_env(:neonfs_core, :metrics_enabled, true)

    on_exit(fn ->
      Application.put_env(:neonfs_core, :metrics_enabled, original_enabled)
    end)

    :ok
  end

  describe "start_link/1" do
    test "starts prometheus reporter, telemetry poller, and bandit" do
      pid = start_supervised!({MetricsSupervisor, bind: "127.0.0.1", port: 0})
      assert is_pid(pid)

      child_ids =
        pid
        |> Supervisor.which_children()
        |> Enum.map(fn {id, _pid, _type, _modules} -> id end)

      assert TelemetryMetricsPrometheus.Core in child_ids
      assert :telemetry_poller in child_ids
      assert Bandit in child_ids
    end

    test "custom port configuration binds bandit on requested port" do
      port = free_port()

      _pid =
        start_supervised!(
          {MetricsSupervisor,
           bind: "127.0.0.1", port: port, telemetry_poller_opts: [period: 60_000]}
        )

      assert :ok == wait_until(fn -> tcp_connectable?(port) end)
    end
  end

  describe "core supervisor integration" do
    test "metrics disabled excludes metrics supervisor child" do
      Application.put_env(:neonfs_core, :metrics_enabled, false)
      assert CoreSupervisor.child_spec_for(MetricsSupervisor) == nil
    end
  end

  defp free_port do
    {:ok, socket} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, {_addr, port}} = :inet.sockname(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp tcp_connectable?(port) do
    case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 200) do
      {:ok, socket} ->
        :ok = :gen_tcp.close(socket)
        true

      {:error, _reason} ->
        false
    end
  end

  defp wait_until(condition, timeout_ms \\ 1_000, interval_ms \\ 10) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(deadline, interval_ms, condition)
  end

  defp do_wait_until(deadline, interval_ms, condition) do
    if condition.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        {:error, :timeout}
      else
        Process.sleep(interval_ms)
        do_wait_until(deadline, interval_ms, condition)
      end
    end
  end
end
