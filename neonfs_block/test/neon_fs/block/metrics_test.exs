defmodule NeonFS.Block.MetricsTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias NeonFS.Block.{MetricsPlug, MetricsSupervisor, Telemetry}

  describe "metrics/0" do
    test "every definition names an event the block target actually emits" do
      emitted = [
        [:neonfs, :block, :command],
        [:neonfs, :block, :attached],
        [:neonfs, :block, :detached]
      ]

      for metric <- Telemetry.metrics() do
        assert metric.event_name in emitted,
               "#{inspect(metric.name)} listens for #{inspect(metric.event_name)}, which nothing emits"
      end
    end

    test "covers duration, count and bytes for commands" do
      names = Enum.map(Telemetry.metrics(), & &1.name)

      assert [:neonfs, :block, :command, :duration] in names
      assert [:neonfs, :block, :command, :count] in names
      assert [:neonfs, :block, :command, :bytes] in names
    end

    test "a command metric is tagged by export and command so one device is separable" do
      for metric <- Telemetry.metrics(),
          metric.event_name == [:neonfs, :block, :command] do
        assert :export in metric.tags
        assert :command in metric.tags
      end
    end

    test "the definitions are accepted by the Prometheus reporter" do
      # The reporter validates buckets, units and tag shapes at start; a
      # definition it rejects would otherwise only fail on a live node.
      {:ok, pid} =
        TelemetryMetricsPrometheus.Core.start_link(
          metrics: Telemetry.metrics(),
          name: :block_metrics_test
        )

      assert is_binary(TelemetryMetricsPrometheus.Core.scrape(:block_metrics_test))

      Supervisor.stop(pid)
    end
  end

  describe "enabled?/0" do
    test "is off unless configured on" do
      refute MetricsSupervisor.enabled?()

      Application.put_env(:neonfs_block, :metrics_enabled, true)
      on_exit(fn -> Application.delete_env(:neonfs_block, :metrics_enabled) end)

      assert MetricsSupervisor.enabled?()
    end
  end

  describe "MetricsPlug" do
    test "GET /metrics returns the prometheus exposition format" do
      opts =
        MetricsPlug.init(scrape_fun: fn -> "# HELP neonfs_block_x test\nneonfs_block_x 1\n" end)

      conn = :get |> conn("/metrics") |> MetricsPlug.call(opts)

      assert conn.status == 200

      assert Plug.Conn.get_resp_header(conn, "content-type") == [
               "text/plain; version=0.0.4; charset=utf-8"
             ]

      assert conn.resp_body =~ "# HELP neonfs_block_x"
    end

    test "POST /metrics returns 405" do
      opts = MetricsPlug.init(scrape_fun: fn -> "" end)

      assert (:post |> conn("/metrics") |> MetricsPlug.call(opts)).status == 405
    end

    test "GET /unknown returns 404" do
      opts = MetricsPlug.init(scrape_fun: fn -> "" end)

      assert (:get |> conn("/unknown") |> MetricsPlug.call(opts)).status == 404
    end
  end
end
