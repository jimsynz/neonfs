defmodule NeonFS.Block.MetricsTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias NeonFS.Block.{MetricsPlug, MetricsSupervisor, Telemetry}

  describe "metrics/0" do
    test "every definition names an event something on a block node emits" do
      emitted = [
        [:neonfs, :block, :command],
        [:neonfs, :block, :attached],
        [:neonfs, :block, :detached],
        [:neonfs, :block, :fenced],
        [:neonfs, :block, :window_drain],
        # Not a block event: `NeonFS.Client.ChunkReader` emits it on this
        # node for the reads `NeonFS.Block.Device` asks it to serve.
        [:neonfs, :client, :chunk_reader, :chunk_fetched]
      ]

      for metric <- Telemetry.metrics() do
        assert metric.event_name in emitted,
               "#{inspect(metric.name)} listens for #{inspect(metric.event_name)}, which nothing emits"
      end
    end

    # A device taken from its holders is the one attachment event an operator
    # has to see, so it is exported rather than only logged.
    test "a fenced device is counted" do
      names = Enum.map(Telemetry.metrics(), & &1.name)

      assert [:neonfs, :block, :fenced, :count] in names
    end

    test "covers duration, count and bytes for commands" do
      names = Enum.map(Telemetry.metrics(), & &1.name)

      assert [:neonfs, :block, :command, :duration] in names
      assert [:neonfs, :block, :command, :count] in names
      assert [:neonfs, :block, :command, :bytes] in names
    end

    test "both directions export chunk bytes, so amplification is a query not a gauge" do
      names = Enum.map(Telemetry.metrics(), & &1.name)

      assert [:neonfs, :block, :window_drain, :chunk_bytes] in names
      assert [:neonfs, :block, :read, :chunk_bytes] in names
    end

    # A zero-fill's cost is mostly metadata, so its amplification ratio
    # tends to zero however many entries it rewrote.
    test "a zero-fill's replaced chunks are exported, since its bytes do not describe it" do
      names = Enum.map(Telemetry.metrics(), & &1.name)

      assert [:neonfs, :block, :command, :chunks_replaced] in names
    end

    test "a command metric is tagged by export and command so one device is separable" do
      for metric <- Telemetry.metrics(),
          metric.event_name == [:neonfs, :block, :command] do
        assert :export in metric.tags
        assert :command in metric.tags
      end
    end

    # A buffered write has moved nothing yet, so its chunk cost is the
    # drain's. Charging it to the write would report a cost not yet paid —
    # and would double-count it when the drain reports it too.
    test "command chunk bytes are kept only for the zero-fill that still writes directly" do
      metric = metric_named([:neonfs, :block, :command, :chunk_bytes])

      assert metric.keep.(%{command: :write_zeroes})
      refute metric.keep.(%{command: :write})
      refute metric.keep.(%{command: :read})
      refute metric.keep.(%{command: :flush})
    end

    # `writes / extents` is the coalescing ratio, which is the number the
    # window exists to raise, so both halves are exported.
    test "the window's drain exports what it coalesced and what it cost" do
      names = Enum.map(Telemetry.metrics(), & &1.name)

      assert [:neonfs, :block, :window_drain, :writes] in names
      assert [:neonfs, :block, :window_drain, :extents] in names
      assert [:neonfs, :block, :window_drain, :chunk_bytes] in names
      assert [:neonfs, :block, :window_drain, :duration] in names
    end

    # A write rewrites every chunk it touches, so it has nothing to replace;
    # a zero on the series would read as "this one replaced nothing" rather
    # than "this one cannot".
    test "replaced chunks are kept only for zero-fills" do
      metric = metric_named([:neonfs, :block, :command, :chunks_replaced])

      assert metric.keep.(%{command: :write_zeroes})
      refute metric.keep.(%{command: :write})
      refute metric.keep.(%{command: :read})
    end

    # The chunk_fetched event is emitted for every ChunkReader caller on the
    # node, and an omnibus release runs FUSE and S3 beside the block target.
    # Only the block target's reads carry an export, and a metric tagged
    # `:export` would drop the rest with a logged error rather than ignore
    # them.
    test "read chunk bytes are kept only for the fetches Device tagged" do
      metric = metric_named([:neonfs, :block, :read, :chunk_bytes])

      assert metric.keep.(%{volume: "vol", export: "vol:/dev.img"})
      refute metric.keep.(%{volume: "vol"})
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

    defp metric_named(name) do
      Enum.find(Telemetry.metrics(), &(&1.name == name)) ||
        flunk("no metric named #{inspect(name)}")
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
