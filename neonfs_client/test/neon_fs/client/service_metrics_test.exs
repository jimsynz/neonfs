defmodule NeonFS.Client.ServiceMetricsTest do
  use ExUnit.Case, async: true

  alias NeonFS.Client.ServiceMetrics

  describe "new/2" do
    test "creates with required node and defaults" do
      metrics = ServiceMetrics.new(:node@host)

      assert metrics.node == :node@host
      assert metrics.cpu_pressure == 0.0
      assert metrics.io_pressure == 0.0
      assert metrics.storage_pressure == 0.0
      assert metrics.queue_depth == 0
      assert %DateTime{} = metrics.updated_at
    end

    test "accepts custom options" do
      now = DateTime.utc_now()

      metrics =
        ServiceMetrics.new(:node@host,
          cpu_pressure: 0.8,
          io_pressure: 0.3,
          storage_pressure: 0.5,
          queue_depth: 42,
          updated_at: now
        )

      assert metrics.cpu_pressure == 0.8
      assert metrics.io_pressure == 0.3
      assert metrics.storage_pressure == 0.5
      assert metrics.queue_depth == 42
      assert metrics.updated_at == now
    end
  end

  describe "to_map/1" do
    test "converts struct to plain map with all fields" do
      metrics = ServiceMetrics.new(:node@host, cpu_pressure: 0.7, queue_depth: 10)
      map = ServiceMetrics.to_map(metrics)

      assert is_map(map)
      refute is_struct(map)
      assert map.node == :node@host
      assert map.cpu_pressure == 0.7
      assert map.io_pressure == 0.0
      assert map.storage_pressure == 0.0
      assert map.queue_depth == 10
      assert %DateTime{} = map.updated_at
    end
  end
end
