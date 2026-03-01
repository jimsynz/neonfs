defmodule NeonFS.Core.TelemetryTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Telemetry, as: NeonTelemetry

  @valid_types [
    Telemetry.Metrics.Counter,
    Telemetry.Metrics.Distribution,
    Telemetry.Metrics.LastValue,
    Telemetry.Metrics.Sum
  ]

  describe "metrics/0" do
    test "returns a non-empty list" do
      metrics = NeonTelemetry.metrics()

      assert [_ | _] = metrics
    end

    test "returns valid Telemetry.Metrics structs" do
      for metric <- NeonTelemetry.metrics() do
        assert %{__struct__: struct_mod} = metric

        assert struct_mod in @valid_types,
               "unexpected metric type: #{inspect(struct_mod)}"
      end
    end

    test "has no duplicate metric names" do
      names =
        NeonTelemetry.metrics()
        |> Enum.map(& &1.name)

      duplicates =
        names
        |> Enum.frequencies()
        |> Enum.filter(fn {_name, count} -> count > 1 end)
        |> Enum.map(fn {name, _count} -> Enum.join(name, ".") end)

      assert duplicates == [],
             "duplicate metric names: #{inspect(duplicates)}"
    end

    test "every metric has a description" do
      for metric <- NeonTelemetry.metrics() do
        name = Enum.join(metric.name, ".")
        assert metric.description != nil, "#{name} missing description"
        assert metric.description != "", "#{name} has empty description"
      end
    end

    test "distributions have histogram buckets defined" do
      distributions =
        NeonTelemetry.metrics()
        |> Enum.filter(&match?(%Telemetry.Metrics.Distribution{}, &1))

      assert [_ | _] = distributions

      for metric <- distributions do
        name = Enum.join(metric.name, ".")
        buckets = get_in(metric.reporter_options, [:buckets])

        assert [_ | _] = buckets,
               "#{name} missing histogram buckets"

        assert buckets == Enum.sort(buckets),
               "#{name} buckets must be sorted ascending"
      end
    end
  end

  describe "chunk metrics" do
    test "includes store and retrieve duration histograms" do
      names = metric_names()

      assert "neonfs.blob.store_chunk.duration" in names
      assert "neonfs.blob.retrieve_chunk.duration" in names
    end

    test "includes store and retrieve counters" do
      names = metric_names()

      assert "neonfs.blob.store_chunk.count" in names
      assert "neonfs.blob.retrieve_chunk.count" in names
    end
  end

  describe "replication metrics" do
    test "includes duration histogram and count counter" do
      names = metric_names()

      assert "neonfs.replication.chunk.duration" in names
      assert "neonfs.replication.chunk.count" in names
    end
  end

  describe "repair metrics" do
    test "includes repair count and read repair counters" do
      names = metric_names()

      assert "neonfs.repair.chunk.count" in names
      assert "neonfs.repair.read_repair.submitted.count" in names
      assert "neonfs.repair.read_repair.completed.count" in names
      assert "neonfs.repair.read_repair.failed.count" in names
    end
  end

  describe "storage metrics" do
    test "includes bytes used, bytes free, and drive count gauges" do
      gauges =
        NeonTelemetry.metrics()
        |> Enum.filter(&match?(%Telemetry.Metrics.LastValue{}, &1))
        |> Enum.map(fn m -> Enum.join(m.name, ".") end)

      assert "neonfs.storage.bytes_used" in gauges
      assert "neonfs.storage.bytes_free" in gauges
      assert "neonfs.storage.drive_count" in gauges
    end
  end

  describe "cluster metrics" do
    test "includes node count and ra gauges" do
      gauges =
        NeonTelemetry.metrics()
        |> Enum.filter(&match?(%Telemetry.Metrics.LastValue{}, &1))
        |> Enum.map(fn m -> Enum.join(m.name, ".") end)

      assert "neonfs.cluster.nodes.count" in gauges
      assert "neonfs.cluster.ra.term" in gauges
      assert "neonfs.cluster.ra.leader" in gauges
    end

    test "includes ra command duration histogram" do
      names = metric_names()

      assert "neonfs.ra.command.duration" in names
    end
  end

  describe "FUSE metrics" do
    test "includes operation duration histogram and count counter" do
      names = metric_names()

      assert "neonfs.fuse.operation.duration" in names
      assert "neonfs.fuse.operation.count" in names
    end

    test "operation metrics are tagged by operation type" do
      fuse_metrics =
        NeonTelemetry.metrics()
        |> Enum.filter(fn m ->
          name = Enum.join(m.name, ".")
          String.starts_with?(name, "neonfs.fuse.")
        end)

      for metric <- fuse_metrics do
        assert :operation in metric.tags,
               "#{Enum.join(metric.name, ".")} should be tagged by :operation"
      end
    end
  end

  describe "cache metrics" do
    test "includes hit, miss, and eviction counters" do
      counters =
        NeonTelemetry.metrics()
        |> Enum.filter(&match?(%Telemetry.Metrics.Counter{}, &1))
        |> Enum.map(fn m -> Enum.join(m.name, ".") end)

      assert "neonfs.cache.chunk.hit.count" in counters
      assert "neonfs.cache.chunk.miss.count" in counters
      assert "neonfs.cache.resolved.eviction.count" in counters
    end

    test "includes cache size gauge" do
      gauges =
        NeonTelemetry.metrics()
        |> Enum.filter(&match?(%Telemetry.Metrics.LastValue{}, &1))
        |> Enum.map(fn m -> Enum.join(m.name, ".") end)

      assert "neonfs.cache.size.bytes" in gauges
      assert "neonfs.cache.entry.count" in gauges
    end
  end

  describe "I/O scheduler metrics" do
    test "includes queue depth gauges and dispatch counter" do
      names = metric_names()

      assert "neonfs.io.queue.depth" in names
      assert "neonfs.worker.queue.depth" in names
      assert "neonfs.io.dispatch.count" in names
    end
  end

  describe "drive metrics" do
    test "includes drive state gauge" do
      names = metric_names()

      assert "neonfs.storage.drive.state" in names
    end
  end

  defp metric_names do
    NeonTelemetry.metrics()
    |> Enum.map(fn m -> Enum.join(m.name, ".") end)
  end
end
