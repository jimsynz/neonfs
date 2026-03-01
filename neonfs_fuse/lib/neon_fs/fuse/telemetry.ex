defmodule NeonFS.FUSE.Telemetry do
  @moduledoc """
  Prometheus metric definitions for the NeonFS FUSE node.

  Maps FUSE telemetry events to Prometheus metric types consumed by
  `TelemetryMetricsPrometheus.Core`. The HTTP endpoint is served by
  `NeonFS.FUSE.MetricsPlug`.
  """

  import Telemetry.Metrics

  @fuse_buckets [0.0001, 0.001, 0.01, 0.1, 1, 10]

  @doc """
  Returns all FUSE-specific Prometheus metric specifications.
  """
  @spec metrics() :: [Telemetry.Metrics.t()]
  def metrics do
    operation_metrics() ++ cache_metrics()
  end

  # -- FUSE operation metrics -------------------------------------------------

  defp operation_metrics do
    [
      distribution("neonfs.fuse.operation.duration",
        event_name: [:neonfs, :fuse, :request, :stop],
        measurement: :duration,
        unit: {:native, :second},
        tags: [:operation, :volume],
        description: "Duration of FUSE operations",
        reporter_options: [buckets: @fuse_buckets]
      ),
      counter("neonfs.fuse.operation.count",
        event_name: [:neonfs, :fuse, :request, :stop],
        tags: [:operation, :volume, :result],
        description: "Total FUSE operations by type and result"
      )
    ]
  end

  # -- Metadata cache metrics -------------------------------------------------

  defp cache_metrics do
    [
      last_value("neonfs.fuse.metadata_cache.size",
        event_name: [:neonfs, :fuse, :metadata_cache, :size],
        measurement: :entry_count,
        tags: [:volume],
        description: "Current metadata cache entry count"
      ),
      counter("neonfs.fuse.metadata_cache.hit.count",
        event_name: [:neonfs, :fuse, :metadata_cache, :hit],
        tags: [:volume, :type],
        description: "Total metadata cache hits"
      ),
      counter("neonfs.fuse.metadata_cache.miss.count",
        event_name: [:neonfs, :fuse, :metadata_cache, :miss],
        tags: [:volume, :type],
        description: "Total metadata cache misses"
      )
    ]
  end
end
