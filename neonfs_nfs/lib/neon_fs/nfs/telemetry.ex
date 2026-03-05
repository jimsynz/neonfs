defmodule NeonFS.NFS.Telemetry do
  @moduledoc """
  Prometheus metric definitions for the NeonFS NFS node.

  Maps NFS telemetry events to Prometheus metric types consumed by
  `TelemetryMetricsPrometheus.Core`.
  """

  import Telemetry.Metrics

  @nfs_buckets [0.0001, 0.001, 0.01, 0.1, 1, 10]

  @doc """
  Returns all NFS-specific Prometheus metric specifications.
  """
  @spec metrics() :: [Telemetry.Metrics.t()]
  def metrics do
    operation_metrics() ++ cache_metrics()
  end

  # -- NFS operation metrics ---------------------------------------------------

  defp operation_metrics do
    [
      distribution("neonfs.nfs.operation.duration",
        event_name: [:neonfs, :nfs, :request, :stop],
        measurement: :duration,
        unit: {:native, :second},
        tags: [:operation, :volume],
        description: "Duration of NFS operations",
        reporter_options: [buckets: @nfs_buckets]
      ),
      counter("neonfs.nfs.operation.count",
        event_name: [:neonfs, :nfs, :request, :stop],
        tags: [:operation, :volume, :result],
        description: "Total NFS operations by type and result"
      )
    ]
  end

  # -- Metadata cache metrics --------------------------------------------------

  defp cache_metrics do
    [
      last_value("neonfs.nfs.metadata_cache.size",
        event_name: [:neonfs, :nfs, :metadata_cache, :size],
        measurement: :entry_count,
        tags: [:volume],
        description: "Current metadata cache entry count"
      ),
      counter("neonfs.nfs.metadata_cache.hit.count",
        event_name: [:neonfs, :nfs, :metadata_cache, :hit],
        tags: [:volume, :type],
        description: "Total metadata cache hits"
      ),
      counter("neonfs.nfs.metadata_cache.miss.count",
        event_name: [:neonfs, :nfs, :metadata_cache, :miss],
        tags: [:volume, :type],
        description: "Total metadata cache misses"
      )
    ]
  end
end
