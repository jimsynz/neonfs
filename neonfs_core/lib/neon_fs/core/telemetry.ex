defmodule NeonFS.Core.Telemetry do
  @moduledoc """
  Prometheus metric definitions for NeonFS.

  Maps existing telemetry events to Prometheus metric types (counters,
  histograms, gauges) organised by category. This module provides the
  metric specifications consumed by `TelemetryMetricsPrometheus.Core`.

  The HTTP endpoint is provided separately (task 0176). This module
  only defines the metric specifications.
  """

  import Telemetry.Metrics

  # Histogram buckets in seconds for different operation types
  @chunk_buckets [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5]
  @fuse_buckets [0.0001, 0.001, 0.01, 0.1, 1, 10]
  @ra_buckets [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1]
  @replication_buckets [0.01, 0.1, 0.5, 1, 5, 10, 30]
  @repair_buckets [0.01, 0.1, 0.5, 1, 5, 10, 30, 60]

  @doc """
  Returns all Prometheus metric specifications.

  Metrics are organised into categories: chunk operations, replication,
  repair, storage, cluster, FUSE, cache, and I/O scheduler.
  """
  @spec metrics() :: [Telemetry.Metrics.t()]
  def metrics do
    cache_metrics() ++
      chunk_metrics() ++
      cluster_metrics() ++
      escalation_metrics() ++
      fuse_metrics() ++
      io_scheduler_metrics() ++
      read_write_metrics() ++
      repair_metrics() ++
      replication_metrics() ++
      storage_metrics()
  end

  # -- Cache metrics ----------------------------------------------------------

  defp cache_metrics do
    [
      counter("neonfs.cache.chunk.hit.count",
        event_name: [:neonfs, :chunk_cache, :hit],
        description: "Total chunk cache hits",
        tags: [:volume_id]
      ),
      counter("neonfs.cache.chunk.miss.count",
        event_name: [:neonfs, :chunk_cache, :miss],
        description: "Total chunk cache misses",
        tags: [:volume_id]
      ),
      counter("neonfs.cache.resolved.hit.count",
        event_name: [:neonfs, :resolved_cache, :hit],
        description: "Total resolved lookup cache hits"
      ),
      counter("neonfs.cache.resolved.miss.count",
        event_name: [:neonfs, :resolved_cache, :miss],
        description: "Total resolved lookup cache misses"
      ),
      counter("neonfs.cache.resolved.eviction.count",
        event_name: [:neonfs, :resolved_cache, :evict],
        description: "Total resolved lookup cache evictions"
      ),
      last_value("neonfs.cache.size.bytes",
        event_name: [:neonfs, :cache, :size],
        measurement: :bytes,
        description: "Current cache size in bytes"
      ),
      last_value("neonfs.cache.entry.count",
        event_name: [:neonfs, :cache, :size],
        measurement: :entry_count,
        description: "Current cache entry count"
      )
    ]
  end

  # -- Chunk operation metrics ------------------------------------------------

  defp chunk_metrics do
    [
      # BlobStore write
      distribution("neonfs.blob.store_chunk.duration",
        event_name: [:neonfs, :blob_store, :write_chunk, :stop],
        measurement: :duration,
        unit: {:native, :second},
        tags: [:drive_id, :tier],
        description: "Duration of chunk store operations",
        reporter_options: [buckets: @chunk_buckets]
      ),
      counter("neonfs.blob.store_chunk.count",
        event_name: [:neonfs, :blob_store, :write_chunk, :stop],
        description: "Total chunk store operations",
        tags: [:drive_id, :tier]
      ),
      sum("neonfs.blob.store_chunk.bytes",
        event_name: [:neonfs, :blob_store, :write_chunk, :stop],
        measurement: :bytes_written,
        description: "Total bytes written to blob store",
        tags: [:drive_id, :tier]
      ),
      counter("neonfs.blob.store_chunk.exception.count",
        event_name: [:neonfs, :blob_store, :write_chunk, :exception],
        description: "Total chunk store errors",
        tags: [:drive_id, :tier]
      ),

      # BlobStore read
      distribution("neonfs.blob.retrieve_chunk.duration",
        event_name: [:neonfs, :blob_store, :read_chunk, :stop],
        measurement: :duration,
        unit: {:native, :second},
        tags: [:drive_id, :tier],
        description: "Duration of chunk retrieve operations",
        reporter_options: [buckets: @chunk_buckets]
      ),
      counter("neonfs.blob.retrieve_chunk.count",
        event_name: [:neonfs, :blob_store, :read_chunk, :stop],
        description: "Total chunk retrieve operations",
        tags: [:drive_id, :tier]
      ),
      sum("neonfs.blob.retrieve_chunk.bytes",
        event_name: [:neonfs, :blob_store, :read_chunk, :stop],
        measurement: :bytes_read,
        description: "Total bytes read from blob store",
        tags: [:drive_id, :tier]
      ),
      counter("neonfs.blob.retrieve_chunk.exception.count",
        event_name: [:neonfs, :blob_store, :read_chunk, :exception],
        description: "Total chunk retrieve errors",
        tags: [:drive_id, :tier]
      ),

      # ChunkFetcher (local vs remote)
      counter("neonfs.chunk_fetcher.local_hit.count",
        event_name: [:neonfs, :chunk_fetcher, :local_hit],
        description: "Total local chunk fetcher hits"
      ),
      distribution("neonfs.chunk_fetcher.remote_fetch.duration",
        event_name: [:neonfs, :chunk_fetcher, :remote_fetch, :stop],
        measurement: :duration,
        unit: {:native, :second},
        description: "Duration of remote chunk fetches",
        reporter_options: [buckets: @chunk_buckets]
      ),
      counter("neonfs.chunk_fetcher.remote_fetch.exception.count",
        event_name: [:neonfs, :chunk_fetcher, :remote_fetch, :exception],
        description: "Total remote chunk fetch errors"
      )
    ]
  end

  # -- Cluster metrics --------------------------------------------------------

  defp cluster_metrics do
    [
      last_value("neonfs.cluster.nodes.count",
        event_name: [:neonfs, :cluster, :nodes],
        measurement: :count,
        description: "Number of connected cluster nodes"
      ),
      last_value("neonfs.cluster.ra.term",
        event_name: [:neonfs, :cluster, :ra],
        measurement: :term,
        description: "Current Ra consensus term"
      ),
      last_value("neonfs.cluster.ra.leader",
        event_name: [:neonfs, :cluster, :ra],
        measurement: :leader,
        description: "1 if this node is Ra leader, else 0"
      ),
      distribution("neonfs.ra.command.duration",
        event_name: [:neonfs, :quorum, :write],
        measurement: :latency_ms,
        unit: :millisecond,
        description: "Duration of Ra/quorum write commands",
        tags: [:segment_id],
        reporter_options: [buckets: @ra_buckets]
      ),
      distribution("neonfs.ra.query.duration",
        event_name: [:neonfs, :quorum, :read],
        measurement: :latency_ms,
        unit: :millisecond,
        description: "Duration of Ra/quorum read queries",
        tags: [:segment_id],
        reporter_options: [buckets: @ra_buckets]
      ),
      last_value("neonfs.clock.skew.milliseconds",
        event_name: [:neonfs, :clock, :skew],
        measurement: :skew_ms,
        description: "Clock skew between nodes in milliseconds",
        tags: [:node]
      ),
      counter("neonfs.clock.quarantine.count",
        event_name: [:neonfs, :clock, :quarantine],
        description: "Total node quarantine events due to clock skew",
        tags: [:node]
      )
    ]
  end

  # -- Escalation metrics -----------------------------------------------------

  defp escalation_metrics do
    [
      counter("neonfs.escalations.raised.count",
        event_name: [:neonfs, :escalation, :raised],
        tags: [:category, :severity],
        description: "Total escalations raised"
      ),
      counter("neonfs.escalations.resolved.count",
        event_name: [:neonfs, :escalation, :resolved],
        tags: [:category, :choice],
        description: "Total escalations resolved via operator action"
      ),
      counter("neonfs.escalations.expired.count",
        event_name: [:neonfs, :escalation, :expired],
        tags: [:category],
        description: "Total escalations that expired without resolution"
      ),
      last_value("neonfs.escalations.pending",
        event_name: [:neonfs, :escalation, :pending_by_category],
        measurement: :count,
        tags: [:category],
        description: "Pending escalations by category"
      )
    ]
  end

  # -- FUSE operation metrics -------------------------------------------------

  defp fuse_metrics do
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

  # -- I/O scheduler metrics --------------------------------------------------

  defp io_scheduler_metrics do
    [
      last_value("neonfs.io.queue.depth",
        event_name: [:neonfs, :io, :queue, :depth],
        measurement: :count,
        tags: [:volume_id],
        description: "Current I/O scheduler queue depth per volume"
      ),
      last_value("neonfs.worker.queue.depth",
        event_name: [:neonfs, :worker, :queue_depth],
        measurement: :count,
        tags: [:priority],
        description: "Current background worker queue depth by priority"
      ),
      counter("neonfs.io.dispatch.count",
        event_name: [:neonfs, :io, :complete],
        tags: [:drive_id, :priority, :type],
        description: "Total I/O operations dispatched"
      ),
      distribution("neonfs.io.operation.duration",
        event_name: [:neonfs, :io, :complete],
        measurement: :duration,
        unit: {:native, :second},
        tags: [:drive_id, :priority, :type],
        description: "Duration of I/O scheduler operations",
        reporter_options: [buckets: @chunk_buckets]
      ),
      counter("neonfs.io.error.count",
        event_name: [:neonfs, :io, :error],
        tags: [:drive_id, :priority, :type],
        description: "Total I/O scheduler operation errors"
      ),
      counter("neonfs.io.submit.count",
        event_name: [:neonfs, :io, :scheduler, :submit],
        tags: [:priority],
        description: "Total I/O operations submitted to scheduler"
      ),
      counter("neonfs.io.cancel.count",
        event_name: [:neonfs, :io, :scheduler, :cancel],
        description: "Total I/O operations cancelled"
      )
    ]
  end

  # -- Read/write operation metrics -------------------------------------------

  defp read_write_metrics do
    [
      distribution("neonfs.read_operation.duration",
        event_name: [:neonfs, :read_operation, :stop],
        measurement: :duration,
        unit: {:native, :second},
        tags: [:volume_id],
        description: "Duration of file read operations",
        reporter_options: [buckets: @chunk_buckets]
      ),
      sum("neonfs.read_operation.bytes",
        event_name: [:neonfs, :read_operation, :stop],
        measurement: :bytes,
        tags: [:volume_id],
        description: "Total bytes read via read operations"
      ),
      counter("neonfs.read_operation.exception.count",
        event_name: [:neonfs, :read_operation, :exception],
        tags: [:volume_id],
        description: "Total read operation errors"
      ),
      distribution("neonfs.write_operation.duration",
        event_name: [:neonfs, :write_operation, :stop],
        measurement: :duration,
        unit: {:native, :second},
        tags: [:volume_id],
        description: "Duration of file write operations",
        reporter_options: [buckets: @chunk_buckets]
      ),
      sum("neonfs.write_operation.bytes",
        event_name: [:neonfs, :write_operation, :stop],
        measurement: :bytes,
        tags: [:volume_id],
        description: "Total bytes written via write operations"
      ),
      counter("neonfs.write_operation.exception.count",
        event_name: [:neonfs, :write_operation, :exception],
        tags: [:volume_id],
        description: "Total write operation errors"
      )
    ]
  end

  # -- Repair metrics ---------------------------------------------------------

  defp repair_metrics do
    [
      distribution("neonfs.repair.chunk.duration",
        event_name: [:neonfs, :stripe_repair, :repair],
        measurement: :missing_chunks,
        description: "Chunks repaired per stripe repair operation",
        tags: [:state, :outcome],
        reporter_options: [buckets: @repair_buckets]
      ),
      counter("neonfs.repair.chunk.count",
        event_name: [:neonfs, :stripe_repair, :repair],
        tags: [:state, :outcome],
        description: "Total stripe repair operations by state and outcome"
      ),
      counter("neonfs.repair.read_repair.submitted.count",
        event_name: [:neonfs, :read_repair, :submitted],
        description: "Total read repair operations submitted",
        tags: [:node]
      ),
      counter("neonfs.repair.read_repair.completed.count",
        event_name: [:neonfs, :read_repair, :completed],
        description: "Total read repair operations completed",
        tags: [:node]
      ),
      counter("neonfs.repair.read_repair.failed.count",
        event_name: [:neonfs, :read_repair, :failed],
        description: "Total read repair operations failed",
        tags: [:node]
      ),
      counter("neonfs.repair.stripe_scan.count",
        event_name: [:neonfs, :stripe_repair, :scan],
        description: "Total stripe repair scans"
      ),
      last_value("neonfs.repair.stripe_scan.degraded",
        event_name: [:neonfs, :stripe_repair, :scan],
        measurement: :degraded_found,
        description: "Degraded stripes found in latest scan"
      ),
      last_value("neonfs.repair.stripe_scan.critical",
        event_name: [:neonfs, :stripe_repair, :scan],
        measurement: :critical_found,
        description: "Critical stripes found in latest scan"
      ),
      counter("neonfs.scrub.corruption.count",
        event_name: [:neonfs, :scrub, :corruption_detected],
        description: "Total chunk corruptions detected by scrub",
        tags: [:node]
      )
    ]
  end

  # -- Replication metrics ----------------------------------------------------

  defp replication_metrics do
    [
      distribution("neonfs.replication.chunk.duration",
        event_name: [:neonfs, :replication, :stop],
        measurement: :duration,
        unit: {:native, :second},
        tags: [:volume_id],
        description: "Duration of chunk replication operations",
        reporter_options: [buckets: @replication_buckets]
      ),
      counter("neonfs.replication.chunk.count",
        event_name: [:neonfs, :replication, :stop],
        tags: [:volume_id],
        description: "Total completed chunk replication operations"
      ),
      counter("neonfs.replication.chunk.exception.count",
        event_name: [:neonfs, :replication, :exception],
        tags: [:volume_id],
        description: "Total failed chunk replication operations"
      )
    ]
  end

  # -- Storage metrics --------------------------------------------------------

  defp storage_metrics do
    [
      last_value("neonfs.storage.bytes_used",
        event_name: [:neonfs, :storage, :utilisation],
        measurement: :used,
        tags: [:node, :tier, :drive_id],
        description: "Storage bytes used by node and tier"
      ),
      last_value("neonfs.storage.bytes_free",
        event_name: [:neonfs, :storage, :utilisation],
        measurement: fn %{capacity: capacity, used: used, reserved: reserved} ->
          capacity - used - reserved
        end,
        tags: [:node, :tier, :drive_id],
        description: "Storage bytes free by node and tier"
      ),
      last_value("neonfs.storage.drive_count",
        event_name: [:neonfs, :storage, :drives],
        measurement: :count,
        tags: [:node],
        description: "Number of storage drives per node"
      ),
      last_value("neonfs.storage.drive.state",
        event_name: [:neonfs, :storage, :drive_state],
        measurement: :state,
        tags: [:node, :tier, :drive_id],
        description: "Drive state (0=standby, 1=active)"
      ),
      distribution("neonfs.gc.duration",
        event_name: [:neonfs, :garbage_collector, :collect],
        measurement: :duration,
        unit: {:native, :second},
        description: "Duration of garbage collection runs",
        reporter_options: [buckets: @repair_buckets]
      ),
      sum("neonfs.gc.chunks_deleted.count",
        event_name: [:neonfs, :garbage_collector, :collect],
        measurement: :chunks_deleted,
        description: "Total chunks deleted by garbage collection"
      ),
      counter("neonfs.tier_migration.count",
        event_name: [:neonfs, :tier_migration, :stop],
        tags: [:source_tier, :target_tier],
        description: "Total tier migration operations"
      ),
      counter("neonfs.tiering.evaluation.count",
        event_name: [:neonfs, :tiering_manager, :evaluation],
        description: "Total tiering evaluation runs"
      ),
      last_value("neonfs.tiering.evaluation.promotions",
        event_name: [:neonfs, :tiering_manager, :evaluation],
        measurement: :promotions,
        description: "Promotions in latest tiering evaluation"
      ),
      last_value("neonfs.tiering.evaluation.demotions",
        event_name: [:neonfs, :tiering_manager, :evaluation],
        measurement: :demotions,
        description: "Demotions in latest tiering evaluation"
      )
    ]
  end
end
