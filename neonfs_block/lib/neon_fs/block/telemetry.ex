defmodule NeonFS.Block.Telemetry do
  @moduledoc """
  Prometheus metric definitions for a NeonFS block target.

  Maps the block telemetry events to Prometheus metric types consumed by
  `TelemetryMetricsPrometheus.Core`. The HTTP endpoint is served by
  `NeonFS.Block.MetricsPlug`.

  ## Events

    * `[:neonfs, :block, :command]` — one guest IO command. Measurements
      `bytes` and `duration`, plus `chunk_bytes` on a write or a
      zero-fill and `chunks_replaced` on a zero-fill; metadata `export`,
      `command` (`:read | :write | :flush | :write_zeroes`), `status`.
    * `[:neonfs, :block, :attached]` / `[:neonfs, :block, :detached]` —
      a device gaining or losing a holder. Measurement `holders`;
      metadata `export`.
    * `[:neonfs, :client, :chunk_reader, :chunk_fetched]` — emitted by
      `NeonFS.Client.ChunkReader` on this node, once per chunk a read
      fetched. Not a block event, which is why it is filtered to the
      ones `NeonFS.Block.Device` tagged with an `export`: on an omnibus
      node the same event also carries FUSE's and S3's reads.

  Flush latency is the one to alert on: a flush is a durability barrier,
  so a guest filesystem's journal commits at whatever rate flush returns.

  ## Amplification

  Both directions export the chunk-layer bytes beside the guest bytes,
  so the ratio is taken at query time rather than baked into a gauge
  that cannot be re-aggregated:

      neonfs_block_command_chunk_bytes{command="write"}
        / neonfs_block_command_bytes{command="write"}

      neonfs_block_read_chunk_bytes / neonfs_block_command_bytes{command="read"}

  The two numerators come from different places because the measurement
  does. A write's cost is arithmetic over the chunk geometry, which core
  does and returns; a read's is only known to the client library that
  fetched the chunks. Neither is computable here, and computing either
  from the request's own offset and length would give an upper bound
  rather than a measurement — a sparse device's unwritten region reads
  as zeroes with no chunk fetched at all.

  A zero-fill is on the `chunk_bytes` numerator too, but its ratio is not
  the number to watch: it rewrites only the chunks it clips, so a
  full-device TRIM's amplification tends to zero however much it cost.
  What it cost is the metadata:

      neonfs_block_command_chunks_replaced{command="write_zeroes"}

  one entry per chunk the range covered, against the single zero blob
  they all now point at.
  """

  import Telemetry.Metrics

  # A guest IO spans page-cache-sized writes to whole-device flushes, so
  # the buckets run from sub-millisecond to ten seconds.
  @io_buckets [0.001, 0.01, 0.05, 0.1, 0.5, 1, 5, 10]

  @doc """
  Returns all block-specific Prometheus metric specifications.
  """
  @spec metrics() :: [Telemetry.Metrics.t()]
  def metrics do
    command_metrics() ++ amplification_metrics() ++ attachment_metrics()
  end

  defp command_metrics do
    [
      distribution("neonfs.block.command.duration",
        event_name: [:neonfs, :block, :command],
        measurement: :duration,
        unit: {:native, :second},
        tags: [:export, :command],
        description: "Duration of block IO commands",
        reporter_options: [buckets: @io_buckets]
      ),
      counter("neonfs.block.command.count",
        event_name: [:neonfs, :block, :command],
        tags: [:export, :command, :status],
        description: "Total block IO commands by type and result"
      ),
      sum("neonfs.block.command.bytes",
        event_name: [:neonfs, :block, :command],
        measurement: :bytes,
        tags: [:export, :command],
        description: "Total bytes moved by block IO commands"
      )
    ]
  end

  defp amplification_metrics do
    [
      sum("neonfs.block.command.chunk_bytes",
        event_name: [:neonfs, :block, :command],
        measurement: :chunk_bytes,
        tags: [:export, :command],
        keep: &(&1.command in [:write, :write_zeroes]),
        description: "Chunk-layer bytes rewritten to serve block writes and zero-fills"
      ),
      sum("neonfs.block.command.chunks_replaced",
        event_name: [:neonfs, :block, :command],
        measurement: :chunks_replaced,
        tags: [:export, :command],
        keep: &(&1.command == :write_zeroes),
        description: "Chunks a zero-fill replaced by hash rather than rewriting"
      ),
      sum("neonfs.block.read.chunk_bytes",
        event_name: [:neonfs, :client, :chunk_reader, :chunk_fetched],
        measurement: :chunk_size,
        tags: [:export],
        keep: &is_map_key(&1, :export),
        description: "Chunk-layer bytes fetched to serve block reads"
      )
    ]
  end

  defp attachment_metrics do
    [
      last_value("neonfs.block.holders",
        event_name: [:neonfs, :block, :attached],
        measurement: :holders,
        tags: [:export],
        description: "Connections currently holding each device"
      ),
      counter("neonfs.block.attached.count",
        event_name: [:neonfs, :block, :attached],
        tags: [:export],
        description: "Total device attachments"
      ),
      counter("neonfs.block.detached.count",
        event_name: [:neonfs, :block, :detached],
        tags: [:export],
        description: "Total device releases"
      )
    ]
  end
end
