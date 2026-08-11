defmodule NeonFS.Block.Telemetry do
  @moduledoc """
  Prometheus metric definitions for a NeonFS block target.

  Maps the block telemetry events to Prometheus metric types consumed by
  `TelemetryMetricsPrometheus.Core`. The HTTP endpoint is served by
  `NeonFS.Block.MetricsPlug`.

  ## Events

    * `[:neonfs, :block, :command]` — one guest IO command. Measurements
      `bytes` and `duration`; metadata `export`, `command`
      (`:read | :write | :flush | :write_zeroes`), `status`.
    * `[:neonfs, :block, :attached]` / `[:neonfs, :block, :detached]` —
      a device gaining or losing a holder. Measurement `holders`;
      metadata `export`.

  Flush latency is the one to alert on: a flush is a durability barrier,
  so a guest filesystem's journal commits at whatever rate flush returns.
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
    command_metrics() ++ attachment_metrics()
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
