defmodule NeonFS.Error.QuorumUnavailable do
  @moduledoc """
  Durability quorum could not be satisfied for the operation — either
  too few drives were available to place the replicas/shards, or too
  few of the attempted writes succeeded.

  `successful` and `failed` carry per-drive detail for the write path
  (`ChunkReplicator`); drive-selection failures leave them `nil`.
  """
  use Splode.Error,
    fields: [:operation, :required, :available, :successful, :failed],
    class: :unavailable

  @impl true
  def message(%{operation: op, required: req, available: avail, failed: failed})
      when not is_nil(op) and not is_nil(req) and not is_nil(avail) and failed not in [nil, []] do
    "Quorum unavailable for #{op}: need #{req}, have #{avail} (failures: #{format_failed(failed)})"
  end

  def message(%{operation: op, required: req, available: avail})
      when not is_nil(op) and not is_nil(req) and not is_nil(avail) do
    "Quorum unavailable for #{op}: need #{req}, have #{avail}"
  end

  def message(%{operation: op}) when not is_nil(op) do
    "Quorum unavailable for #{op}"
  end

  def message(_), do: "Quorum unavailable"

  defp format_failed(failed) do
    Enum.map_join(failed, ", ", fn
      {drive_id, reason} -> "#{drive_id}: #{inspect(reason)}"
      reason -> inspect(reason)
    end)
  end

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
