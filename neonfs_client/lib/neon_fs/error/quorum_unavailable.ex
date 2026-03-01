defmodule NeonFS.Error.QuorumUnavailable do
  @moduledoc """
  A quorum of nodes could not be reached for the operation.
  """
  use Splode.Error, fields: [:operation, :required, :available], class: :unavailable

  @impl true
  def message(%{operation: op, required: req, available: avail})
      when not is_nil(op) and not is_nil(req) and not is_nil(avail) do
    "Quorum unavailable for #{op}: need #{req}, have #{avail}"
  end

  def message(%{operation: op}) when not is_nil(op) do
    "Quorum unavailable for #{op}"
  end

  def message(_), do: "Quorum unavailable"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
