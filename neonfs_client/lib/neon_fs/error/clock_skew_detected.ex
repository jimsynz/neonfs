defmodule NeonFS.Error.ClockSkewDetected do
  @moduledoc """
  A remote timestamp's wall-clock component ran further ahead of local
  time than the configured maximum skew allows, so it could not be
  safely incorporated into the local hybrid logical clock.
  """
  use Splode.Error, fields: [:skew_ms, :max_skew_ms], class: :unavailable

  @impl true
  def message(%{skew_ms: skew, max_skew_ms: max}) when not is_nil(skew) and not is_nil(max) do
    "Clock skew detected: remote is #{skew}ms ahead, exceeds the #{max}ms limit"
  end

  def message(%{skew_ms: skew}) when not is_nil(skew) do
    "Clock skew detected: remote is #{skew}ms ahead of local time"
  end

  def message(_), do: "Clock skew detected"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
