defmodule NeonFS.Error.InvalidConfig do
  @moduledoc """
  A configuration value is invalid or missing.
  """
  use Splode.Error, fields: [:field, :reason], class: :invalid

  @impl true
  def message(%{field: field, reason: reason}) when not is_nil(field) and not is_nil(reason) do
    "Invalid configuration for #{field}: #{reason}"
  end

  def message(%{field: field}) when not is_nil(field) do
    "Invalid configuration: #{field}"
  end

  def message(_), do: "Invalid configuration"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
