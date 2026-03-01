defmodule NeonFS.Error.Invalid do
  @moduledoc """
  Error class for invalid input and validation failures.

  Used when request parameters, configuration values, or paths
  fail validation checks.
  """
  use Splode.Error, fields: [message: nil, details: %{}], class: :invalid

  @impl true
  def message(%{message: message}) when is_binary(message), do: message
  def message(_), do: "Invalid input"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
