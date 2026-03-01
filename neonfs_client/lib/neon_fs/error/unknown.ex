defmodule NeonFS.Error.Unknown do
  @moduledoc """
  Catch-all error for values that cannot be classified into a known error type.

  Used by the Splode framework as the `unknown_error` fallback.
  """
  use Splode.Error, fields: [message: nil, details: %{}], class: :internal

  @impl true
  def message(%{message: message}) when is_binary(message), do: message
  def message(_), do: "Unknown error"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
