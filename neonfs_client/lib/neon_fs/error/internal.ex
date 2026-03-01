defmodule NeonFS.Error.Internal do
  @moduledoc """
  Error class for unexpected internal failures.

  Used for NIF errors, unhandled exceptions, and other unexpected
  conditions that indicate a bug or system-level issue.
  """
  use Splode.Error, fields: [message: nil, details: %{}], class: :internal

  @impl true
  def message(%{message: message}) when is_binary(message), do: message
  def message(_), do: "Internal error"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
