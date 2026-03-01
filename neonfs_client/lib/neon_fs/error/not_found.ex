defmodule NeonFS.Error.NotFound do
  @moduledoc """
  Error class for resource-not-found failures.

  Used when a volume, file, chunk, or key cannot be located.
  """
  use Splode.Error, fields: [message: nil, details: %{}], class: :not_found

  @impl true
  def message(%{message: message}) when is_binary(message), do: message
  def message(_), do: "Resource not found"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
