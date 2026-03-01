defmodule NeonFS.Error.Forbidden do
  @moduledoc """
  Error class for authorisation failures.

  Used when ACL checks or permission enforcement denies access.
  """
  use Splode.Error, fields: [message: nil, details: %{}], class: :forbidden

  @impl true
  def message(%{message: message}) when is_binary(message), do: message
  def message(_), do: "Permission denied"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
