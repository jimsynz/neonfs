defmodule NeonFS.Error.KeyNotFound do
  @moduledoc """
  The requested key does not exist in the KV store.
  """
  use Splode.Error, fields: [:key], class: :not_found

  @impl true
  def message(%{key: key}) when not is_nil(key), do: "Key not found: #{inspect(key)}"
  def message(_), do: "Key not found"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
