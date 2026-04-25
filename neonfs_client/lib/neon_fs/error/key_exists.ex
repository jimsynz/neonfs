defmodule NeonFS.Error.KeyExists do
  @moduledoc """
  A KV store write was rejected because the key is already present and
  the caller asked not to overwrite (`overwrite?: false`).
  """
  use Splode.Error, fields: [:key], class: :invalid

  @impl true
  def message(%{key: key}) when not is_nil(key), do: "Key already exists: #{inspect(key)}"
  def message(_), do: "Key already exists"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
