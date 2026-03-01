defmodule NeonFS.Error.InvalidPath do
  @moduledoc """
  The provided path is malformed or violates path constraints.
  """
  use Splode.Error, fields: [:file_path, :reason], class: :invalid

  @impl true
  def message(%{file_path: path, reason: reason}) when is_binary(path) and not is_nil(reason) do
    "Invalid path '#{path}': #{reason}"
  end

  def message(%{file_path: path}) when is_binary(path) do
    "Invalid path: #{path}"
  end

  def message(_), do: "Invalid path"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
