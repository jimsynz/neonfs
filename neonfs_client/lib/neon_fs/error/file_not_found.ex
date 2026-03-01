defmodule NeonFS.Error.FileNotFound do
  @moduledoc """
  The requested file or directory does not exist.
  """
  use Splode.Error, fields: [:file_path, :volume_id], class: :not_found

  @impl true
  def message(%{file_path: path}) when is_binary(path), do: "File not found: #{path}"
  def message(_), do: "File not found"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
