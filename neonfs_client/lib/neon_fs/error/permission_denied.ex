defmodule NeonFS.Error.PermissionDenied do
  @moduledoc """
  The caller lacks permission for the requested operation.
  """
  use Splode.Error, fields: [:file_path, :operation, :uid, :gid], class: :forbidden

  @impl true
  def message(%{file_path: path, operation: op}) when is_binary(path) and not is_nil(op) do
    "Permission denied: #{op} on #{path}"
  end

  def message(%{file_path: path}) when is_binary(path) do
    "Permission denied: #{path}"
  end

  def message(_), do: "Permission denied"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
