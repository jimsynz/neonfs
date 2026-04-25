defmodule NeonFS.Error do
  @moduledoc """
  Structured error system for NeonFS using splode.

  Provides five error classes:

    * `:invalid` — bad input, validation failures
    * `:not_found` — volume, file, chunk, or key not found
    * `:forbidden` — ACL/permission denied
    * `:unavailable` — quorum loss, all nodes unreachable
    * `:internal` — unexpected failures, NIF errors

  ## Specific errors

  Domain-specific error modules provide richer context:

    * `NeonFS.Error.VolumeNotFound`
    * `NeonFS.Error.FileNotFound`
    * `NeonFS.Error.ChunkNotFound`
    * `NeonFS.Error.KeyNotFound`
    * `NeonFS.Error.KeyExists`
    * `NeonFS.Error.QuorumUnavailable`
    * `NeonFS.Error.PermissionDenied`
    * `NeonFS.Error.InvalidPath`
    * `NeonFS.Error.InvalidConfig`
  """

  use Splode,
    error_classes: [
      forbidden: NeonFS.Error.Forbidden,
      internal: NeonFS.Error.Internal,
      invalid: NeonFS.Error.Invalid,
      not_found: NeonFS.Error.NotFound,
      unavailable: NeonFS.Error.Unavailable
    ],
    unknown_error: NeonFS.Error.Unknown

  @doc """
  Renders an error struct as a human-readable string.

  ## Examples

      iex> error = %NeonFS.Error.VolumeNotFound{volume_name: "data"}
      iex> NeonFS.Error.to_string(error)
      "Volume 'data' not found"
  """
  @spec to_string(Exception.t()) :: String.t()
  def to_string(%{__exception__: true} = error), do: Exception.message(error)
  def to_string(error), do: inspect(error)
end
