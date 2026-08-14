defmodule NeonFS.Core.BlockAttachment do
  @moduledoc """
  The namespace-claim path a block volume's attachment is recorded at.

  A block volume is attached to at most one node, and the record of which
  node that is *is* an exclusive `NeonFS.Core.NamespaceCoordinator` claim on
  the path this module names. Two packages need to agree on it and cannot
  depend on each other: `NeonFS.CSI.AttachRegistry` takes the claim, and the
  core CLI handler reads the claims back to report attachment state. The
  convention lives here, in the library both build on, so a change to it
  cannot land on one side only.
  """

  @prefix "csi:attach:"

  @doc """
  The claim path for `volume_id`.

  One path per volume, so a second node's exclusive claim collides with the
  first rather than sitting beside it.
  """
  @spec path(String.t()) :: String.t()
  def path(volume_id) when is_binary(volume_id), do: @prefix <> volume_id

  @doc """
  The prefix every attachment claim path starts with, for listing them.
  """
  @spec path_prefix() :: String.t()
  def path_prefix, do: @prefix

  @doc """
  The volume id `path` records an attachment for, or `:error` when the path
  is not an attachment claim.
  """
  @spec volume_id(String.t()) :: {:ok, String.t()} | :error
  def volume_id(@prefix <> volume_id) when volume_id != "", do: {:ok, volume_id}
  def volume_id(path) when is_binary(path), do: :error
end
