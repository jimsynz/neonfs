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

  ## The key is the volume name

  A path is built from the volume's **name**, not its id. That is what the
  CSI controller has in hand: a CSI `volume_id` is the NeonFS volume name
  (`NeonFS.CSI.ControllerServer` answers `CreateVolume` with `volume.name`),
  and unpublishing has to work for a volume that no longer exists, so
  resolving a name to an id first is not available on that path.

  The consequence is that a name reused after a delete inherits the old
  volume's attachment path. Deleting an attached volume is already refused,
  so the claim is released before the name can be taken again.
  """

  @prefix "csi:attach:"

  @doc """
  The claim path for the volume named `volume_name`.

  One path per volume, so a second node's exclusive claim collides with the
  first rather than sitting beside it.
  """
  @spec path(String.t()) :: String.t()
  def path(volume_name) when is_binary(volume_name), do: @prefix <> volume_name

  @doc """
  The prefix every attachment claim path starts with, for listing them.
  """
  @spec path_prefix() :: String.t()
  def path_prefix, do: @prefix

  @doc """
  The volume name `path` records an attachment for, or `:error` when the path
  is not an attachment claim.
  """
  @spec volume_name(String.t()) :: {:ok, String.t()} | :error
  def volume_name(@prefix <> volume_name) when volume_name != "", do: {:ok, volume_name}
  def volume_name(path) when is_binary(path), do: :error
end
