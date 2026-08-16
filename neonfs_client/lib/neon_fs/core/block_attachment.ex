defmodule NeonFS.Core.BlockAttachment do
  @moduledoc """
  The namespace-claim path a block device's attachment is recorded at.

  A block device is attached by at most one route at a time, and the record
  of which node holds it *is* an exclusive `NeonFS.Core.NamespaceCoordinator`
  claim on the path this module names. Three packages need to agree on it and
  cannot depend on each other: `NeonFS.CSI.AttachRegistry` claims on
  `ControllerPublishVolume`, `NeonFS.Block.DeviceRegistry` claims when an NBD
  export gains its first connection, and the core CLI handler reads the claims
  back to report attachment state. The convention lives here, in the library
  all three build on, so a change to it cannot land on one side only.

  ## The key is the device, not the volume

  A path names a *device* — a volume name and the path of the backing file
  within it — because a volume can hold more than one. `NeonFS.Block.Device`
  reads an NBD export as `<volume>:<path>`, so a volume-keyed exclusive claim
  would refuse the second device of a multi-device volume.

  Both halves have to be the *resolved* device: a bare `<volume>` export names
  that volume's own device at `default_device_path/0`, so a path built from
  the raw export string would let `blockvol` and `blockvol:/dev.img` sit beside
  each other as two claims on one device — and a CSI attachment would not
  collide with an NBD attachment of the very thing it names.

  The volume half is its **name**, not its id. That is what the CSI controller
  has in hand: a CSI `volume_id` is the NeonFS volume name
  (`NeonFS.CSI.ControllerServer` answers `CreateVolume` with `volume.name`),
  and unpublishing has to work for a volume that no longer exists, so
  resolving a name to an id first is not available on that path.

  The consequence is that a name reused after a delete inherits the old
  volume's attachment paths. Deleting an attached volume is already refused,
  so the claims are released before the name can be taken again.

  ## The holder names the node, and which node depends on the route

  A claim is held by a pid, and the coordinator releases it when that pid
  dies — that monitor is the whole "a dead node releases its attachment"
  mechanism. For a CSI attachment the holder is `NeonFS.CSI.AttachHolder` on
  the kubelet node consuming the device. For an NBD attachment the client is
  an arbitrary host that is not a BEAM node at all, so the holder is the
  `neonfs_block` node serving the socket.

  So the node a claim reports means *consumer* on one route and *gateway* on
  the other. Both are true, neither is the other, and two NBD clients reaching
  one device through the same block node are both admitted — NBD carries no
  client identity that could tell them apart from the second socket blk-mq
  opens. Closing that gap needs fencing epochs at the metadata commit, not a
  finer claim here.
  """

  @prefix "block:attach:"
  @default_device_path "/dev.img"

  @doc """
  The claim path for the device at `device_path` in the volume named
  `volume_name`.

  One path per device, so a second holder's exclusive claim collides with the
  first rather than sitting beside it.
  """
  @spec path(String.t(), String.t()) :: String.t()
  def path(volume_name, device_path)
      when is_binary(volume_name) and is_binary(device_path),
      do: @prefix <> volume_name <> ":" <> device_path

  @doc """
  The prefix every attachment claim path starts with, for listing them.
  """
  @spec path_prefix() :: String.t()
  def path_prefix, do: @prefix

  @doc """
  The path of the single backing file a block volume is provisioned with.

  A cluster-wide constant rather than per-volume state, which is what lets
  `NeonFS.CSI.AttachRegistry` name a volume's device without a round trip to
  core — including on the unpublish path, where the volume may already be
  gone. `NeonFS.Core.BlockBacking` provisions against this same value.
  """
  @spec default_device_path() :: String.t()
  def default_device_path, do: @default_device_path

  @doc """
  The device `path` records an attachment for, or `:error` when the path is
  not an attachment claim.
  """
  @spec device(String.t()) :: {:ok, String.t(), String.t()} | :error
  def device(@prefix <> rest) do
    case String.split(rest, ":", parts: 2) do
      [volume_name, device_path] when volume_name != "" and device_path != "" ->
        {:ok, volume_name, device_path}

      _otherwise ->
        :error
    end
  end

  def device(path) when is_binary(path), do: :error
end
